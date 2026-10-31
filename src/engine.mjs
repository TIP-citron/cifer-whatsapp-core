import makeWASocket, { Browsers, DisconnectReason, WAMessageStubType, proto } from 'baileys';
import pino from 'pino';
import { Store } from './store.mjs';
import { encryptedEdit, decryptEdit, editAuthors } from './encrypted-edits.mjs';
import { Mutations } from './mutations.mjs';
import { Inbox, decodeCursor } from './inbox.mjs';
import { authStore } from './auth-store.mjs';
import { Photos } from './photos.mjs';
import { Avatars } from './avatars.mjs';
import { Groups } from './groups.mjs';
import { ChatSync } from './chat-sync.mjs';
import { WEEK, assertPeer, assertChat, isGroup, canonicalJSON, digest, exact, extract, keyOf, messageId, normalizePeer, peerPattern, protectedWire, receipt, sanitizeMessage, sanitizeMutation, strongest, uuidPattern, validateBinding, validatePrepared } from './content.mjs';

const logger=pino({level:'silent'});
async function bounded(promise,ms,fallback){let timer;try{return await Promise.race([promise,new Promise(resolve=>{timer=setTimeout(()=>resolve(fallback),ms);})]);}finally{clearTimeout(timer);}}
const safeClone=value=>JSON.parse(JSON.stringify(value));
const METHODS=new Set(['status','pair','chats','messages','photo','history','send','prepareSend','operation','operations','read','markRead','profiles','chat','setArchived','binding','bindContact','prepareMutation','mutate','mutations','disconnect']);

class Engine {
  constructor({directory,key,account,connection,onEvent=()=>{},socketFactory=makeWASocket,avatarFetch=globalThis.fetch,photoFetch=globalThis.fetch}) {
    if(typeof directory!=='string'||!directory||typeof account!=='string'||!account||account.length>200||typeof connection!=='string'||!connection||connection.length>200||typeof onEvent!=='function')throw Error('Invalid engine configuration.');
    this.account=account;this.connection=connection;this.emit=onEvent;this.factory=socketFactory;
    this.store=new Store(directory,key,{account,connection});this.closed=false;this.stopping=false;this.epoch=0;this.socket=null;this.timer=null;this.attempts=0;
    this.state='disconnected';this.qr=null;this.pairCode=null;this.pairExpiresAt=null;this.identity=this.store.get('system','identity')||null;
    this.queue=Promise.resolve();this.queued=0;this.aliasEditReplay=null;this.inflight=new Set();this.changedScheduled=false;this.metadataScheduled=false;this.pairBusy=false;this.pairWaiters=new Set();this.historyRequests=new Map();
    this.inbox=new Inbox(this.store,(chat,metadata)=>metadata?this.metadata(chat):this.changed(chat),{},key=>{this.chatSync?.forget(key);this.photos?.forget(key);});
    this.avatars=new Avatars({store:this.store,socket:()=>this.socket,live:()=>!this.closed&&!this.stopping&&!this.closing&&this.state==='connected'&&!!this.socket,changed:chat=>this.metadata(chat),fetcher:avatarFetch});
    for(const {id,value}of this.store.list('operation'))if(value.state==='sending')this.store.set('operation',id,{...value,state:'uncertain'},{expires:value.expiresAt,created:value.createdAt,tag:value.messageId});
    this.photos=new Photos(this,photoFetch);
    this.mutations=new Mutations(this);
    this.groups=new Groups(this);this.chatSync=new ChatSync(this);this.appStateReady=false;
    this.inbox.prune();
    this.pruner=setInterval(()=>{if(this.closed)return;try{this.inbox.prune();}catch{this.pause('reconnect-required');}},60000);this.pruner.unref?.();
  }
  current(epoch){return !this.closed&&!this.stopping&&!this.closing&&epoch===this.epoch;}
  active(){if(this.closed||this.stopping||this.closing)throw Error('WhatsApp is locked.');}
  event(value){if(this.closed||this.stopping)return;try{this.emit(value);}catch{/* Consumer failures cannot expose transport errors. */}}
  status(){this.active();return {state:this.state,identity:this.identity,qr:this.qr&&this.pairExpiresAt>Date.now()?this.qr:null,pairCode:this.pairCode&&this.pairExpiresAt>Date.now()?this.pairCode:null,pairExpiresAt:this.pairExpiresAt||0,connection:this.connection,revision:this.store.revision(),...this.inbox.unread()};}
  statusChanged(){this.event({type:'status',status:this.status()});}
  changed(chat){
    const epoch=this.epoch;if(this.changedScheduled===epoch||!this.current(epoch))return;this.changedScheduled=epoch;
    queueMicrotask(()=>{if(this.changedScheduled!==epoch)return;this.changedScheduled=false;if(this.current(epoch))this.event({type:'changed',revision:this.store.revision(),...(chat?{chat}:{})});});
  }
  metadata(chat){
    const epoch=this.epoch;if(this.metadataScheduled===epoch||!this.current(epoch))return;this.metadataScheduled=epoch;
    queueMicrotask(()=>{if(this.metadataScheduled!==epoch)return;this.metadataScheduled=false;if(this.current(epoch))this.event({type:'metadata',...(chat?{chat}:{})});});
  }
  enqueue(epoch,job){
    if(this.closed||this.stopping||epoch!==this.epoch)return false;
    if(this.queued>=128){this.pause('reconnecting');this.scheduleReconnect();return false;}
    this.queued++;
    this.queue=this.queue.then(async()=>{if(!this.closed&&!this.stopping&&epoch===this.epoch)await job();}).catch(()=>{if(this.current(epoch))this.event({type:'status',status:{...this.status(),error:'WhatsApp synchronization needs retry.'}});}).finally(()=>{this.queued--;});
    return true;
  }
  async flushEvents(epoch=this.epoch){
    const deadline=Date.now()+5000,timeout=Symbol('timeout');
    while(this.current(epoch)){
      const queue=this.queue,left=deadline-Date.now();
      if(left<=0||await bounded(queue,left,timeout)===timeout)throw Error('WhatsApp synchronization is still in progress.');
      if(!this.current(epoch))break;if(this.queue===queue)return;
    }
    throw Error('WhatsApp connection changed.');
  }
  pause(state='disconnected'){
    this.photos?.pause();this.avatars.pause();this.chatSync.pause();this.appStateReady=false;this.aliasEditReplay=null;
    ++this.epoch;clearTimeout(this.timer);this.timer=null;const socket=this.socket;this.socket=null;this.state=state;this.qr=null;this.pairCode=null;this.pairExpiresAt=null;
    for(const finish of this.pairWaiters)finish(false);
    try{socket?.end(new Error('Local session paused'));}catch{}
  }
  scheduleReconnect(){if(this.closed||this.stopping)return;clearTimeout(this.timer);this.timer=setTimeout(()=>{this.timer=null;void this.start().catch(()=>{if(!this.closed&&!this.stopping){this.state='reconnect-required';this.statusChanged();}});},Math.min(30000,1000*2**Math.min(this.attempts++,5)));this.timer.unref?.();}
  async canonical(value,alternate,epoch=this.epoch){
    if(!this.current(epoch))return null;const peer=normalizePeer(value);if(isGroup(peer))return peer;if(!peerPattern.test(peer))return null;
    if(peer.endsWith('@s.whatsapp.net'))return peer;
    let mapped=peer===normalizePeer(this.socket?.user?.lid)&&this.identity?.endsWith('@s.whatsapp.net')?this.identity:normalizePeer(alternate);
    if(!peerPattern.test(mapped)||!mapped.endsWith('@s.whatsapp.net'))mapped=this.store.get('alias',peer);
    if(!mapped&&this.socket?.signalRepository?.lidMapping) mapped=normalizePeer(await this.socket.signalRepository.lidMapping.getPNForLID(peer).catch(()=>null));
    if(this.closed||this.stopping||epoch!==this.epoch)return null;
    if(mapped&&peerPattern.test(mapped)&&mapped.endsWith('@s.whatsapp.net')){
      if(this.store.get('alias',peer)!==mapped){this.store.set('alias',peer,mapped);this.inbox.rebind(peer,mapped);this.inbox.rebindParticipant(peer,mapped);this.chatSync?.rebind(peer,mapped);this.chatSync?.rebindParticipant(peer,mapped);
        this.queueAliasEdits(mapped,epoch);}return mapped;
    }
    return peer;
  }
  queueAliasEdits(participant,epoch){
    if(!this.current(epoch)||!this.store.totals('encrypted-edit').count)return;
    const pending=this.aliasEditReplay;
    if(pending?.epoch===epoch){pending.participants.add(participant);return;}
    // A contacts/history/roster batch can resolve thousands of aliases while it
    // owns the queue. One replay behind that batch must cover every affected author.
    const replay={epoch,participants:new Set([participant])};this.aliasEditReplay=replay;
    this.enqueue(epoch,async()=>{
      if(this.aliasEditReplay!==replay||!this.current(epoch))return;
      // Alias discoveries during decryption may schedule one subsequent batch.
      this.aliasEditReplay=null;
      const values=this.store.list('encrypted-edit',{limit:10000}).map(r=>r.value).filter(r=>isGroup(r.chat)&&replay.participants.has(r.participant));
      if(values.length)await this.applyEncryptedEdits(values,epoch);
    });
  }
  ownKey(raw) {
    return isGroup(raw?.remoteJid)&&raw?.fromMe&&!raw.participant?{...raw,participant:this.socket?.user?.id||this.identity||this.socket?.user?.lid}:raw;
  }
  protocolKey(row) {
    return row.protocolKey||{remoteJid:row.chat,id:row.id,fromMe:row.fromMe,...(row.participant?{participant:row.participant}:{})};
  }
  async cleanKey(raw,epoch=this.epoch){
    if(!this.current(epoch))return null;const clean=keyOf(this.ownKey(raw));if(!clean)return null;
    const chat=await this.canonical(clean.chat,clean.alternate,epoch);if(!chat||this.closed||this.stopping||epoch!==this.epoch)return null;
    if(!isGroup(chat))return {chat,id:clean.id,fromMe:clean.fromMe};
    const participant=await this.canonical(clean.participant,clean.participantAlt,epoch);
    if(!participant||!peerPattern.test(participant)||this.closed||this.stopping||epoch!==this.epoch)return null;
    if(clean.fromMe){const self=await this.canonical(this.identity||this.socket?.user?.id,this.socket?.user?.lid,epoch);if(!this.current(epoch)||participant!==self)return null;}
    return {...clean,chat,participant};
  }
  async start(){
    this.active();if(this.socket)return this.status();
    this.store.set('system','auto-connect',true);const epoch=++this.epoch,live=()=>!this.closed&&!this.stopping&&epoch===this.epoch;
    const auth=authStore(this.store,live);this.state=auth.state.creds.registered?'reconnecting':'pairing';this.qr=null;this.pairCode=null;this.pairExpiresAt=null;
    const socket=this.factory({auth:auth.state,logger,browser:Browsers.macOS('CIFER Local'),markOnlineOnConnect:false,syncFullHistory:false,enableRecentMessageCache:false,generateHighQualityLinkPreview:false,
      shouldSyncHistoryMessage:h=>![proto.Message.HistorySyncType.FULL,proto.Message.HistorySyncType.RECENT].includes(h.syncType),
      getMessage:async key=>{if(!live())return undefined;const clean=await this.cleanKey(isGroup(key?.remoteJid)&&key.fromMe?{...key,participant:this.identity,participantAlt:undefined}:key,epoch);if(!live()||!clean||this.store.get('tombstone',messageId(clean)))return undefined;const row=this.store.get('message',messageId(clean));return row&&row.expiresAt>Date.now()&&row.fromMe&&!row.editUnavailableAt&&['ordinary','protected'].includes(row.kind)?{conversation:row.wire||row.text||''}:undefined;}
    });this.socket=socket;
    let pendingNotifications=false,readinessQueued=false;
    const syncReady=()=>{
      if(!live())return;
      if(!pendingNotifications||!auth.state.creds.myAppStateKeyId){this.appStateReady=false;this.chatSync.setReady(false,epoch);return;}
      if(this.appStateReady||readinessQueued)return;readinessQueued=true;
      // Baileys flush delivers callbacks; our encrypted-store queue must drain too.
      this.enqueue(epoch,()=>{
        readinessQueued=false;if(!live()||!pendingNotifications||!auth.state.creds.myAppStateKeyId)return;
        this.appStateReady=true;this.chatSync.setReady(true,epoch);
      });
    };
    socket.ev.on('creds.update',()=>{if(live()){auth.save();syncReady();}});
    socket.ev.on('connection.update',update=>{
      if(!live())return;
      if(update.receivedPendingNotifications){pendingNotifications=true;syncReady();}
      if(update.qr){this.qr=update.qr;this.pairExpiresAt=Date.now()+60000;this.state='pairing';this.statusChanged();}
      if(update.connection==='open'){
        const identity=normalizePeer(socket.user?.id||auth.state.creds.me?.id);
        if(!peerPattern.test(identity)||(this.identity&&this.identity!==identity)){this.pause('reconnect-required');this.store.set('system','auto-connect',false);this.statusChanged();return;}
        this.identity=identity;this.store.set('system','identity',identity);auth.save();this.state='connected';this.qr=null;this.pairCode=null;this.pairExpiresAt=null;this.attempts=0;this.statusChanged();
        syncReady();
        // Optional group discovery must not hold the inbound synchronization queue during network I/O.
        void this.groups.discover().then(()=>{if(live())this.chatSync.resume();}).catch(()=>{if(live())this.metadata();});
      }
      if(update.connection==='close'){
        const code=update.lastDisconnect?.error?.output?.statusCode;
        const invalid=[DisconnectReason.loggedOut,DisconnectReason.badSession,DisconnectReason.connectionReplaced,DisconnectReason.multideviceMismatch,DisconnectReason.forbidden].includes(code);
        this.pause(invalid?'reconnect-required':'reconnecting');
        if(invalid)this.store.set('system','auto-connect',false);else this.scheduleReconnect();this.statusChanged();
      }
    });
    socket.ev.on('groups.upsert',rows=>this.enqueue(epoch,()=>this.groups.update(rows)));
    socket.ev.on('groups.update',rows=>this.enqueue(epoch,async()=>{
      // groupFetchAllParticipating emits then immediately returns its exact rows.
      // Yield once so discovery can tag that echo before applying genuine events.
      await Promise.resolve();if(!live())return;const accepted=this.groups.eventRows(rows,epoch,socket);if(accepted.length)await this.groups.update(accepted);
    }));
    socket.ev.on('group-participants.update',event=>this.enqueue(epoch,()=>this.groups.participants(event)));
    socket.ev.on('contacts.upsert',rows=>this.enqueue(epoch,()=>this.contacts(rows,epoch)));
    socket.ev.on('contacts.update',rows=>this.enqueue(epoch,()=>this.contacts(rows,epoch)));
    socket.ev.on('chats.upsert',rows=>this.enqueue(epoch,()=>this.chatSettings(rows,epoch)));
    socket.ev.on('chats.update',rows=>this.enqueue(epoch,()=>this.chatSettings(rows,epoch)));
    socket.ev.on('messages.upsert',event=>{
      const raw=event.messages.map(m=>({...m,key:this.ownKey(m.key)})),snapshot=this.chatSync.snapshot(raw);
      const sealed=raw.map(m=>encryptedEdit(m)).filter(Boolean);
      const mutations=raw.map(m=>sanitizeMutation(m)).filter(Boolean);
      const values=raw.filter(m=>{const change=sanitizeMutation(m);return !change||!change.removed&&!change.envelope;}).map(m=>sanitizeMessage(m)).filter(Boolean);
      this.enqueue(epoch,async()=>{await this.chatSync.capture(snapshot);if(!live())return;await this.ingest(values,false,epoch);if(!live())return;await this.applyMutations(mutations,epoch);if(live())await this.applyEncryptedEdits(sealed,epoch);});
    });
    socket.ev.on('messages.update',rows=>{
      const clean=rows.map(({key,update})=>({key:keyOf(this.ownKey(key)),removed:update.message===null||update.messageStubType===WAMessageStubType.REVOKE,message:update.message?{normalizedContent:extract(update.message)}:undefined,at:Number(update.messageTimestamp||Date.now()/1000)*1000,status:update.status})).filter(r=>r.key);
      this.enqueue(epoch,async()=>{for(const row of clean){if(!live())return;const key=await this.cleanKey({...this.protocolKey(row.key),remoteJidAlt:row.key.alternate},epoch);if(!key)continue;if(isGroup(key.chat)){const state=receipt(row.status);if(state&&!key.fromMe)this.inbox.delivery(key,state);continue;}if(row.removed)this.inbox.tombstone(key);else if(row.message)this.inbox.edit(key,row.message,row.at);const state=receipt(row.status);if(state)this.inbox.delivery(key,state);}});
    });
    socket.ev.on('messages.delete',event=>this.enqueue(epoch,async()=>{
      if(event.keys)for(const raw of event.keys){if(!live())return;const key=await this.cleanKey(raw,epoch);if(key){this.chatSync.invalidate(key.chat);this.inbox.tombstone(key);}}
      else if(event.all&&event.jid){const chat=await this.canonical(event.jid,undefined,epoch);if(chat&&live()){this.chatSync.invalidate(chat);for(const {value}of this.store.list('message',{tag:chat}))this.inbox.tombstone(value);}}
    }));
    socket.ev.on('message-receipt.update',rows=>this.enqueue(epoch,async()=>{
      for(const row of rows){
        if(!live())return;const r=row.receipt,state=Number(r.readTimestamp)>0||Number(r.playedTimestamp)>0?'read':Number(r.receiptTimestamp)>0?'delivered':null;
        if(!state)continue;
        // Baileys puts the RECEIPT reader in key.participant. It is not the original author.
        if(isGroup(row.key?.remoteJid)){
          const recipient=await this.canonical(r.userJid,undefined,epoch);if(!live())return;if(!recipient||!peerPattern.test(recipient))continue;
          if(row.key.fromMe){
            const key=await this.cleanKey({...row.key,participant:this.identity,participantAlt:undefined},epoch);
            if(key&&recipient!==this.identity)this.inbox.groupReceipt(key,recipient,state);
          }else if(state==='read'&&recipient===this.identity){
            const candidates=this.store.list('message',{tag:row.key.remoteJid,limit:10000}).map(r=>r.value).filter(m=>m.id===row.key.id&&!m.fromMe);
            if(candidates.length===1)this.inbox.delivery(candidates[0],'read');
          }
        }else{
          const key=await this.cleanKey(row.key,epoch);if(key?.fromMe)this.inbox.delivery(key,state);
        }
      }
    }));
    socket.ev.on('messaging-history.set',event=>{
      const raw=(event.messages||[]).map(m=>({...m,key:this.ownKey(m.key)})),snapshot=this.chatSync.snapshot(raw);
      const sealed=raw.map(m=>encryptedEdit(m)).filter(Boolean);
      const mutations=raw.map(m=>sanitizeMutation(m)).filter(Boolean);
      const messages=raw.filter(m=>{const change=sanitizeMutation(m);return !change||!change.removed&&!change.envelope;}).map(m=>sanitizeMessage(m)).filter(Boolean),contacts=(event.contacts||[]),chats=(event.chats||[]),requestId=event.peerDataRequestSessionId;
      this.enqueue(epoch,async()=>{
        await this.chatSync.capture(snapshot,{history:true,requestId,progress:event.progress});if(!live())return;
        await this.contacts(contacts,epoch);if(!live())return;await this.chatSettings(chats,epoch);if(!live())return;
        await this.applyMutations(mutations,epoch);if(!live())return;
        await this.applyEncryptedEdits(sealed,epoch);if(!live())return;
        const request=requestId?this.historyRequests.get(requestId):null;
        if(request){const selected=[];for(const m of messages){if(await this.canonical(m.chat,m.alternate,epoch)===request.chat)selected.push(m);}await this.ingest(selected.slice(-20),true,epoch);if(!live())return;request.state='ready';request.hasMore=selected.length>=20;this.changed(request.chat);}
        else {const peers=new Map(),selected=[];for(const m of messages.sort((a,b)=>b.createdAt-a.createdAt)){const chat=await this.canonical(m.chat,m.alternate,epoch);if(!live())return;if(!chat)continue;if(!peers.has(chat)&&peers.size>=10)continue;const count=peers.get(chat)||0;if(count>=20)continue;peers.set(chat,count+1);selected.push(m);}await this.ingest(selected,true,epoch);}
      });
    });
    this.statusChanged();return this.status();
  }
  async applyEncryptedEdits(values,epoch=this.epoch){
    for(const seal of values){
      if(this.closed||this.stopping||epoch!==this.epoch)return;
      const key=await this.cleanKey({remoteJid:seal.chat,remoteJidAlt:seal.alternate,id:seal.id,fromMe:seal.fromMe,participant:seal.participant,participantAlt:seal.participantAlt},epoch);
      if(this.closed||this.stopping||epoch!==this.epoch)return;
      if(!key||this.store.get('tombstone',messageId(key)))continue;
      const id=messageId(key),row=this.store.get('message',id);
      if(!row?.editSecret&&row?.editedAt>=seal.at)continue;
      if(!row?.editSecret){
        const old=this.store.get('encrypted-edit',id);if(old&&old.at>=seal.at){this.store.transaction(()=>{this.inbox.unavailableEdit(key,old.at);this.store.set('encrypted-edit',id,old,{expires:old.expiresAt,created:old.at,tag:key.chat});});continue;}
        if(!old&&this.store.totals('encrypted-edit').count>=10000)throw Error('Message lifecycle storage is full.');
        this.inbox.trim(Buffer.byteLength(JSON.stringify(seal))+512);
        const seconds=this.store.get('setting',key.chat)?.seconds;
        const expiresAt=Math.min(old?.expiresAt||Date.now()+120000,row?.expiresAt||seal.at-900000+WEEK,seconds?seal.at-900000+seconds*1000:Infinity);
        if(expiresAt<=Date.now()){this.inbox.tombstone(key);continue;}
        this.store.transaction(()=>{this.inbox.unavailableEdit(key,seal.at);this.store.set('encrypted-edit',id,{...seal,...key,expiresAt},{expires:expiresAt,created:seal.at,tag:key.chat});});
        continue;
      }
      let authors=key.fromMe?editAuthors([this.socket?.user?.id,this.socket?.user?.lid,this.identity]):editAuthors(isGroup(key.chat)?[...(seal.authors||[]),seal.participant,seal.participantAlt,key.participant]:[...(seal.authors||[]),seal.chat,seal.alternate,key.chat]);
      const mappings=this.socket?.signalRepository?.lidMapping;
      for(const author of [...authors]){try{const mapped=author.endsWith('@lid')?await bounded(mappings?.getPNForLID?.(author),1000,null):await bounded(mappings?.getLIDForPN?.(author),1000,null);authors=editAuthors([...authors,mapped]);}catch{}}
      if(this.closed||this.stopping||epoch!==this.epoch)return;
      const edit=decryptEdit(seal,row.editSecret,authors);
      if(edit){this.store.remove('encrypted-edit',id);this.inbox.edit(key,{normalizedContent:edit.content},edit.at);}
    }
  }
  async applyMutations(values,epoch=this.epoch){
    for(let row of values){
      if(!this.current(epoch))return;
      if(isGroup(row.chat))row=await this.groups.authorizeMutation(row);
      if(!row||this.closed||this.stopping||epoch!==this.epoch)continue;
      const key=await this.cleanKey({remoteJid:row.chat,remoteJidAlt:row.alternate,id:row.id,fromMe:row.fromMe,participant:row.participant,participantAlt:row.participantAlt},epoch);
      if(!key)continue;if(row.removed)this.inbox.tombstone(key);else this.inbox.edit(key,{normalizedContent:row.content},row.at);
    }
  }
  async contacts(rows,epoch=this.epoch){
    for(const row of rows.slice(0,10000)){
      if(!this.current(epoch))return;if(isGroup(row.id))continue;
      const chat=await this.canonical(row.id||row.lid,row.phoneNumber,epoch);if(!this.current(epoch))return;if(!chat)continue;
      this.inbox.contact(chat,{name:row.name||row.notify||row.verifiedName});
      if(Object.hasOwn(row,'imgUrl'))this.avatars.invalidate(chat,row.imgUrl==='removed'||row.imgUrl===null);
      await this.applyEncryptedEdits(this.store.list('encrypted-edit',{tag:chat,limit:10000}).map(r=>r.value),epoch);
    }this.metadata();
  }
  async chatSettings(rows,epoch=this.epoch){
    for(const row of rows.slice(0,10000)){
      if(!this.current(epoch))return;const chat=await this.canonical(row.id,row.pnJid,epoch);if(!this.current(epoch))return;if(!chat)continue;
      this.chatSync.chatUpdate(row,chat);
      const updatedAt=Math.min(Date.now(),Number(row.conversationTimestamp||0)*1000);
      this.inbox.contact(chat,{...(row.name?{name:row.name}:{}),...(Number.isSafeInteger(updatedAt)?{updatedAt}:{})});
      if(row.ephemeralExpiration!==undefined){const seconds=Number(row.ephemeralExpiration);if(Number.isSafeInteger(seconds)&&seconds>=0)this.store.set('setting',chat,{seconds});}
      const seeds=(row.messages||[]).map(r=>r.message).filter(Boolean);
      await this.chatSync.capture(this.chatSync.snapshot(seeds),{history:true});
      if(!this.current(epoch))return;const seed=seeds[0],key=seed?await this.cleanKey(seed.key,epoch):null,at=Number(seed?.messageTimestamp)*1000;
      if(!this.current(epoch))return;if(key&&Number.isSafeInteger(at)&&at>Date.now()-WEEK)this.store.set('history-seed',chat,{...key,chat,createdAt:at},{expires:at+WEEK,created:at});
    }this.metadata();
  }
  async ingest(values,history=false,epoch=this.epoch){
    for(const row of values){
      if(!this.current(epoch))return;
      const key=await this.cleanKey({remoteJid:row.chat,remoteJidAlt:row.alternate,id:row.id,fromMe:row.fromMe,participant:row.participant,participantAlt:row.participantAlt},epoch);
      if(!key)continue;const chat=key.chat;
      const candidates=row.fromMe?this.store.list('operation',{tag:row.id,limit:100}).map(r=>r.value):[];
      let op=null;for(const candidate of candidates)if(await this.canonical(candidate.chat,undefined,epoch)===chat){op=candidate;break;}
      if(!this.current(epoch))return;const target=op?op.chat:chat,clean={...row,...key,chat:target};
      this.inbox.ingest(clean,{history,operationId:op?.operationId});
      const pending=this.store.get('encrypted-edit',messageId(clean));if(pending)await this.applyEncryptedEdits([pending],epoch);if(!this.current(epoch))return;
      if(isGroup(target))await this.groups.reconcilePending(target);if(!this.current(epoch))return;
      if(op){const expiresAt=Math.min(op.expiresAt,row.expiresAt);this.store.set('operation',op.operationId,{...op,expiresAt},{expires:expiresAt,created:op.createdAt,tag:op.messageId});this.inbox.delivery({...clean,fromMe:true},isGroup(target)?'sent':receipt(row.status)||'sent');}
    }
  }
  async pairingReady(socket,epoch){
    if(this.state==='pairing'&&this.qr&&this.pairExpiresAt>Date.now())return true;
    return new Promise(resolve=>{
      let timer;
      const finish=value=>{clearTimeout(timer);socket.ev.off('connection.update',update);this.pairWaiters.delete(finish);resolve(value);};
      const update=()=>{if(this.closed||this.stopping||epoch!==this.epoch||this.state!=='pairing')finish(false);else if(this.qr&&this.pairExpiresAt>Date.now())finish(true);};
      this.pairWaiters.add(finish);socket.ev.on('connection.update',update);timer=setTimeout(()=>finish(false),10000);update();
    });
  }
  async pair(args){
    exact(args,[],['phone']);if(args.phone!==undefined&&(typeof args.phone!=='string'||!/^\d{7,15}$/.test(args.phone)))throw Error('Enter a WhatsApp phone number with country code.');
    if(this.pairBusy)throw Error('WhatsApp pairing is already in progress.');
    this.pairBusy=true;
    try{
      if(this.state==='pairing'&&this.socket&&this.pairExpiresAt&&this.pairExpiresAt<=Date.now())this.pause('pairing');
      await this.start();if(!args.phone||this.state==='connected')return this.status();
      const epoch=this.epoch,socket=this.socket;
      if(!await this.pairingReady(socket,epoch))throw Error('WhatsApp pairing is not ready. Try again.');
      const code=await bounded(socket.requestPairingCode(args.phone),10000,null);
      if(this.closed||this.stopping||epoch!==this.epoch)throw Error('WhatsApp connection changed.');
      if(typeof code!=='string'||!/^[A-Z0-9]{8}$/.test(code))throw Error('Pairing code is unavailable.');
      this.pairCode=code;this.qr=null;this.pairExpiresAt=Date.now()+120000;this.statusChanged();return this.status();
    }finally{this.pairBusy=false;}
  }
  sendInput(args){
    exact(args,['operationId','chat','text','kind'],['prepared']);
    if(!uuidPattern.test(args.operationId)||!['ordinary','protected'].includes(args.kind)||typeof args.text!=='string'||!args.text.trim())throw Error('Invalid WhatsApp message.');
    const chat=assertChat(args.chat);if(isGroup(chat)&&args.kind!=='ordinary')throw Error('CIFER protection is not available in groups yet.');if(args.kind==='ordinary'){if(Buffer.byteLength(args.text)>2000||args.prepared!==undefined||args.text.startsWith('CIFER:WA:'))throw Error('Invalid ordinary message.');}
    else{const context=protectedWire(args.text);if(context.sender!==this.account||context.createdAt<=Date.now()-WEEK||context.createdAt>Date.now()+60000)throw Error('Protected message belongs to another account or expired.');validatePrepared(args.prepared,this.account,chat,args.text);if(canonicalJSON(this.store.get('contact-binding',chat))!==canonicalJSON(args.prepared.binding))throw Error('The confirmed contact changed. Prepare the message again.');}
    return safeClone({operationId:args.operationId,chat,text:args.text,kind:args.kind,...(args.prepared!==undefined?{prepared:args.prepared}:{})});
  }
  operationView(row){return {operationId:row.operationId,state:row.state==='sending'?'queued':row.state,messageId:row.messageId,...(row.request?.prepared?{prepared:row.request.prepared}:{}),...(row.request?{request:row.request}:{})};}
  prepare(args){
    const input=this.sendInput(args),hash=digest(canonicalJSON(input)),old=this.store.get('operation',input.operationId);
    if(old){if(old.digest!==hash)throw Error('This operation already contains another message.');return this.operationView(old);}
    const prior=this.store.get('operation-identity',input.operationId);if(prior){if(prior.digest!==hash)throw Error('This operation already contains another message.');return {operationId:input.operationId,messageId:prior.messageId,state:'cancelled'};}
    if(this.store.totals('operation').count>=10000)throw Error('Local operation storage is full.');
    const now=Date.now(),setting=this.store.get('setting',input.chat),expiresAt=Math.min(now+WEEK,setting?.seconds?now+setting.seconds*1000:Infinity,input.kind==='protected'?protectedWire(input.text).createdAt+WEEK:Infinity);
    const row={operationId:input.operationId,chat:input.chat,...(isGroup(input.chat)?{participant:this.identity}:{}),digest:hash,request:input,state:'prepared',messageId:'3EB0'+digest(input.operationId).slice(0,28).toUpperCase(),createdAt:now,expiresAt};
    this.inbox.trim(Buffer.byteLength(JSON.stringify({id:row.operationId,value:row})));
    this.store.transaction(()=>{this.store.set('operation-identity',row.operationId,{digest:hash,messageId:row.messageId});this.store.set('operation',row.operationId,row,{expires:row.expiresAt,created:now,tag:row.messageId});this.inbox.contact(row.chat,{updatedAt:now});this.inbox.revision(row.chat);});
    return this.operationView(row);
  }
  async send(args){
    const input=this.sendInput(args),existing=this.store.get('operation',input.operationId);
    if(existing&&existing.digest!==digest(canonicalJSON(input)))throw Error('This operation already contains another message.');
    if(existing&&existing.state!=='prepared')return this.operationView(existing);
    if(this.state!=='connected'||!this.socket)throw Error('Reconnect WhatsApp before sending.');
    if(!existing){const prepared=this.prepare(input);if(prepared.state!=='prepared')return prepared;}let record=this.store.get('operation',input.operationId),epoch=this.epoch,socket=this.socket;
    if(isGroup(record.chat))await this.groups.authorizeSend(record.chat);
    if(this.closed||this.stopping||epoch!==this.epoch||socket!==this.socket)throw Error('WhatsApp connection changed.');
    const setting=this.store.get('setting',record.chat);
    record={...record,state:'sending',expiresAt:Math.min(record.expiresAt,setting?.seconds?record.createdAt+setting.seconds*1000:Infinity)};
    if(record.expiresAt<=Date.now()){
      this.store.transaction(()=>{this.store.remove('operation',record.operationId);this.inbox.tombstone({chat:record.chat,id:record.messageId,fromMe:true,...(isGroup(record.chat)?{participant:this.identity}:{})});});
      return this.operationView({...record,state:'cancelled',request:undefined});
    }
    this.store.set('operation',record.operationId,record,{expires:record.expiresAt,created:record.createdAt,tag:record.messageId});this.changed(record.chat);
    const job=(async()=>{
      try{
        const sent=await socket.sendMessage(record.chat,{text:input.text,linkPreview:null},{messageId:record.messageId,...(setting?{ephemeralExpiration:setting.seconds}:{})});
        if(this.closed||this.stopping||epoch!==this.epoch)throw Error('WhatsApp connection changed.');
        const sentWithKey=sent?{...sent,key:this.ownKey(sent.key)}:null;
        if(sentWithKey)await this.chatSync.capture(this.chatSync.snapshot([sentWithKey]));
        if(!this.current(epoch))throw Error('WhatsApp connection changed.');
        const clean=sentWithKey?sanitizeMessage(sentWithKey):null;if(clean)await this.ingest([clean],false,epoch);
        if(!this.current(epoch))throw Error('WhatsApp connection changed.');
        const latest=this.store.get('operation',record.operationId);if(latest){record={...latest,state:strongest(latest.state,'sent'),request:undefined};this.store.set('operation',record.operationId,record,{expires:record.expiresAt,created:record.createdAt,tag:record.messageId});}
      }catch{
        if(!this.closed&&!this.stopping){const latest=this.store.get('operation',record.operationId);if(latest){record={...latest,state:['sent','delivered','read'].includes(latest.state)?latest.state:'uncertain'};this.store.set('operation',record.operationId,record,{expires:record.expiresAt,created:record.createdAt,tag:record.messageId});}}
      }
      if(this.closed||this.stopping||epoch!==this.epoch)throw Error('WhatsApp connection changed.');this.changed(record.chat);return this.operationView(record);
    })();this.inflight.add(job);try{return await job;}finally{this.inflight.delete(job);}
  }
  async history(args){
    exact(args,['chat'],['before']);const chat=await this.canonical(assertChat(args.chat)),before=decodeCursor(args.before);
    const old=[...this.historyRequests.values()].find(r=>r.chat===chat&&r.before===args.before&&Date.now()-r.startedAt<30000);
    if(old){if(old.state==='loading'&&Date.now()-old.startedAt>20000)old.state='unavailable';return {state:old.state,...(old.hasMore!==undefined?{hasMore:old.hasMore}:{})};}
    if(this.state!=='connected'||!this.socket)return {state:'unavailable'};
    const oldest=this.store.list('message',{tag:chat,oldest:true,limit:1})[0]?.value||this.store.get('history-seed',chat);
    const anchor=before?this.store.list('message',{tag:chat}).map(r=>r.value).find(r=>r.id===before[1]&&r.createdAt===before[0]&&(!isGroup(chat)||before.length===4&&r.fromMe===before[2]&&r.participant===before[3])):oldest;
    if(!anchor||anchor.createdAt<=Date.now()-WEEK)return {state:'ready',hasMore:false};
    const epoch=this.epoch,requestId=await this.socket.fetchMessageHistory(20,this.protocolKey(anchor),anchor.createdAt);
    if(this.closed||epoch!==this.epoch)throw Error('WhatsApp connection changed.');
    this.historyRequests.set(requestId,{chat,before:args.before,startedAt:Date.now(),state:'loading'});for(const [id,r]of this.historyRequests)if(Date.now()-r.startedAt>60000)this.historyRequests.delete(id);
    return {state:'loading'};
  }
  async profiles(args){exact(args,['peers']);if(!Array.isArray(args.peers)||args.peers.length>20)throw Error('Invalid contact selection.');const peers=[];for(const peer of args.peers)peers.push(await this.canonical(assertChat(peer)));return this.avatars.request([...new Set(peers)]);}
  async request(method,args={}){
    this.active();if(!METHODS.has(method))throw Error('Unknown WhatsApp operation.');
    if(method==='status'){exact(args,[]);return this.status();}
    if(method==='pair')return this.pair(args);
    if(method==='disconnect'){exact(args,[]);return this.disconnect();}
    if(method==='prepareSend')return this.prepare(args);
    if(method==='send')return this.send(args);
    if(method==='prepareMutation')return this.mutations.prepare(args);
    if(method==='mutate')return this.mutations.run(args);
    if(method==='mutations'){exact(args,['chat']);return {mutations:this.mutations.list(assertChat(args.chat))};}
    if(method==='history')return this.history(args);
    if(method==='profiles')return this.profiles(args);
    if(method==='photo')return this.photos.request(args);
    if(method==='chats'){exact(args,[],['before','limit','view']);return this.inbox.chats(args);}
    if(method==='chat'){exact(args,['chat']);return this.inbox.chat(await this.canonical(assertChat(args.chat)));}
    if(method==='setArchived')return this.chatSync.setArchived(args);
    if(method==='read'||method==='markRead'){const epoch=this.epoch;await this.flushEvents(epoch);return this.chatSync.markRead(args);}
    if(method==='operation'){exact(args,['id']);if(!uuidPattern.test(args.id))throw Error('Invalid operation.');const row=this.store.get('operation',args.id),prior=this.store.get('operation-identity',args.id);return row?this.operationView(row):prior?{operationId:args.id,messageId:prior.messageId,state:'cancelled'}:null;}
    if(method==='operations'){exact(args,[],['limit']);const limit=args.limit??50;if(!Number.isSafeInteger(limit)||limit<1||limit>100)throw Error('Invalid operation page.');return {operations:this.store.list('operation',{limit}).map(r=>this.operationView(r.value))};}
    if(method==='binding'||method==='bindContact'){
      exact(args,method==='binding'?['chat']:['chat','binding']);const chat=assertPeer(args.chat);
      if(method==='binding')return this.store.get('contact-binding',chat);
      if(args.binding===null)this.store.remove('contact-binding',chat);else this.store.set('contact-binding',chat,validateBinding(args.binding,this.account,chat));return {ok:true};
    }
    exact(args,['chat'],['before','limit']);const chat=await this.canonical(assertChat(args.chat));
    if(method==='messages')return this.inbox.messages(chat,args);
    return this.inbox.read(chat,args.ids);
  }
  async disconnect(){
    if(this.disconnectTask)return this.disconnectTask;this.active();
    this.disconnectTask=this.finishDisconnect();try{return await this.disconnectTask;}finally{this.disconnectTask=null;}
  }
  async finishDisconnect(){
    this.active();this.store.set('system','auto-connect',false);const socket=this.socket;this.stopping=true;++this.epoch;clearTimeout(this.timer);this.timer=null;this.socket=null;this.state='disconnected';this.qr=null;this.pairCode=null;this.pairExpiresAt=null;let remoteUnlinked=false;
    this.photos.pause();this.avatars.pause();for(const finish of this.pairWaiters)finish(false);
    if(socket){try{remoteUnlinked=await bounded(Promise.resolve(socket.logout()).then(()=>true),3000,false);}catch{}}
    try{socket?.end(new Error('WhatsApp disconnected'));}catch{}
    await this.queue;this.store.transaction(()=>{this.store.clear('wa-auth');this.store.clear('wa-key');this.store.remove('system','identity');this.inbox.reset();this.chatSync.reset();this.groups.clear();});this.store.checkpoint();this.identity=null;this.historyRequests.clear();this.stopping=false;if(!this.closing)this.statusChanged();return {disconnected:true,remoteUnlinked};
  }
  async close(){
    if(this.closeTask)return this.closeTask;if(this.closed)return;this.closing=true;
    this.closeTask=this.finishClose();return this.closeTask;
  }
  async finishClose(){
    if(this.disconnectTask)await this.disconnectTask.catch(()=>{});this.stopping=true;clearInterval(this.pruner);this.pause('disconnected');
    await this.queue;await bounded(Promise.allSettled([...this.inflight]),3000,undefined);this.store.close();this.closed=true;
  }
}

export async function createEngine(options){const engine=new Engine(options);try{if(engine.store.get('system','auto-connect'))await engine.start();}catch(error){await engine.close();throw error;}return {request:(method,args)=>engine.request(method,args),close:()=>engine.close(),disconnect:()=>engine.disconnect()};}
