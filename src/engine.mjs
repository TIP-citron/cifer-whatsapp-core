import makeWASocket, { Browsers, DisconnectReason, WAMessageStubType, proto } from 'baileys';
import pino from 'pino';
import { Store } from './store.mjs';
import { Inbox, decodeCursor } from './inbox.mjs';
import { authStore } from './auth-store.mjs';
import { Avatars } from './avatars.mjs';
import { WEEK, assertPeer, canonicalJSON, digest, exact, extract, keyOf, messageId, normalizePeer, peerPattern, protectedWire, receipt, sanitizeMessage, strongest, uuidPattern, validateBinding, validatePrepared } from './content.mjs';

const logger=pino({level:'silent'});
async function bounded(promise,ms,fallback){let timer;try{return await Promise.race([promise,new Promise(resolve=>{timer=setTimeout(()=>resolve(fallback),ms);})]);}finally{clearTimeout(timer);}}
const safeClone=value=>JSON.parse(JSON.stringify(value));
const METHODS=new Set(['status','pair','chats','messages','history','send','prepareSend','operation','operations','read','markRead','profiles','binding','bindContact','disconnect']);

class Engine {
  constructor({directory,key,account,connection,onEvent=()=>{},socketFactory=makeWASocket,avatarFetch=globalThis.fetch}) {
    if(typeof directory!=='string'||!directory||typeof account!=='string'||!account||account.length>200||typeof connection!=='string'||!connection||connection.length>200||typeof onEvent!=='function')throw Error('Invalid engine configuration.');
    this.account=account;this.connection=connection;this.emit=onEvent;this.factory=socketFactory;
    this.store=new Store(directory,key,{account,connection});this.closed=false;this.stopping=false;this.epoch=0;this.socket=null;this.timer=null;this.attempts=0;
    this.state='disconnected';this.qr=null;this.pairCode=null;this.pairExpiresAt=null;this.identity=this.store.get('system','identity')||null;
    this.queue=Promise.resolve();this.queued=0;this.inflight=new Set();this.changedScheduled=false;this.metadataScheduled=false;this.pairBusy=false;this.pairWaiters=new Set();this.historyRequests=new Map();
    this.inbox=new Inbox(this.store,(chat,metadata)=>metadata?this.metadata(chat):this.changed(chat));
    this.avatars=new Avatars({store:this.store,socket:()=>this.socket,live:()=>!this.closed&&!this.stopping&&!this.closing&&this.state==='connected'&&!!this.socket,changed:chat=>this.metadata(chat),fetcher:avatarFetch});
    for(const {id,value}of this.store.list('operation'))if(value.state==='sending')this.store.set('operation',id,{...value,state:'uncertain'},{expires:value.expiresAt,created:value.createdAt,tag:value.messageId});
    this.inbox.prune();
    this.pruner=setInterval(()=>{if(this.closed)return;try{this.inbox.prune();}catch{this.pause('reconnect-required');}},60000);this.pruner.unref?.();
  }
  active(){if(this.closed||this.stopping||this.closing)throw Error('WhatsApp is locked.');}
  event(value){if(this.closed||this.stopping)return;try{this.emit(value);}catch{/* Consumer failures cannot expose transport errors. */}}
  status(){this.active();return {state:this.state,identity:this.identity,qr:this.qr&&this.pairExpiresAt>Date.now()?this.qr:null,pairCode:this.pairCode&&this.pairExpiresAt>Date.now()?this.pairCode:null,pairExpiresAt:this.pairExpiresAt||0,connection:this.connection,revision:this.store.revision(),...this.inbox.unread()};}
  statusChanged(){this.event({type:'status',status:this.status()});}
  changed(chat){
    if(this.changedScheduled||this.closed||this.stopping)return;this.changedScheduled=true;
    queueMicrotask(()=>{this.changedScheduled=false;if(!this.closed&&!this.stopping)this.event({type:'changed',revision:this.store.revision(),...(chat?{chat}:{})});});
  }
  metadata(chat){
    if(this.metadataScheduled||this.closed||this.stopping)return;this.metadataScheduled=true;
    queueMicrotask(()=>{this.metadataScheduled=false;if(!this.closed&&!this.stopping)this.event({type:'metadata',...(chat?{chat}:{})});});
  }
  enqueue(epoch,job){
    if(this.closed||this.stopping||epoch!==this.epoch)return;
    if(this.queued>=128){this.pause('reconnecting');this.scheduleReconnect();return;}
    this.queued++;
    this.queue=this.queue.then(async()=>{if(!this.closed&&!this.stopping&&epoch===this.epoch)await job();}).catch(()=>{if(!this.closed&&!this.stopping)this.event({type:'status',status:{...this.status(),error:'WhatsApp synchronization needs retry.'}});}).finally(()=>{this.queued--;});
  }
  pause(state='disconnected'){
    this.avatars.pause();
    ++this.epoch;clearTimeout(this.timer);this.timer=null;const socket=this.socket;this.socket=null;this.state=state;this.qr=null;this.pairCode=null;this.pairExpiresAt=null;
    for(const finish of this.pairWaiters)finish(false);
    try{socket?.end(new Error('Local session paused'));}catch{}
  }
  scheduleReconnect(){if(this.closed||this.stopping)return;clearTimeout(this.timer);this.timer=setTimeout(()=>{this.timer=null;void this.start().catch(()=>{if(!this.closed&&!this.stopping){this.state='reconnect-required';this.statusChanged();}});},Math.min(30000,1000*2**Math.min(this.attempts++,5)));this.timer.unref?.();}
  async canonical(value,alternate){
    const peer=normalizePeer(value);if(!peerPattern.test(peer))return null;
    if(peer.endsWith('@s.whatsapp.net'))return peer;
    let mapped=normalizePeer(alternate);
    if(!peerPattern.test(mapped)||!mapped.endsWith('@s.whatsapp.net'))mapped=this.store.get('alias',peer);
    if(!mapped&&this.socket?.signalRepository?.lidMapping) mapped=normalizePeer(await this.socket.signalRepository.lidMapping.getPNForLID(peer).catch(()=>null));
    if(mapped&&peerPattern.test(mapped)&&mapped.endsWith('@s.whatsapp.net')){
      if(this.store.get('alias',peer)!==mapped){this.store.set('alias',peer,mapped);this.inbox.rebind(peer,mapped);}return mapped;
    }
    return peer;
  }
  async cleanKey(key){const clean=keyOf(key);if(!clean)return null;const chat=await this.canonical(clean.chat,clean.alternate);return chat?{chat,id:clean.id,fromMe:clean.fromMe}:null;}
  async start(){
    this.active();if(this.socket)return this.status();
    this.store.set('system','auto-connect',true);const epoch=++this.epoch,live=()=>!this.closed&&!this.stopping&&epoch===this.epoch;
    const auth=authStore(this.store,live);this.state=auth.state.creds.registered?'reconnecting':'pairing';this.qr=null;this.pairCode=null;this.pairExpiresAt=null;
    const socket=this.factory({auth:auth.state,logger,browser:Browsers.macOS('CIFER Local'),markOnlineOnConnect:false,syncFullHistory:false,enableRecentMessageCache:false,generateHighQualityLinkPreview:false,
      shouldSyncHistoryMessage:h=>![proto.Message.HistorySyncType.FULL,proto.Message.HistorySyncType.RECENT].includes(h.syncType),
      getMessage:async key=>{if(!live())return undefined;const clean=await this.cleanKey(key);if(!live()||!clean||this.store.get('tombstone',messageId(clean)))return undefined;const row=this.store.get('message',messageId(clean));return row&&row.expiresAt>Date.now()&&row.fromMe?{conversation:row.wire||row.text||''}:undefined;}
    });this.socket=socket;
    socket.ev.on('creds.update',()=>{if(live())auth.save();});
    socket.ev.on('connection.update',update=>{
      if(!live())return;
      if(update.qr){this.qr=update.qr;this.pairExpiresAt=Date.now()+60000;this.state='pairing';this.statusChanged();}
      if(update.connection==='open'){
        const identity=normalizePeer(socket.user?.id||auth.state.creds.me?.id);
        if(!peerPattern.test(identity)||(this.identity&&this.identity!==identity)){this.pause('reconnect-required');this.store.set('system','auto-connect',false);this.statusChanged();return;}
        this.identity=identity;this.store.set('system','identity',identity);auth.save();this.state='connected';this.qr=null;this.pairCode=null;this.pairExpiresAt=null;this.attempts=0;this.statusChanged();
      }
      if(update.connection==='close'){
        const code=update.lastDisconnect?.error?.output?.statusCode;
        const invalid=[DisconnectReason.loggedOut,DisconnectReason.badSession,DisconnectReason.connectionReplaced,DisconnectReason.multideviceMismatch,DisconnectReason.forbidden].includes(code);
        this.pause(invalid?'reconnect-required':'reconnecting');
        if(invalid)this.store.set('system','auto-connect',false);else this.scheduleReconnect();this.statusChanged();
      }
    });
    socket.ev.on('contacts.upsert',rows=>this.enqueue(epoch,()=>this.contacts(rows)));
    socket.ev.on('contacts.update',rows=>this.enqueue(epoch,()=>this.contacts(rows)));
    socket.ev.on('chats.upsert',rows=>this.enqueue(epoch,()=>this.chatSettings(rows)));
    socket.ev.on('chats.update',rows=>this.enqueue(epoch,()=>this.chatSettings(rows)));
    socket.ev.on('messages.upsert',event=>{
      const values=event.messages.map(m=>sanitizeMessage(m)).filter(Boolean);
      this.enqueue(epoch,()=>this.ingest(values));
    });
    socket.ev.on('messages.update',rows=>{
      const clean=rows.map(({key,update})=>({key:keyOf(key),removed:update.message===null||update.messageStubType===WAMessageStubType.REVOKE,message:update.message?{normalizedContent:extract(update.message)}:undefined,at:Number(update.messageTimestamp||Date.now()/1000)*1000,status:update.status})).filter(r=>r.key);
      this.enqueue(epoch,async()=>{for(const row of clean){const key=await this.cleanKey({remoteJid:row.key.chat,remoteJidAlt:row.key.alternate,id:row.key.id,fromMe:row.key.fromMe});if(!key)continue;if(row.removed)this.inbox.tombstone(key);else if(row.message)this.inbox.edit(key,row.message,row.at);const state=receipt(row.status);if(state)this.inbox.delivery(key,state);}});
    });
    socket.ev.on('messages.delete',event=>this.enqueue(epoch,async()=>{
      if(event.keys)for(const raw of event.keys){const key=await this.cleanKey(raw);if(key)this.inbox.tombstone(key);}
      else if(event.all&&event.jid){const chat=await this.canonical(event.jid);if(chat)for(const {value}of this.store.list('message',{tag:chat}))this.inbox.tombstone(value);}
    }));
    socket.ev.on('message-receipt.update',rows=>this.enqueue(epoch,async()=>{for(const row of rows){const key=await this.cleanKey(row.key);if(!key?.fromMe)continue;const r=row.receipt,state=Number(r.readTimestamp)>0||Number(r.playedTimestamp)>0?'read':Number(r.receiptTimestamp)>0?'delivered':null;if(state)this.inbox.delivery(key,state);}}));
    socket.ev.on('messaging-history.set',event=>{
      const messages=(event.messages||[]).map(m=>sanitizeMessage(m)).filter(Boolean),contacts=(event.contacts||[]),chats=(event.chats||[]),requestId=event.peerDataRequestSessionId;
      this.enqueue(epoch,async()=>{
        await this.contacts(contacts);await this.chatSettings(chats);
        const request=requestId?this.historyRequests.get(requestId):null;
        if(request){const selected=[];for(const m of messages){if(await this.canonical(m.chat,m.alternate)===request.chat)selected.push(m);}await this.ingest(selected.slice(-20),true);request.state='ready';request.hasMore=selected.length>=20;this.changed(request.chat);}
        else {const peers=new Map(),selected=[];for(const m of messages.sort((a,b)=>b.createdAt-a.createdAt)){const chat=await this.canonical(m.chat,m.alternate);if(!chat)continue;if(!peers.has(chat)&&peers.size>=10)continue;const count=peers.get(chat)||0;if(count>=20)continue;peers.set(chat,count+1);selected.push(m);}await this.ingest(selected,true);}
      });
    });
    this.statusChanged();return this.status();
  }
  async contacts(rows){for(const row of rows.slice(0,10000)){const chat=await this.canonical(row.id||row.lid,row.phoneNumber);if(!chat)continue;this.inbox.contact(chat,{name:row.name||row.notify||row.verifiedName});if(Object.hasOwn(row,'imgUrl'))this.avatars.invalidate(chat,row.imgUrl==='removed'||row.imgUrl===null);}this.metadata();}
  async chatSettings(rows){for(const row of rows.slice(0,10000)){const chat=await this.canonical(row.id,row.pnJid);if(!chat)continue;const updatedAt=Math.min(Date.now(),Number(row.conversationTimestamp||0)*1000);this.inbox.contact(chat,{...(row.name?{name:row.name}:{}),...(Number.isSafeInteger(updatedAt)?{updatedAt}:{})});if(row.ephemeralExpiration!==undefined){const seconds=Number(row.ephemeralExpiration);if(Number.isSafeInteger(seconds)&&seconds>=0)this.store.set('setting',chat,{seconds});}const seed=row.messages?.[0]?.message,key=seed?keyOf(seed.key):null,at=Number(seed?.messageTimestamp)*1000;if(key&&Number.isSafeInteger(at)&&at>Date.now()-WEEK)this.store.set('history-seed',chat,{...key,chat,createdAt:at},{expires:at+WEEK,created:at});}this.changed();}
  async ingest(values,history=false){for(const row of values){const chat=await this.canonical(row.chat,row.alternate);if(!chat)continue;const candidates=row.fromMe?this.store.list('operation',{tag:row.id,limit:100}).map(r=>r.value):[];let op=null;for(const candidate of candidates)if(await this.canonical(candidate.chat)===chat){op=candidate;break;}const target=op?op.chat:chat;this.inbox.ingest({...row,chat:target},{history,operationId:op?.operationId});if(op){const expiresAt=Math.min(op.expiresAt,row.expiresAt);this.store.set('operation',op.operationId,{...op,expiresAt},{expires:expiresAt,created:op.createdAt,tag:op.messageId});this.inbox.delivery({chat:target,id:row.id,fromMe:true},receipt(row.status)||'sent');}}}
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
    const chat=assertPeer(args.chat);if(args.kind==='ordinary'){if(Buffer.byteLength(args.text)>2000||args.prepared!==undefined||args.text.startsWith('CIFER:WA:'))throw Error('Invalid ordinary message.');}
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
    const row={operationId:input.operationId,chat:input.chat,digest:hash,request:input,state:'prepared',messageId:'3EB0'+digest(input.operationId).slice(0,28).toUpperCase(),createdAt:now,expiresAt};
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
    const setting=this.store.get('setting',record.chat);
    record={...record,state:'sending',expiresAt:Math.min(record.expiresAt,setting?.seconds?record.createdAt+setting.seconds*1000:Infinity)};
    if(record.expiresAt<=Date.now()){
      this.store.transaction(()=>{this.store.remove('operation',record.operationId);this.inbox.tombstone({chat:record.chat,id:record.messageId,fromMe:true});});
      return this.operationView({...record,state:'cancelled',request:undefined});
    }
    this.store.set('operation',record.operationId,record,{expires:record.expiresAt,created:record.createdAt,tag:record.messageId});this.changed(record.chat);
    const job=(async()=>{
      try{
        const sent=await socket.sendMessage(record.chat,{text:input.text,linkPreview:null},{messageId:record.messageId,...(setting?{ephemeralExpiration:setting.seconds}:{})});
        if(this.closed||this.stopping||epoch!==this.epoch)throw Error('WhatsApp connection changed.');
        const clean=sent?sanitizeMessage(sent):null;if(clean)await this.ingest([clean]);
        const latest=this.store.get('operation',record.operationId);if(latest){record={...latest,state:strongest(latest.state,'sent'),request:undefined};this.store.set('operation',record.operationId,record,{expires:record.expiresAt,created:record.createdAt,tag:record.messageId});}
      }catch{
        if(!this.closed&&!this.stopping){const latest=this.store.get('operation',record.operationId);if(latest){record={...latest,state:['sent','delivered','read'].includes(latest.state)?latest.state:'uncertain'};this.store.set('operation',record.operationId,record,{expires:record.expiresAt,created:record.createdAt,tag:record.messageId});}}
      }
      if(this.closed||this.stopping||epoch!==this.epoch)throw Error('WhatsApp connection changed.');this.changed(record.chat);return this.operationView(record);
    })();this.inflight.add(job);try{return await job;}finally{this.inflight.delete(job);}
  }
  async history(args){
    exact(args,['chat'],['before']);const chat=await this.canonical(assertPeer(args.chat)),before=decodeCursor(args.before);
    const old=[...this.historyRequests.values()].find(r=>r.chat===chat&&r.before===args.before&&Date.now()-r.startedAt<30000);
    if(old){if(old.state==='loading'&&Date.now()-old.startedAt>20000)old.state='unavailable';return {state:old.state,...(old.hasMore!==undefined?{hasMore:old.hasMore}:{})};}
    if(this.state!=='connected'||!this.socket)return {state:'unavailable'};
    const oldest=this.store.list('message',{tag:chat,oldest:true,limit:1})[0]?.value||this.store.get('history-seed',chat);
    const anchor=before?this.store.list('message',{tag:chat}).map(r=>r.value).find(r=>r.id===before[1]&&r.createdAt===before[0]):oldest;
    if(!anchor||anchor.createdAt<=Date.now()-WEEK)return {state:'ready',hasMore:false};
    const epoch=this.epoch,requestId=await this.socket.fetchMessageHistory(20,{remoteJid:chat,id:anchor.id,fromMe:anchor.fromMe},anchor.createdAt);
    if(this.closed||epoch!==this.epoch)throw Error('WhatsApp connection changed.');
    this.historyRequests.set(requestId,{chat,before:args.before,startedAt:Date.now(),state:'loading'});for(const [id,r]of this.historyRequests)if(Date.now()-r.startedAt>60000)this.historyRequests.delete(id);
    return {state:'loading'};
  }
  async profiles(args){exact(args,['peers']);if(!Array.isArray(args.peers)||args.peers.length>20)throw Error('Invalid contact selection.');const peers=[];for(const peer of args.peers)peers.push(await this.canonical(assertPeer(peer)));return this.avatars.request([...new Set(peers)]);}
  async request(method,args={}){
    this.active();if(!METHODS.has(method))throw Error('Unknown WhatsApp operation.');
    if(method==='status'){exact(args,[]);return this.status();}
    if(method==='pair')return this.pair(args);
    if(method==='disconnect'){exact(args,[]);return this.disconnect();}
    if(method==='prepareSend')return this.prepare(args);
    if(method==='send')return this.send(args);
    if(method==='history')return this.history(args);
    if(method==='profiles')return this.profiles(args);
    if(method==='chats'){exact(args,[],['before','limit']);return this.inbox.chats(args);}
    if(method==='operation'){exact(args,['id']);if(!uuidPattern.test(args.id))throw Error('Invalid operation.');const row=this.store.get('operation',args.id),prior=this.store.get('operation-identity',args.id);return row?this.operationView(row):prior?{operationId:args.id,messageId:prior.messageId,state:'cancelled'}:null;}
    if(method==='operations'){exact(args,[],['limit']);const limit=args.limit??50;if(!Number.isSafeInteger(limit)||limit<1||limit>100)throw Error('Invalid operation page.');return {operations:this.store.list('operation',{limit}).map(r=>this.operationView(r.value))};}
    if(method==='binding'||method==='bindContact'){
      exact(args,method==='binding'?['chat']:['chat','binding']);const chat=assertPeer(args.chat);
      if(method==='binding')return this.store.get('contact-binding',chat);
      if(args.binding===null)this.store.remove('contact-binding',chat);else this.store.set('contact-binding',chat,validateBinding(args.binding,this.account,chat));return {ok:true};
    }
    exact(args,['chat'],method==='messages'?['before','limit']:['ids']);const chat=await this.canonical(assertPeer(args.chat));
    if(method==='messages')return this.inbox.messages(chat,args);
    return this.inbox.read(chat,args.ids);
  }
  async disconnect(){
    if(this.disconnectTask)return this.disconnectTask;this.active();
    this.disconnectTask=this.finishDisconnect();try{return await this.disconnectTask;}finally{this.disconnectTask=null;}
  }
  async finishDisconnect(){
    this.active();this.store.set('system','auto-connect',false);const socket=this.socket;this.stopping=true;++this.epoch;clearTimeout(this.timer);this.timer=null;this.socket=null;this.state='disconnected';this.qr=null;this.pairCode=null;this.pairExpiresAt=null;let remoteUnlinked=false;
    this.avatars.pause();for(const finish of this.pairWaiters)finish(false);
    if(socket){try{remoteUnlinked=await bounded(Promise.resolve(socket.logout()).then(()=>true),3000,false);}catch{}}
    try{socket?.end(new Error('WhatsApp disconnected'));}catch{}
    await this.queue;this.store.transaction(()=>{this.store.clear('wa-auth');this.store.clear('wa-key');this.store.remove('system','identity');this.inbox.reset();});this.store.checkpoint();this.identity=null;this.historyRequests.clear();this.stopping=false;if(!this.closing)this.statusChanged();return {disconnected:true,remoteUnlinked};
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
