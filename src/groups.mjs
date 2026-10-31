import { randomUUID } from 'node:crypto';
import { WEEK, MAX_MESSAGES, assertChat, isGroup, normalizePeer, peerPattern, messageId, canonicalJSON } from './content.mjs';

const DAY=86400000, ROSTERS=20, PARTICIPANTS=10000, PER_GROUP=2048, ROLE_BYTES=4*1024*1024;
const admin = value => value?.admin==='admin'||value?.admin==='superadmin'||value?.isAdmin===true||value?.isSuperAdmin===true;
const text = value => typeof value==='string'?value.slice(0,120):undefined;
const same = (a,b) => JSON.stringify(a)===JSON.stringify(b);

/** Group discovery and authorization stay inside the account-bound encrypted helper. */
export class Groups {
  constructor(engine) { this.engine=engine;this.store=engine.store;this.inbox=engine.inbox;this.discovery=null;this.discoveryEchoes=new WeakMap();this.fetches=new Map();this.session=randomUUID();this.versions=new Map();this.refreshQueue=new Map();this.refreshActive=0; }
  live(epoch,socket) { return !this.engine.closed&&!this.engine.stopping&&!this.engine.closing&&this.engine.epoch===epoch&&this.engine.socket===socket; }
  async bounded(promise,epoch,socket) {
    let timeout,poll;
    try{return await Promise.race([promise,new Promise((_,reject)=>{
      timeout=setTimeout(()=>reject(Error('Group synchronization timed out.')),8000);
      poll=setInterval(()=>{if(!this.live(epoch,socket))reject(Error('WhatsApp connection changed.'));},50);
    })]);}finally{clearTimeout(timeout);clearInterval(poll);}
  }
  cancelPrepared(chat) {
    for(const ns of ['operation','mutation'])for(const {id,value}of this.store.list(ns,{limit:MAX_MESSAGES})){
      if(value.chat!==chat||!['prepared','sending'].includes(value.state))continue;
      const next={...value,state:value.state==='prepared'?'cancelled':'uncertain',request:undefined};
      this.store.set(ns,id,next,{expires:value.expiresAt,created:value.createdAt,tag:ns==='operation'?value.messageId:messageId({chat,id:value.targetId,fromMe:value.fromMe,participant:value.participant})});
    }
    this.engine.changed(chat);
  }
  async canonicalParticipant(value,alternate) {
    const person=normalizePeer(value);if(!peerPattern.test(person))return null;
    const self=normalizePeer(this.engine.identity),me=this.engine.socket?.user;
    if([normalizePeer(me?.id),normalizePeer(me?.lid)].includes(person)&&peerPattern.test(self))return self;
    return await this.engine.canonical(person,alternate);
  }
  async people(rows) {
    if(!Array.isArray(rows)||rows.length>PER_GROUP)return null;
    const people=new Map();
    for(const raw of rows){
      const p=typeof raw==='string'?{id:raw}:raw;if(!p||typeof p!=='object')return null;
      const id=await this.canonicalParticipant(p.id||p.lid||p.phoneNumber,p.phoneNumber||p.lid);if(!id)return null;
      people.set(id,{id,admin:admin(p),...(text(p.name||p.notify)?{name:text(p.name||p.notify)}:{})});
    }
    return [...people.values()];
  }
  saveRoles(chat,admins,at=Date.now()) {
    const old=this.store.get('group-role',chat),intervals=(old?.intervals||[]).filter(x=>x.to===null||x.to>Date.now()-WEEK);
    const last=intervals.at(-1),sorted=[...new Set(admins)].sort(),epoch=this.engine.epoch,session=this.session;
    if(last?.to===null&&last.session===session&&last.epoch===epoch&&same(last.admins,sorted))return;
    if(last?.to===null)last.to=Math.floor(at/1000)*1000;
    intervals.push({from:at,to:null,admins:sorted,epoch,session});
    const row={intervals:intervals.slice(-128),checkedAt:at};
    while(this.store.totals('group-role').bytes+Buffer.byteLength(JSON.stringify(row))>ROLE_BYTES){const first=this.store.list('group-role',{oldest:true,limit:1})[0];if(!first)break;this.store.remove('group-role',first.id);}
    this.store.set('group-role',chat,row,{created:at,expires:at+WEEK});
  }
  cacheRoster(chat,people) {
    const at=Date.now();this.store.set('group-roster',chat,{participants:people,checkedAt:at,epoch:this.engine.epoch,session:this.session},{created:at,expires:at+DAY});
    let rows=this.store.list('group-roster',{oldest:true,limit:MAX_MESSAGES}),count=rows.reduce((n,r)=>n+r.value.participants.length,0);
    while(rows.length>ROSTERS||count>PARTICIPANTS){const row=rows.shift();count-=row.value.participants.length;this.store.remove('group-roster',row.id);}
  }
  async update(rows,{retainRoster=false,expectedVersions,valid=()=>true}={}) {
    const epoch=this.engine.epoch,socket=this.engine.socket,applied=new Set();
    for(const raw of (Array.isArray(rows)?rows:[]).slice(0,MAX_MESSAGES)){
      if(!valid()||!this.live(epoch,socket))break;
      const chat=normalizePeer(raw?.id);if(!isGroup(chat))continue;
      const previousVersion=this.versions.get(chat)||0;
      if(expectedVersions&&previousVersion!==(expectedVersions.get(chat)||0))continue;
      const version=previousVersion+1;this.versions.set(chat,version);
      const prior=this.store.get('chat',chat),fields={kind:'group'};let people;
      if(raw.subject!==undefined)fields.name=text(raw.subject);
      if(raw.announce!==undefined)fields.announce=raw.announce===true;
      if(raw.isCommunity!==undefined)fields.isCommunity=raw.isCommunity===true;
      if(raw.isCommunityAnnounce!==undefined)fields.isCommunityAnnounce=raw.isCommunityAnnounce===true;
      if(['pn','lid'].includes(raw.addressingMode))fields.addressingMode=raw.addressingMode;
      if(raw.readOnly!==undefined)fields.readOnly=raw.readOnly===true;
      if(raw.participants!==undefined){
        people=await this.people(raw.participants);if(!valid()||!this.live(epoch,socket))break;
        if(people){
          const self=await this.canonicalParticipant(this.engine.identity||socket?.user?.id,socket?.user?.lid);if(!valid()||!this.live(epoch,socket))break;
          const own=people.find(p=>p.id===self);Object.assign(fields,{member:!!own,readOnly:!own,ownAdmin:!!own?.admin,participantCount:people.length,metadataCheckedAt:Date.now()});
        }else{fields.member=undefined;fields.metadataCheckedAt=0;fields.sendRestriction='metadata-unavailable';}
      }
      if((this.versions.get(chat)||0)!==version)continue;
      if(people){this.saveRoles(chat,people.filter(p=>p.admin).map(p=>p.id));if(retainRoster)this.cacheRoster(chat,people);}
      const current={...prior,...fields},restricted=current.isCommunity||current.isCommunityAnnounce?'unsupported-group':current.member===false||current.readOnly===true?'left':current.member!==true||current.metadataCheckedAt===0?'metadata-unavailable':current.announce&&!current.ownAdmin?'admins-only':undefined;
      fields.canSend=!restricted;fields.sendRestriction=restricted||null;
      this.inbox.contact(chat,fields);
      if(['left','admins-only','unsupported-group'].includes(restricted))this.cancelPrepared(chat);
      if(raw.ephemeralDuration!==undefined){const seconds=Number(raw.ephemeralDuration);if(Number.isSafeInteger(seconds)&&seconds>=0)this.store.set('setting',chat,{seconds});}
      this.engine.metadata(chat);this.reconcilePending(chat);applied.add(chat);
    }
    return applied;
  }
  eventRows(rows,epoch,socket) {
    return (Array.isArray(rows)?rows:[]).flatMap(row=>{
      if(!Array.isArray(row?.participants))return [row];
      const request=this.discoveryEchoes.get(row.participants);
      if(request?.epoch!==epoch||request.socket!==socket||request.chat!==row.id)return [row];
      this.discoveryEchoes.delete(row.participants);
      if(request.fingerprint===canonicalJSON(row))return [];
      // A buffered partial update can share the discovery roster. Preserve only
      // changed fields; an announcement change cannot authorize that stale roster.
      const delta={id:row.id};
      for(const field of Object.keys(row))if(field!=='id'&&field!=='participants'&&canonicalJSON(row[field])!==canonicalJSON(request.snapshot[field]))delta[field]=row[field];
      // An active discovery will reconcile its version mismatch once; only a
      // late buffered echo needs to start reconciliation here.
      if(this.discovery?.epoch!==epoch||this.discovery.socket!==socket)this.scheduleRefresh(row.id);
      return Object.keys(delta).length>1?[delta]:[];
    });
  }
  async applyQueued(epoch,socket,job) {
    if(!this.live(epoch,socket))throw Error('WhatsApp connection changed.');
    if(typeof this.engine.enqueue!=='function')return job(()=>this.live(epoch,socket));
    let cancelled=false;
    const valid=()=>!cancelled&&this.live(epoch,socket);
    const pending=new Promise((resolve,reject)=>{
      const accepted=this.engine.enqueue(epoch,async()=>{
        if(!valid()){reject(Error('WhatsApp connection changed.'));return;}
        try{await job(valid);resolve();}catch(error){reject(error);}
      });
      if(accepted===false)reject(Error('WhatsApp connection changed.'));
    });
    try{return await this.bounded(pending,epoch,socket);}finally{cancelled=true;}
  }
  async discover() {
    const epoch=this.engine.epoch,socket=this.engine.socket;
    if(this.discovery?.epoch===epoch)return this.discovery.promise;
    if(!socket?.groupFetchAllParticipating)return;
    if(this.discoveryEpoch!==epoch){
      this.discoveryEpoch=epoch;this.store.clear('group-roster');
      for(const {value}of this.store.list('chat',{limit:MAX_MESSAGES}))if(isGroup(value.id))this.inbox.contact(value.id,{canSend:false,metadataCheckedAt:0,sendRestriction:value.member===false?'left':'metadata-unavailable'});
    }
    const request={epoch,socket,versions:new Map(this.versions),promise:null};this.discovery=request;
    request.promise=(async()=>{
      const query=Promise.resolve(socket.groupFetchAllParticipating()).then(result=>{
        for(const row of Object.values(result||{}))if(Array.isArray(row?.participants)){const fingerprint=canonicalJSON(row);this.discoveryEchoes.set(row.participants,{epoch,socket,chat:row.id,fingerprint,snapshot:JSON.parse(fingerprint)});}
        return result;
      });
      const result=await this.bounded(query,epoch,socket);if(!this.live(epoch,socket))return;
      const rows=Object.values(result||{});if(rows.length>MAX_MESSAGES)throw Error('Too many WhatsApp groups.');
      await this.applyQueued(epoch,socket,async valid=>{
        const applied=await this.update(rows,{expectedVersions:request.versions,valid});if(!valid())return;
        const ids=new Set(rows.map(r=>r.id));
        for(const row of rows)if(isGroup(row?.id)&&!applied.has(row.id))this.scheduleRefresh(row.id);
        for(const {value}of this.store.list('chat',{limit:MAX_MESSAGES}))if(isGroup(value.id)&&value.member===true&&!ids.has(value.id)&&(this.versions.get(value.id)||0)===(request.versions.get(value.id)||0)){
          this.versions.set(value.id,(this.versions.get(value.id)||0)+1);
          this.inbox.contact(value.id,{member:false,canSend:false,sendRestriction:'left'});this.store.remove('group-roster',value.id);this.cancelPrepared(value.id);this.engine.metadata(value.id);
        }
      });
    })();
    try{return await request.promise;}finally{if(this.discovery===request)this.discovery=null;}
  }
  async refresh(chat) {
    assertChat(chat);if(!isGroup(chat))return null;
    const epoch=this.engine.epoch,socket=this.engine.socket,old=this.fetches.get(chat),version=this.versions.get(chat)||0;
    if(old?.epoch===epoch&&old.version===version)return old.promise;
    if(!socket?.groupMetadata||this.engine.state!=='connected')throw Error('Reconnect WhatsApp to refresh this group.');
    const promise=(async()=>{const row=await this.bounded(socket.groupMetadata(chat),epoch,socket);if(!this.live(epoch,socket))throw Error('WhatsApp connection changed.');if((this.versions.get(chat)||0)!==version)throw Error('Group membership changed. Retry.');if(row?.id!==chat||!Array.isArray(row.participants))throw Error('Group information is unavailable.');const applied=await this.update([row],{retainRoster:true,expectedVersions:new Map([[chat,version]])});if(!applied.has(chat)||(this.versions.get(chat)||0)!==version+1)throw Error('Group membership changed. Retry.');if(!this.live(epoch,socket))throw Error('WhatsApp connection changed.');return this.store.get('chat',chat);})();
    this.fetches.set(chat,{epoch,version,promise});try{return await promise;}finally{if(this.fetches.get(chat)?.promise===promise)this.fetches.delete(chat);}
  }
  scheduleRefresh(chat) {
    this.refreshQueue.set(chat,this.engine.epoch);
    if(this.refreshQueue.size>100)this.refreshQueue.delete(this.refreshQueue.keys().next().value);
    queueMicrotask(()=>this.pumpRefresh());
  }
  pumpRefresh() {
    while(this.refreshActive<2&&this.refreshQueue.size){
      const [chat,epoch]=this.refreshQueue.entries().next().value;this.refreshQueue.delete(chat);
      if(this.engine.closed||this.engine.stopping||this.engine.closing||epoch!==this.engine.epoch)continue;
      this.refreshActive++;
      void this.refresh(chat).catch(()=>{}).finally(()=>{this.refreshActive--;this.pumpRefresh();});
    }
  }
  async authorizeSend(chat) {
    if(!isGroup(chat))return;
    const row=await this.refresh(chat);if(!row?.canSend)throw Error(row?.sendRestriction==='admins-only'?'Only group admins can send messages.':'This group is read-only or unavailable.');
  }
  async participants(event) {
    const chat=normalizePeer(event?.id),epoch=this.engine.epoch,socket=this.engine.socket;
    if(!isGroup(chat)||!['add','remove','promote','demote','modify'].includes(event.action))return;
    this.versions.set(chat,(this.versions.get(chat)||0)+1);
    this.inbox.contact(chat,{canSend:false,metadataCheckedAt:0,sendRestriction:'metadata-unavailable'});
    const people=await this.people(event.participants);if(!this.live(epoch,socket))return;
    this.store.remove('group-roster',chat);
    if(!people){this.inbox.contact(chat,{canSend:false,sendRestriction:'metadata-unavailable',metadataCheckedAt:0});this.engine.metadata(chat);this.scheduleRefresh(chat);return;}
    const previous=this.store.get('chat',chat)||{},last=this.store.get('group-role',chat)?.intervals?.at(-1),admins=new Set(last?.session===this.session&&last.epoch===epoch&&last.to===null?last.admins:[]);
    const self=await this.canonicalParticipant(this.engine.identity||socket?.user?.id,socket?.user?.lid);if(!this.live(epoch,socket))return;
    const fields={metadataCheckedAt:0,canSend:false,sendRestriction:'metadata-unavailable'};
    for(const person of people){
      if(event.action==='promote')admins.add(person.id);else if(['demote','remove'].includes(event.action))admins.delete(person.id);
      if(person.id===self){if(event.action==='remove')Object.assign(fields,{member:false,readOnly:true,ownAdmin:false,sendRestriction:'left'});if(event.action==='add')Object.assign(fields,{member:true,readOnly:false});if(event.action==='promote')fields.ownAdmin=true;if(event.action==='demote')fields.ownAdmin=false;}
    }
    if(last?.session===this.session&&last.epoch===epoch&&last.to===null)this.saveRoles(chat,[...admins]);
    if(Number.isInteger(previous.participantCount)&&['add','remove'].includes(event.action))fields.participantCount=Math.max(0,previous.participantCount+(event.action==='add'?1:-1)*people.length);
    this.inbox.contact(chat,fields);if(fields.member===false||fields.ownAdmin===false&&previous.announce)this.cancelPrepared(chat);this.engine.metadata(chat);
    this.reconcilePending(chat);this.scheduleRefresh(chat);
  }
  roleAt(chat,actor,at) {
    if(!Number.isSafeInteger(at))return false;
    return (this.store.get('group-role',chat)?.intervals||[]).some(r=>r.session===this.session&&r.epoch===this.engine.epoch&&r.from<=at&&(r.to===null||at<r.to)&&r.admins.includes(actor));
  }
  async authorizeMutation(change) {
    if(!isGroup(change?.chat))return change;
    if(!change.envelope)return null;
    const epoch=this.engine.epoch,socket=this.engine.socket,actor=await this.canonicalParticipant(change.participant,change.participantAlt);
    if(!actor||!this.live(epoch,socket))return null;
    let targetPerson=change.targetParticipant?await this.canonicalParticipant(change.targetParticipant,change.targetParticipantAlt):actor;
    if(change.removed&&change.envelope&&!change.targetParticipant){const candidates=this.store.list('message',{tag:change.chat,limit:MAX_MESSAGES}).map(r=>r.value).filter(r=>r.id===change.id);if(candidates.length===1)targetPerson=candidates[0].participant;else if(candidates.length>1)return null;}
    if(!targetPerson||!this.live(epoch,socket))return null;
    const self=await this.canonicalParticipant(this.engine.identity||socket?.user?.id,socket?.user?.lid);if(!this.live(epoch,socket))return null;
    const key={chat:change.chat,id:change.id,fromMe:targetPerson===self,participant:targetPerson};
    if(!change.removed&&targetPerson!==actor)return null;
    if(change.removed&&targetPerson!==actor){
      if(!this.roleAt(change.chat,actor,change.envelopeAt)||!this.store.get('message',messageId(key))){
        this.deferMutation(key,actor,change.envelopeAt);return null;
      }
    }
    return {...change,...key,participantAlt:undefined,actor};
  }
  deferMutation(key,actor,at) {
    const id=messageId(key);
    if(Number.isSafeInteger(at)&&at>Date.now()-WEEK&&at<=Date.now()+60000&&(this.store.get('group-pending-mutation',id)||this.store.totals('group-pending-mutation').count<MAX_MESSAGES))this.store.set('group-pending-mutation',id,{...key,actor,at,removed:true,expiresAt:Math.min(Date.now()+120000,at+WEEK)},{created:at,expires:Math.min(Date.now()+120000,at+WEEK),tag:key.chat});
  }
  reconcilePending(chat) {
    if(!isGroup(chat))return;
    for(const {id,value} of this.store.list('group-pending-mutation',{tag:chat,limit:MAX_MESSAGES})){
      if(!this.roleAt(chat,value.actor,value.at)||!this.store.get('message',messageId(value)))continue;
      this.inbox.tombstone(value);this.store.remove('group-pending-mutation',id);
    }
  }
  clear() { this.discovery=null;this.fetches.clear();this.refreshQueue.clear();for(const ns of ['group-roster','group-role','group-pending-mutation'])this.store.clear(ns); }
}
