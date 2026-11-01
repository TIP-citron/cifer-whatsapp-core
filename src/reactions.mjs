import { historyDays, DAY } from './storage-policy.mjs';
import { WEEK, digest, isGroup, keyOf, messageId, normalizePeer, peerPattern } from './content.mjs';

import { encryptedReaction, decryptReaction, reactionAuthors } from './encrypted-reactions.mjs';

const emoji = new RegExp('^(?:\\p{RGI_Emoji}|\\p{Extended_Pictographic}\\uFE0F?)$', 'v');
export const validReaction = text => typeof text==='string' && Buffer.byteLength(text)<=64 && (text==='' || emoji.test(text));
const identity = key => ({chat:key.chat,id:key.id,fromMe:key.fromMe,...(isGroup(key.chat)?{participant:key.participant}:{})});
const recordId = row => JSON.stringify([messageId(row),row.actor]);
const pendingId = row => JSON.stringify([messageId(row),row.actor,row.networkId]);
const newer = (a,b) => !b || a.at>b.at || a.at===b.at && a.networkId>b.networkId;
const rawKey = key => key && {remoteJid:key.remoteJid,remoteJidAlt:key.remoteJidAlt,id:key.id,fromMe:key.fromMe,participant:key.participant,participantAlt:key.participantAlt};

/** Account-bound, encrypted reaction state. Empty reactions are ordering tombstones. */
export class Reactions {
  constructor(engine) { this.engine=engine;this.store=engine.store;this.inbox=engine.inbox; }
  snapshot(events) {
    const result=[];
    for(const event of (Array.isArray(events)?events:[]).slice(0,10000)) {
      const r=event?.reaction,at=Number(r?.senderTimestampMs);
      if(!r || !validReaction(r.text) || !Number.isSafeInteger(at) || at<=Date.now()-historyDays(this.store)*DAY || at>Date.now()+60000)continue;
      const key=rawKey(event.key),envelope=rawKey(r.key);
      if(!keyOf(key)||!keyOf(envelope))continue;
      result.push({key,envelope,text:r.text,at});
    }
    return result;
  }
  messages(messages) {
    const events=[];
    for(const m of messages) {
      // Aggregated reactions may omit the redundant enclosing conversation JID.
      // Do not infer group actors or overwrite an explicitly different chat.
      for(const r of (Array.isArray(m.reactions)?m.reactions:[]).slice(0,2048)) {
        const key=r?.key?{...r.key,remoteJid:r.key.remoteJid||m.key?.remoteJid}:null;
        // Compact phone aggregates can omit the false direction field. A group
        // actor is still explicitly authenticated by its participant identity.
        // Do not infer a missing participant or a direct-chat author.
        if(key&&key.fromMe==null&&isGroup(key.remoteJid)&&peerPattern.test(normalizePeer(key.participant)))key.fromMe=false;
        events.push({key:m.key,reaction:{...r,key}});
      }
      // Upsert includes the authenticated envelope, even when the buffer folds its
      // derived event into an existing row. cleanMessage has normalized the target.
      let body=m.message;
      for(let i=0;i<8&&body?.ephemeralMessage;i++)body=body.ephemeralMessage.message;
      if(body?.reactionMessage)events.push({key:body.reactionMessage.key,reaction:{...body.reactionMessage,key:m.key}});
    }
    return [...this.snapshot(events),...messages.map(m=>encryptedReaction(m,Date.now(),historyDays(this.store)*DAY)).filter(Boolean)];
  }
  async ingest(events,epoch=this.engine.epoch) {
    const e=this.engine;
    for(const value of events) {
      if(!e.current(epoch))return;
      if(value.encrypted){await this.ingestEncrypted(value,epoch);continue;}
      const key=await e.cleanKey(value.key,epoch),source=keyOf(e.ownKey(value.envelope));
      if(!key||!source||!e.current(epoch))continue;
      const sourceChat=await e.canonical(source.chat,source.alternate,epoch);
      if(!e.current(epoch))return;if(sourceChat!==key.chat)continue;
      const actor=await e.canonical(source.fromMe?e.identity||e.socket?.user?.id:isGroup(key.chat)?source.participant:sourceChat,source.fromMe?e.socket?.user?.lid:source.participantAlt,epoch);
      if(!e.current(epoch))return;if(!peerPattern.test(actor||''))continue;
      this.apply({...identity(key),actor,text:value.text,at:value.at,networkId:source.id});
    }
  }
  async authors(values,epoch) {
    const e=this.engine,all=reactionAuthors(values),mapping=e.socket?.signalRepository?.lidMapping;
    for(const author of [...all]){
      if(!e.current(epoch))return [];
      let timer;
      try{const mapped=await Promise.race([author.endsWith('@lid')?mapping?.getPNForLID?.(author):mapping?.getLIDForPN?.(author),new Promise(resolve=>{timer=setTimeout(()=>resolve(null),1000);timer.unref?.();})]);if(mapped)all.push(mapped);}catch{}finally{clearTimeout(timer);}
    }
    return reactionAuthors(all);
  }
  async ingestEncrypted(value,epoch) {
    const e=this.engine,source=keyOf(e.ownKey(value.envelope));if(!source)return;
    const chat=await e.canonical(source.chat,source.alternate,epoch);
    if(!e.current(epoch)||!chat)return;
    const actor=await e.canonical(source.fromMe?e.identity||e.socket?.user?.id:isGroup(chat)?source.participant:chat,source.fromMe?e.socket?.user?.lid:source.participantAlt,epoch);
    if(!e.current(epoch)||!peerPattern.test(actor||''))return;
    let participant;
    if(isGroup(chat)){
      participant=value.key.fromMe?actor:await e.canonical(value.key.participant,value.key.participantAlt,epoch);
      if(!e.current(epoch)||!peerPattern.test(participant||''))return;
    }
    const key={chat,id:value.key.id,fromMe:isGroup(chat)?participant===this.self():source.fromMe?value.key.fromMe:!value.key.fromMe,...(participant?{participant}:{})};
    const tag=messageId(key);if(this.store.get('tombstone',tag))return;
    const target=this.store.get('message',tag),expiresAt=Math.min(Date.now()+120000,target?.expiresAt||Infinity,value.at+historyDays(this.store)*DAY);
    if(expiresAt<=Date.now())return;
    const row={...key,actor,networkId:source.id,at:value.at,payload:value.payload,iv:value.iv,actorAliases:reactionAuthors([actor,...(source.fromMe?[e.socket?.user?.id,e.socket?.user?.lid]:isGroup(chat)?[source.participant,source.participantAlt]:[source.chat,source.alternate])]),expiresAt};
    if(await this.resolveEncrypted(row,epoch))return;
    if(!e.current(epoch)||expiresAt<=Date.now()||this.store.get('tombstone',tag))return;
    const id=pendingId(row),old=this.store.get('reaction-pending',id);
    if(!newer(row,old)||!old&&(this.store.totals('reaction-pending').count>=10000||this.store.list('reaction-pending',{tag,limit:2048}).length>=2048))return;
    this.inbox.trim(Buffer.byteLength(JSON.stringify(row))+256);
    if(!this.store.get('tombstone',tag))this.store.set('reaction-pending',id,row,{expires:expiresAt,created:row.at,tag});
  }
  async resolveEncrypted(row,epoch) {
    const e=this.engine,tag=messageId(row),target=this.store.get('message',tag);
    if(!target?.editSecret||target.expiresAt<=Date.now()||this.store.get('tombstone',tag))return false;
    const creators=await this.authors(target.fromMe?[e.identity,e.socket?.user?.id,e.socket?.user?.lid]:[isGroup(row.chat)?target.participant:row.chat],epoch);
    const actors=await this.authors([row.actor,...row.actorAliases],epoch);
    if(!e.current(epoch)||!this.store.get('message',tag)||this.store.get('tombstone',tag))return false;
    const reaction=decryptReaction(row,target.editSecret,creators,actors,Date.now(),historyDays(this.store)*DAY);
    if(!reaction||!validReaction(reaction.text))return false;
    this.apply({...row,...reaction});
    const pending=this.store.get('reaction-pending',pendingId(row));
    if(pending?.networkId===row.networkId)this.store.remove('reaction-pending',pendingId(row));
    return true;
  }
  async replay(chat,key,epoch=this.engine.epoch) {
    for(const {value}of this.store.list('reaction-pending',key?{tag:messageId(key),limit:2048}:{limit:10000})){
      if(!this.engine.current(epoch))return;
      if(value.chat===chat)await this.resolveEncrypted(value,epoch);
    }
  }
  save(row) { this.store.set('reaction',recordId(row),row,{expires:row.expiresAt,created:row.at,tag:messageId(row)}); }
  apply(value) {
    const tag=messageId(value),target=this.store.get('message',tag),id=recordId(value),old=this.store.get('reaction',id);
    if(this.store.get('tombstone',tag)||!newer(value,old))return;
    const expiresAt=target?.expiresAt??Math.min(Date.now()+120000,value.at+historyDays(this.store)*DAY);
    if(expiresAt<=Date.now())return;
    if(!old&&(this.store.totals('reaction').count>=10000||this.store.list('reaction',{tag,limit:2048}).length>=2048))return;
    const row={...identity(value),actor:value.actor,text:value.text,at:value.at,networkId:value.networkId,expiresAt};
    this.inbox.trim(Buffer.byteLength(JSON.stringify(row))+256);
    // Trimming can remove the original; never leave its reaction behind.
    if(this.store.get('tombstone',tag))return;
    this.store.transaction(()=>{
      this.save(row);this.reconcile(row);
      if(target)this.inbox.revision(row.chat,'metadata',{id:row.id,fromMe:row.fromMe,participant:row.participant},true);
    });
  }
  reconcile(row) {
    const self=this.self();if(!self||row.actor!==self)return;
    for(const {value:op} of this.store.list('mutation',{tag:messageId(row)})) {
      if(op.action!=='react'||!['prepared','sending','uncertain'].includes(op.state)||row.at<op.createdAt)continue;
      this.engine.mutations.save({...op,request:undefined,state:op.contentDigest===digest(row.text)?'sent':'cancelled'});
    }
  }
  nextTimestamp(key) { const old=this.store.get('reaction',recordId({...key,actor:this.self()}));return Math.max(Date.now(),(old?.at||0)+1); }
  self() { const id=normalizePeer(this.engine.identity||this.engine.socket?.user?.id);return this.store.get('alias',id)||id; }
  attach(key) {
    const tag=messageId(key),target=this.store.get('message',tag);
    if(!target||this.store.get('tombstone',tag)){this.forget(key);return;}
    this.store.transaction(()=>{for(const {value}of this.store.list('reaction',{tag,limit:2048}))if(value.expiresAt!==target.expiresAt)this.save({...value,expiresAt:target.expiresAt});});
  }
  view(key) {
    const target=this.store.get('message',messageId(key));if(!target)return [];
    const self=this.self(),groups=new Map();
    for(const {value:r}of this.store.list('reaction',{tag:messageId(key),limit:2048})) {
      if(!r.text)continue;const group=groups.get(r.text)||{emoji:r.text,count:0,mine:false};
      group.count++;group.mine||=r.actor===self;groups.set(r.text,group);
    }
    return [...groups.values()].sort((a,b)=>b.count-a.count||(a.emoji<b.emoji?-1:1));
  }
  participants(key) {
    if(!this.store.get('message',messageId(key)))return [];
    return this.store.list('reaction',{tag:messageId(key),limit:2048}).map(r=>r.value).filter(r=>!!r.text).map(r=>({
      id:r.actor,emoji:r.text,mine:r.actor===this.self(),name:r.actor===this.self()?'You':this.store.get('chat',r.actor)?.name||(r.actor.endsWith('@s.whatsapp.net')?'+'+r.actor.split('@')[0]:'WhatsApp participant')
    }));
  }
  forget(key) { for(const ns of ['reaction','reaction-pending'])for(const expired of [false,true])for(const {id}of this.store.list(ns,{tag:messageId(key),limit:2048,expired}))this.store.remove(ns,id); }
  rebind(old,person) {
    this.store.transaction(()=>{
      for(const ns of ['reaction','reaction-pending'])for(const {id,value}of this.store.list(ns,{limit:10000})) {
        if(value.chat!==old&&value.participant!==old&&value.actor!==old)continue;
        this.store.remove(ns,id);
        const next={...value,chat:value.chat===old?person:value.chat,actor:value.actor===old?person:value.actor,...(value.participant===old?{participant:person}:{})};
        if(this.store.get('tombstone',messageId(next)))continue;
        const target=this.store.get('message',messageId(next));if(target)next.expiresAt=ns==='reaction'?target.expiresAt:Math.min(next.expiresAt,target.expiresAt);
        const nextId=ns==='reaction'?recordId(next):pendingId(next);
        if(newer(next,this.store.get(ns,nextId))){if(ns==='reaction')this.save(next);else this.store.set(ns,nextId,next,{expires:next.expiresAt,created:next.at,tag:messageId(next)});}
        this.inbox.revision(next.chat,'metadata',{id:next.id,fromMe:next.fromMe,participant:next.participant},true);
      }
    });
  }
}
