import { WEEK, digest, isGroup, keyOf, messageId, normalizePeer, peerPattern } from './content.mjs';

const emoji = new RegExp('^(?:\\p{RGI_Emoji}|\\p{Extended_Pictographic}\\uFE0F?)$', 'v');
export const validReaction = text => typeof text==='string' && Buffer.byteLength(text)<=64 && (text==='' || emoji.test(text));
const identity = key => ({chat:key.chat,id:key.id,fromMe:key.fromMe,...(isGroup(key.chat)?{participant:key.participant}:{})});
const recordId = row => JSON.stringify([messageId(row),row.actor]);
const newer = (a,b) => !b || a.at>b.at || a.at===b.at && a.networkId>b.networkId;
const rawKey = key => key && {remoteJid:key.remoteJid,remoteJidAlt:key.remoteJidAlt,id:key.id,fromMe:key.fromMe,participant:key.participant,participantAlt:key.participantAlt};

/** Account-bound, encrypted reaction state. Empty reactions are ordering tombstones. */
export class Reactions {
  constructor(engine) { this.engine=engine;this.store=engine.store;this.inbox=engine.inbox; }
  snapshot(events) {
    const result=[];
    for(const event of (Array.isArray(events)?events:[]).slice(0,10000)) {
      const r=event?.reaction,at=Number(r?.senderTimestampMs);
      if(!r || !validReaction(r.text) || !Number.isSafeInteger(at) || at<=Date.now()-WEEK || at>Date.now()+60000)continue;
      const key=rawKey(event.key),envelope=rawKey(r.key);
      if(!keyOf(key)||!keyOf(envelope))continue;
      result.push({key,envelope,text:r.text,at});
    }
    return result;
  }
  messages(messages) {
    const events=[];
    for(const m of messages) {
      for(const r of (Array.isArray(m.reactions)?m.reactions:[]).slice(0,2048))events.push({key:m.key,reaction:r});
      // Upsert includes the authenticated envelope, even when the buffer folds its
      // derived event into an existing row. cleanMessage has normalized the target.
      let body=m.message;
      for(let i=0;i<8&&body?.ephemeralMessage;i++)body=body.ephemeralMessage.message;
      if(body?.reactionMessage)events.push({key:body.reactionMessage.key,reaction:{...body.reactionMessage,key:m.key}});
    }
    return this.snapshot(events);
  }
  async ingest(events,epoch=this.engine.epoch) {
    const e=this.engine;
    for(const value of events) {
      if(!e.current(epoch))return;
      const key=await e.cleanKey(value.key,epoch),source=keyOf(e.ownKey(value.envelope));
      if(!key||!source||!e.current(epoch))continue;
      const sourceChat=await e.canonical(source.chat,source.alternate,epoch);
      if(!e.current(epoch))return;if(sourceChat!==key.chat)continue;
      const actor=await e.canonical(source.fromMe?e.identity||e.socket?.user?.id:isGroup(key.chat)?source.participant:sourceChat,source.fromMe?e.socket?.user?.lid:source.participantAlt,epoch);
      if(!e.current(epoch))return;if(!peerPattern.test(actor||''))continue;
      this.apply({...identity(key),actor,text:value.text,at:value.at,networkId:source.id});
    }
  }
  save(row) { this.store.set('reaction',recordId(row),row,{expires:row.expiresAt,created:row.at,tag:messageId(row)}); }
  apply(value) {
    const tag=messageId(value),target=this.store.get('message',tag),id=recordId(value),old=this.store.get('reaction',id);
    if(this.store.get('tombstone',tag)||!newer(value,old))return;
    const expiresAt=target?.expiresAt??Math.min(Date.now()+120000,value.at+WEEK);
    if(expiresAt<=Date.now())return;
    if(!old&&(this.store.totals('reaction').count>=10000||this.store.list('reaction',{tag,limit:2048}).length>=2048))return;
    const row={...identity(value),actor:value.actor,text:value.text,at:value.at,networkId:value.networkId,expiresAt};
    this.inbox.trim(Buffer.byteLength(JSON.stringify(row))+256);
    // Trimming can remove the original; never leave its reaction behind.
    if(this.store.get('tombstone',tag))return;
    this.store.transaction(()=>{
      this.save(row);this.reconcile(row);
      if(target)this.inbox.revision(row.chat,'metadata',{id:row.id,participant:row.participant},true);
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
  forget(key) { for(const expired of [false,true])for(const {id}of this.store.list('reaction',{tag:messageId(key),limit:2048,expired}))this.store.remove('reaction',id); }
  rebind(old,person) {
    this.store.transaction(()=>{
      for(const {id,value}of this.store.list('reaction',{limit:10000})) {
        if(value.chat!==old&&value.participant!==old&&value.actor!==old)continue;
        this.store.remove('reaction',id);
        const next={...value,chat:value.chat===old?person:value.chat,actor:value.actor===old?person:value.actor,...(value.participant===old?{participant:person}:{})};
        if(this.store.get('tombstone',messageId(next)))continue;
        const target=this.store.get('message',messageId(next));if(target)next.expiresAt=target.expiresAt;
        if(newer(next,this.store.get('reaction',recordId(next))))this.save(next);
        this.inbox.revision(next.chat,'metadata',{id:next.id,participant:next.participant},true);
      }
    });
  }
}
