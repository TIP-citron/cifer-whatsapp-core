import { WEEK, MAX_BYTES, MAX_MESSAGES, messageId, extract, receipt, strongest } from './content.mjs';

const compare = (a, b) => b.createdAt - a.createdAt || b.id.localeCompare(a.id);
export const encodeCursor = row => Buffer.from(JSON.stringify([row.createdAt ?? row.updatedAt, row.id])).toString('base64url');
export function decodeCursor(value) {
  if (value == null) return null;
  if (typeof value !== 'string' || value.length > 512) throw Error('Invalid history cursor.');
  let result; try { result = JSON.parse(Buffer.from(value, 'base64url').toString('utf8')); } catch { throw Error('Invalid history cursor.'); }
  if (!Array.isArray(result) || result.length !== 2 || !Number.isSafeInteger(result[0]) || typeof result[1] !== 'string' || result[1].length > 128) throw Error('Invalid history cursor.');
  return result;
}
const pageLimit = (value, fallback) => { const limit = value ?? fallback; if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100) throw Error('Invalid page size.'); return limit; };

export class Inbox {
  constructor(store, changed, limits = {}) { this.store = store; this.changed = changed; this.maxMessages=Math.min(MAX_MESSAGES,limits.maxMessages??MAX_MESSAGES);this.maxBytes=Math.min(MAX_BYTES,limits.maxBytes??MAX_BYTES); }
  revision(chat, type = 'upsert', key = {}) {
    const revision = this.store.next();
    this.store.set('change', String(revision), { revision, type, chat, ...key }, { expires: Date.now() + WEEK, created: revision });
    this.changed(chat); return revision;
  }
  contact(chat, value = {}) {
    const old = this.store.get('chat', chat) || { id: chat, name: chat.endsWith('@s.whatsapp.net') ? '+' + chat.split('@')[0] : 'WhatsApp contact', updatedAt: 0 };
    const name = typeof value.name === 'string' && value.name.trim() ? value.name.slice(0, 120) : old.name;
    const row = { ...old, ...value, id: chat, name, updatedAt: Math.max(old.updatedAt || 0, value.updatedAt || 0) };
    if (chat.endsWith('@s.whatsapp.net')) row.phone = '+' + chat.split('@')[0];
    this.store.set('chat', chat, row, { created: row.updatedAt }); return row;
  }
  ingest(clean, { history = false, operationId } = {}) {
    const id = messageId(clean), now = Date.now();
    if (clean.expiresAt <= now || clean.createdAt <= now - WEEK || this.store.get('tombstone', id)) return false;
    const old = this.store.get('message', id), state = receipt(clean.status) || 'sent';
    if (old) {
      const expiresAt = Math.min(old.expiresAt, clean.expiresAt), delivery = strongest(old.delivery, state);
      if (old.expiresAt !== expiresAt || old.delivery !== delivery) this.save({ ...old, expiresAt, delivery });
      return false;
    }
    const row = { id: clean.id, chat: clean.chat, fromMe: clean.fromMe, createdAt: clean.createdAt, expiresAt: clean.expiresAt,
      kind: clean.kind, ...(clean.text !== undefined ? { text:clean.text } : {}), ...(clean.wire ? { wire:clean.wire } : {}), ...(clean.label ? { label:clean.label } : {}),
      delivery: state, readAt: clean.fromMe || history ? now : null, editedAt: null, ...(operationId ? { operationId } : {}) };
    this.store.transaction(() => { this.save(row); this.contact(row.chat, { updatedAt: row.createdAt, ...(!row.fromMe && clean.name ? { name:clean.name } : {}) }); });
    this.trim(); return true;
  }
  save(row) {
    row.revision = this.revision(row.chat, 'upsert', { id:row.id });
    this.store.set('message', messageId(row), row, { expires:row.expiresAt, created:row.createdAt, tag:row.chat, unread:!row.fromMe&&!row.readAt });
  }
  tombstone(key, now = Date.now()) {
    const id = messageId(key);
    this.store.transaction(() => {
      this.store.remove('message', id); this.store.remove('retry', id);
      this.store.set('tombstone', id, { ...key, deleted:true }, { expires:now + WEEK, created:now, tag:key.chat });
      for (const {id:opId,value} of this.store.list('operation', {tag:key.id})) {
        if(!key.fromMe||value.chat!==key.chat)continue;
        const next={...value,request:undefined,deleted:true}; this.store.set('operation',opId,next,{expires:value.expiresAt,created:value.createdAt,tag:key.id});
      }
      this.revision(key.chat, 'delete', { id:key.id });
    });
  }
  edit(key, body, editedAt) {
    const id = messageId(key), now = Date.now(), old = this.store.get('message', id), content = Object.hasOwn(body||{},'normalizedContent')?body.normalizedContent:extract(body);
    if (!old || !content) { this.tombstone(key); return; }
    if (this.store.get('tombstone', id) || !Number.isSafeInteger(editedAt) || editedAt > now + 60000 || editedAt <= (old.editedAt || 0)) return;
    const expiresAt = Math.min(old.expiresAt, content.expiration ? old.createdAt + content.expiration * 1000 : Infinity);
    if (expiresAt <= now) { this.tombstone(key); return; }
    this.store.transaction(()=>{
      this.store.remove('retry',id);
      // The original body/grants must not survive an edit in the recoverable outbox.
      for(const {id:opId,value}of this.store.list('operation',{tag:key.id}))if(key.fromMe&&value.chat===key.chat)this.store.set('operation',opId,{...value,request:undefined,state:value.state==='prepared'?'cancelled':value.state},{expires:Math.min(value.expiresAt,expiresAt),created:value.createdAt,tag:key.id});
      this.save({ ...old, text:content.text, wire:content.wire, label:content.label, kind:content.kind, expiresAt, editedAt });
    });
  }
  delivery(key, state) {
    const id=messageId(key),old=this.store.get('message',id);
    if (old) { const delivery=strongest(old.delivery,state); if(delivery!==old.delivery)this.save({...old,delivery}); }
    for(const {id:opId,value} of this.store.list('operation',{tag:key.id})) {
      if(!key.fromMe||value.chat!==key.chat)continue;
      if (!['prepared','failed'].includes(value.state)) {
        const delivery=strongest(value.state,state);
        this.store.set('operation',opId,{...value,state:delivery,...(['sent','delivered','read'].includes(delivery)?{request:undefined}:{})},{expires:value.expiresAt,created:value.createdAt,tag:key.id});
      }
    }
  }
  messages(chat, args = {}) {
    const before=decodeCursor(args.before),limit=pageLimit(args.limit,20),now=Date.now();
    let rows=this.store.list('message',{tag:chat,now,limit:MAX_MESSAGES}).map(r=>r.value).filter(r=>r.createdAt>now-WEEK&&r.expiresAt>now).sort(compare);
    // Prepared protected sends survive a crash before publication/network transmission.
    for (const {value:op} of this.store.list('operation',{limit:MAX_MESSAGES})) {
      if (op.chat!==chat||!op.request||op.deleted||rows.some(m=>m.id===op.messageId)||!['prepared','sending','uncertain'].includes(op.state)) continue;
      rows.push({id:op.messageId,chat,fromMe:true,kind:op.request.kind,...(op.request.kind==='protected'?{wire:op.request.text}:{text:op.request.text}),createdAt:op.createdAt,expiresAt:op.expiresAt,delivery:op.state==='uncertain'?'uncertain':'queued',operationId:op.operationId});
    }
    rows.sort(compare);if(before)rows=rows.filter(r=>r.createdAt<before[0]||r.createdAt===before[0]&&r.id<before[1]);
    const page=rows.slice(0,limit);return {messages:page.map(({readAt,revision,...r})=>r),cursor:rows.length>limit?encodeCursor(page.at(-1)):null};
  }
  chats(args = {}) {
    const before=decodeCursor(args.before),limit=pageLimit(args.limit,10),rows=[],stats=this.store.messageStats();
    for(const {value:chat} of this.store.list('chat',{limit:MAX_MESSAGES})) {
      const stat=stats.get(this.store.hash(chat.id)),updatedAt=Math.max(chat.updatedAt,stat?.createdAt||0);
      rows.push({...chat,updatedAt,unread:stat?.unread||0});
    }
    rows.sort((a,b)=>b.updatedAt-a.updatedAt||b.id.localeCompare(a.id));
    const filtered=before?rows.filter(r=>r.updatedAt<before[0]||r.updatedAt===before[0]&&r.id<before[1]):rows,page=filtered.slice(0,limit);
    return {chats:page.map(chat=>{const last=this.store.list('message',{tag:chat.id,limit:1})[0]?.value;return {...chat,...(last?{lastMessage:last.kind==='protected'?'CIFER protected message':last.text||last.label||'Message'}:{})};}),cursor:filtered.length>limit?encodeCursor(page.at(-1)):null};
  }
  unread() {
    const stats=this.store.messageStats(),chats=[];let total=0;
    for(const {value:chat} of this.store.list('chat',{limit:MAX_MESSAGES})){const stat=stats.get(this.store.hash(chat.id));if(stat?.unread){total+=stat.unread;if(chats.length<100)chats.push({chat:chat.id,count:stat.unread,createdAt:stat.createdAt});}}
    return {unreadTotal:total,unreadChats:chats};
  }
  read(chat, ids) {
    if(ids!==undefined&&(!Array.isArray(ids)||ids.length>100||ids.some(x=>typeof x!=='string')))throw Error('Invalid message selection.');
    this.store.transaction(()=>{for(const {value:row} of this.store.list('message',{tag:chat,limit:MAX_MESSAGES}))if(!row.fromMe&&!row.readAt&&(!ids||ids.includes(row.id)))this.save({...row,readAt:Date.now()});});
    return {ok:true};
  }
  rebind(old, chat) {
    if(old===chat)return;
    this.store.transaction(()=>{
      // Apply deletions first, including deletion of an already-existing canonical copy.
      for(const {id,value}of this.store.list('tombstone',{tag:old,limit:30000})){this.store.remove('tombstone',id);this.tombstone({...value,chat});}
      for(const {id,value}of this.store.list('message',{tag:old,limit:MAX_MESSAGES})){
        const target={...value,chat},targetId=messageId(target);this.store.remove('message',id);this.store.remove('retry',id);
        if(this.store.get('tombstone',targetId))continue;
        const existing=this.store.get('message',targetId);
        const moved=existing?{...(Number(value.editedAt)>Number(existing.editedAt)?target:existing),chat,
          createdAt:Math.min(value.createdAt,existing.createdAt),expiresAt:Math.min(value.expiresAt,existing.expiresAt),
          readAt:existing.readAt||value.readAt||null,delivery:strongest(existing.delivery,value.delivery)}:target;
        if(moved.expiresAt<=Date.now()){this.tombstone(moved);continue;}this.save(moved);
      }
      for(const {id,value}of this.store.list('operation',{limit:10000}))if(value.chat===old){
        // Alias discovery cannot silently retarget a prepared protected contact/grant.
        this.store.set('operation',id,{...value,chat,request:undefined,state:value.state==='prepared'?'cancelled':value.state},{expires:value.expiresAt,created:value.createdAt,tag:value.messageId});
      }
      this.store.remove('contact-binding',old);
      const setting=this.store.get('setting',old);if(setting&&!this.store.get('setting',chat))this.store.set('setting',chat,setting);this.store.remove('setting',old);
      const seed=this.store.get('history-seed',old);if(seed&&!this.store.get('history-seed',chat))this.store.set('history-seed',chat,{...seed,chat},{created:seed.createdAt,expires:seed.createdAt+WEEK});this.store.remove('history-seed',old);
      const previous=this.store.get('chat',old),canonical=this.store.get('chat',chat);if(previous){this.contact(chat,{...previous,...(canonical?.name&&canonical.name!=='WhatsApp contact'&&!canonical.name.startsWith('+')?{name:canonical.name}:{})});this.store.remove('chat',old);}
      this.revision(chat,'reset');
    });
  }
  trim(reserveBytes = 0) {
    const operations=this.store.totals('operation').bytes+this.store.totals('retry').bytes;
    if(operations+reserveBytes>this.maxBytes)throw Error('Local message and outbox storage is full.');
    let {count,bytes}=this.store.totals('message');bytes+=operations+reserveBytes;
    if(count>this.maxMessages||bytes>this.maxBytes)for(const {value:row} of this.store.list('message',{oldest:true,limit:MAX_MESSAGES+100})){
      if(count<=this.maxMessages&&bytes<=this.maxBytes)break;
      const size=Buffer.byteLength(JSON.stringify({id:messageId(row),value:row}));this.tombstone(row);count--;bytes-=size;
    }
    const changes=this.store.list('change',{limit:30000,oldest:true});for(const {id} of changes.slice(0,Math.max(0,changes.length-20000)))this.store.remove('change',id);
    if(this.store.totals('tombstone').count>30000)throw Error('Message lifecycle storage is full.');
  }
  prune() {
    for(const {value:row} of this.store.list('message',{expired:true,limit:MAX_MESSAGES}))this.tombstone(row);
    this.store.prune();this.trim();this.store.checkpoint();
  }
  reset() { this.store.transaction(()=>{for(const ns of ['message','tombstone','retry','change','operation','operation-identity','chat','alias','setting','history','history-seed','contact-binding'])this.store.clear(ns);this.revision(null,'reset');});if(!this.store.depth)this.store.checkpoint(); }
}
