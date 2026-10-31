import { WEEK, MAX_BYTES, MAX_MESSAGES, EDIT_WINDOW, messageId, messageActions, digest, extract, receipt, strongest } from './content.mjs';

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
  constructor(store, changed, limits = {}) { this.store = store; this.changed = changed; this.maxMessages=Math.min(MAX_MESSAGES,limits.maxMessages??MAX_MESSAGES);this.maxBytes=Math.min(MAX_BYTES,limits.maxBytes??MAX_BYTES);
    // Older candidates sometimes passed a complete message to tombstone(). Strip
    // any retained content while preserving deletion identity on upgrade.
    if(!store.get('system','content-free-tombstones-v1')) store.transaction(()=>{
      for(const {id,value} of store.list('tombstone')) store.set('tombstone',id,{chat:value.chat,id:value.id,fromMe:value.fromMe,deleted:true},{expires:Date.now()+WEEK,created:Date.now(),tag:value.chat});
      store.set('system','content-free-tombstones-v1',true);
    });
    // Earlier versions saved READ delivery from another linked device without
    // clearing the incoming unread bit. Repair only those confirmed receipts.
    if(!store.get('system','incoming-read-state-v1')) store.transaction(()=>{
      for(const {value:row} of store.list('message',{limit:MAX_MESSAGES}))
        if(!row.fromMe&&!row.readAt&&row.delivery==='read')this.save({...row,readAt:Date.now()},true);
      store.set('system','incoming-read-state-v1',true);
    });
  }
  revision(chat, type = 'upsert', key = {}, metadata = false) {
    const revision = this.store.next();
    this.store.set('change', String(revision), { revision, type, chat, ...key }, { expires: Date.now() + WEEK, created: revision });
    this.changed(chat,metadata); return revision;
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
    const expiredEdit=this.store.get('encrypted-edit',id,0);
    if(expiredEdit?.expiresAt<=now){this.tombstone(clean);return false;}
    const old = this.store.get('message', id), state = receipt(clean.status) || 'sent';
    const pendingRead=this.store.get('read-receipt',id), knownRead=state==='read'||!!pendingRead;
    if (old) {
      if(!old.editSecret&&clean.editSecret){old.editSecret=clean.editSecret;this.save(old);this.trim();}
      if(clean.editedAt)this.edit(old,{normalizedContent:clean},clean.editedAt);
      const current=this.store.get('message',id);if(!current)return false;
      const expiresAt = Math.min(current.expiresAt, clean.expiresAt), delivery = strongest(current.delivery, knownRead ? 'read' : state);
      const readAt=current.readAt||(!current.fromMe&&knownRead?now:null);
      if (current.expiresAt !== expiresAt || current.delivery !== delivery || current.readAt !== readAt) this.save({ ...current, expiresAt, delivery, readAt });
      if(pendingRead)this.store.remove('read-receipt',id);
      return false;
    }
    const row = { id: clean.id, chat: clean.chat, fromMe: clean.fromMe, createdAt: clean.createdAt, expiresAt: clean.expiresAt,
      kind: clean.kind, ...(clean.text !== undefined ? { text:clean.text } : {}), ...(clean.wire ? { wire:clean.wire } : {}), ...(clean.label ? { label:clean.label } : {}),
      editSecret:clean.editSecret, delivery: knownRead ? 'read' : state, readAt: clean.fromMe || history || knownRead ? now : null, editedAt: clean.editedAt || null, ...(operationId ? { operationId } : {}) };
    this.store.transaction(() => { this.save(row); if(pendingRead)this.store.remove('read-receipt',id); this.contact(row.chat, { updatedAt: row.createdAt, ...(!row.fromMe && clean.name ? { name:clean.name } : {}) });
      const pending = this.store.get('pending-edit', id);
      if (pending) { this.edit(row, { normalizedContent: pending.content }, pending.at); this.store.remove('pending-edit', id); }
    });
    this.trim(); return true;
  }
  save(row, metadata = false) {
    row.revision = this.revision(row.chat, metadata?'metadata':'upsert', { id:row.id }, metadata);
    this.store.set('message', messageId(row), row, { expires:row.expiresAt, created:row.createdAt, tag:row.chat, unread:!row.fromMe&&!row.readAt });
  }
  tombstone(key, now = Date.now(), mutationId) {
    const id = messageId(key);
    this.store.transaction(() => {
      this.store.remove('message', id); this.store.remove('read-receipt', id); this.store.remove('retry', id); this.store.remove('pending-edit', id); this.store.remove('encrypted-edit', id);
      this.store.set('tombstone', id, { chat:key.chat, id:key.id, fromMe:key.fromMe, deleted:true }, { expires:now + WEEK, created:now, tag:key.chat });
      for (const {id:opId,value} of this.store.list('operation', {tag:key.id})) {
        if(!key.fromMe||value.chat!==key.chat)continue;
        const next={...value,request:undefined,deleted:true,state:value.state==='prepared'?'cancelled':value.state}; this.store.set('operation',opId,next,{expires:value.expiresAt,created:value.createdAt,tag:key.id});
      }
      this.resolveMutations(key, null, mutationId);
      this.revision(key.chat, 'delete', { id:key.id });
    });
  }
  edit(key, body, editedAt, mutationId) {
    const id = messageId(key), now = Date.now(), old = this.store.get('message', id), content = Object.hasOwn(body||{},'normalizedContent')?body.normalizedContent:extract(body);
    if (this.store.get('tombstone', id) || !Number.isSafeInteger(editedAt) || editedAt <= now - WEEK || editedAt > now + 60000) return;
    if (!content) { this.tombstone(key); return; }
    if (!old) {
      const pending = this.store.get('pending-edit', id);
      if (pending && pending.at >= editedAt) return;
      if (!pending && this.store.totals('pending-edit').count >= MAX_MESSAGES) throw Error('Message lifecycle storage is full.');
      this.trim(Buffer.byteLength(JSON.stringify(content)) + 512);
      const earliestCreatedAt = editedAt - EDIT_WINDOW;
      const expires = Math.min(now+120000, earliestCreatedAt+WEEK, content.expiration?earliestCreatedAt+content.expiration*1000:Infinity, content.protectedCreatedAt?content.protectedCreatedAt+WEEK:Infinity);
      if(expires<=now){this.tombstone(key);return;}
      this.store.set('pending-edit', id, { chat:key.chat,id:key.id,fromMe:key.fromMe,content,at:editedAt }, { expires, created:editedAt, tag:key.chat });
      return;
    }
    if (editedAt <= (old.editedAt || 0)) return;
    const expiresAt = Math.min(old.expiresAt, content.expiration ? old.createdAt + content.expiration * 1000 : Infinity, content.protectedCreatedAt ? content.protectedCreatedAt + WEEK : Infinity);
    if (expiresAt <= now) { this.tombstone(key); return; }
    this.store.transaction(()=>{
      this.store.remove('retry',id);this.store.remove('pending-edit',id);this.store.remove('encrypted-edit',id);
      // The original body/grants must not survive an edit in the recoverable outbox.
      for(const {id:opId,value}of this.store.list('operation',{tag:key.id}))if(key.fromMe&&value.chat===key.chat)this.store.set('operation',opId,{...value,request:undefined,state:value.state==='prepared'?'cancelled':value.state},{expires:Math.min(value.expiresAt,expiresAt),created:value.createdAt,tag:key.id});
      this.resolveMutations(key, content, mutationId);
      this.save({ ...old, text:content.text, wire:content.wire, label:content.label, kind:content.kind, expiresAt, editedAt, editUnavailableAt:undefined });
    });
    this.trim();
  }
  unavailableEdit(key, at) {
    const row=this.store.get('message',messageId(key));if(!row||row.editUnavailableAt)return;
    const prior=row.editedAt;
    this.edit(key,{normalizedContent:{kind:'unsupported',text:'Edited message unavailable · open in WhatsApp',expiration:0}},Math.max(at,(prior||0)+1));
    const changed=this.store.get('message',messageId(key));if(changed)this.save({...changed,editedAt:prior,editUnavailableAt:at});
  }
  resolveMutations(key, content, except) {
    for (const {value} of this.store.list('mutation',{tag:messageId(key)})) {
      if (value.operationId===except || !['prepared','sending','uncertain'].includes(value.state)) continue;
      // A later phone edit/delete clears saved retry bodies. It cannot trigger another send.
      const confirmed = content ? value.action==='edit' && value.contentDigest===digest(content.wire??content.text??'') : value.action==='delete-everyone';
      this.store.set('mutation',value.operationId,{...value,request:undefined,state:confirmed?'sent':'cancelled'},
        {expires:value.expiresAt,created:value.createdAt,tag:messageId(key)});
    }
  }
  delivery(key, state) {
    const id=messageId(key),old=this.store.get('message',id);
    if (old) {
      const delivery=strongest(old.delivery,state), readAt=old.readAt||(!old.fromMe&&state==='read'?Date.now():null);
      if(delivery!==old.delivery||readAt!==old.readAt)this.save({...old,delivery,readAt},!old.fromMe);
      if(!old.fromMe&&state==='read')this.store.remove('read-receipt',id);
    } else if(!key.fromMe&&state==='read'&&!this.store.get('tombstone',id)&&!this.store.get('read-receipt',id)) {
      // A phone receipt can precede the message or an alias mapping. Keep only
      // bounded, encrypted identity metadata until the message arrives.
      if(this.store.totals('read-receipt').count>=MAX_MESSAGES)throw Error('Read synchronization storage is full.');
      const readAt=Date.now();this.store.set('read-receipt',id,{chat:key.chat,id:key.id,fromMe:false,readAt},{created:readAt,expires:readAt+WEEK,tag:key.chat});
    }
    for(const {id:opId,value} of this.store.list('operation',{tag:key.id})) {
      if(!key.fromMe||value.chat!==key.chat)continue;
      if (!['prepared','failed','cancelled'].includes(value.state)) {
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
    const page=rows.slice(0,limit);return {messages:page.map(({readAt,revision,editSecret,editUnavailableAt,...r})=>({...r,...(editUnavailableAt?{editedAt:r.editedAt||editUnavailableAt}:{}),...(r.editedAt!==undefined?messageActions(r):{})})),cursor:rows.length>limit?encodeCursor(page.at(-1)):null};
  }
  chats(args = {}) {
    const before=decodeCursor(args.before),limit=pageLimit(args.limit,10),rows=[],stats=this.store.messageStats();
    for(const {value:chat} of this.store.list('chat',{limit:MAX_MESSAGES})) {
      const stat=stats.get(this.store.hash(chat.id)),updatedAt=Math.max(chat.updatedAt,stat?.createdAt||0);
      rows.push({...chat,updatedAt,unread:stat?.unread||0});
    }
    rows.sort((a,b)=>b.updatedAt-a.updatedAt||b.id.localeCompare(a.id));
    const filtered=before?rows.filter(r=>r.updatedAt<before[0]||r.updatedAt===before[0]&&r.id<before[1]):rows,page=filtered.slice(0,limit);
    return {chats:page.map(chat=>{const last=this.store.list('message',{tag:chat.id,limit:1})[0]?.value,avatar=this.store.get('avatar',chat.id)?.avatar||null;return {...chat,avatar,...(last?{lastMessage:last.kind==='protected'?'CIFER protected message':last.text||last.label||'Message'}:{})};}),cursor:filtered.length>limit?encodeCursor(page.at(-1)):null};
  }
  unread() {
    const stats=this.store.messageStats(),chats=[];let total=0;
    for(const {value:chat} of this.store.list('chat',{limit:MAX_MESSAGES})){const stat=stats.get(this.store.hash(chat.id));if(stat?.unread){total+=stat.unread;if(chats.length<100)chats.push({chat:chat.id,count:stat.unread,createdAt:stat.createdAt});}}
    return {unreadTotal:total,unreadChats:chats};
  }
  read(chat, ids) {
    if(ids!==undefined&&(!Array.isArray(ids)||ids.length>100||ids.some(x=>typeof x!=='string')))throw Error('Invalid message selection.');
    this.store.transaction(()=>{for(const {value:row} of this.store.list('message',{tag:chat,limit:MAX_MESSAGES}))if(!row.fromMe&&!row.readAt&&(!ids||ids.includes(row.id)))this.save({...row,readAt:Date.now()},true);});
    return {ok:true};
  }
  rebind(old, chat) {
    if(old===chat)return;
    this.store.transaction(()=>{
      // Apply deletions first, including deletion of an already-existing canonical copy.
      for(const {id,value}of this.store.list('tombstone',{tag:old,limit:30000})){this.store.remove('tombstone',id);this.tombstone({...value,chat});}
      for(const {id,value}of this.store.list('encrypted-edit',{tag:old,limit:MAX_MESSAGES})){this.store.remove('encrypted-edit',id);const next={...value,chat},targetId=messageId(next);if(!this.store.get('tombstone',targetId)){const present=this.store.get('encrypted-edit',targetId),moved={...(present&&present.at>=value.at?present:next),expiresAt:Math.min(present?.expiresAt||Infinity,value.expiresAt)};this.store.set('encrypted-edit',targetId,moved,{expires:moved.expiresAt,created:moved.at,tag:chat});}}
      for(const {id,value}of this.store.list('pending-edit',{tag:old,limit:MAX_MESSAGES})){this.store.remove('pending-edit',id);this.edit({...value,chat},{normalizedContent:value.content},value.at);}
      for(const {id,value}of this.store.list('message',{tag:old,limit:MAX_MESSAGES})){
        const target={...value,chat},targetId=messageId(target);this.store.remove('message',id);this.store.remove('retry',id);
        if(this.store.get('tombstone',targetId))continue;
        const existing=this.store.get('message',targetId);
        const moved=existing?{...(Number(value.editedAt)>Number(existing.editedAt)?target:existing),chat,
          createdAt:Math.min(value.createdAt,existing.createdAt),expiresAt:Math.min(value.expiresAt,existing.expiresAt),
          editSecret:existing.editSecret||value.editSecret,readAt:existing.readAt||value.readAt||null,delivery:strongest(existing.delivery,value.delivery)}:target;
        if(moved.expiresAt<=Date.now()){this.tombstone(moved);continue;}this.save(moved);if(this.store.get('read-receipt',targetId))this.delivery(moved,'read');const pending=this.store.get('pending-edit',targetId);if(pending){this.store.remove('pending-edit',targetId);this.edit(moved,{normalizedContent:pending.content},pending.at);}
      }
      for(const {id,value}of this.store.list('read-receipt',{tag:old,limit:MAX_MESSAGES})){
        this.store.remove('read-receipt',id);const next={...value,chat},targetId=messageId(next);
        if(this.store.get('tombstone',targetId))continue;
        const readAt=Math.min(value.readAt,this.store.get('read-receipt',targetId)?.readAt||Infinity);
        this.store.set('read-receipt',targetId,{...next,readAt},{created:readAt,expires:readAt+WEEK,tag:chat});this.delivery(next,'read');
      }
      for(const {id,value}of this.store.list('operation',{limit:10000}))if(value.chat===old){
        // Alias discovery cannot silently retarget a prepared protected contact/grant.
        this.store.set('operation',id,{...value,chat,request:undefined,state:value.state==='prepared'?'cancelled':value.state},{expires:value.expiresAt,created:value.createdAt,tag:value.messageId});
      }
      for(const {id,value}of this.store.list('mutation',{limit:10000}))if(value.chat===old)this.store.set('mutation',id,{...value,chat,request:undefined,state:value.state==='prepared'?'cancelled':value.state==='sending'?'uncertain':value.state},{expires:value.expiresAt,created:value.createdAt,tag:messageId({chat,id:value.targetId,fromMe:value.fromMe})});
      this.store.remove('contact-binding',old);
      const avatar=this.store.get('avatar',old);if(avatar&&!this.store.get('avatar',chat))this.store.set('avatar',chat,avatar,{created:avatar.checkedAt,expires:avatar.expiresAt});this.store.remove('avatar',old);
      const setting=this.store.get('setting',old);if(setting&&!this.store.get('setting',chat))this.store.set('setting',chat,setting);this.store.remove('setting',old);
      const seed=this.store.get('history-seed',old);if(seed&&!this.store.get('history-seed',chat))this.store.set('history-seed',chat,{...seed,chat},{created:seed.createdAt,expires:seed.createdAt+WEEK});this.store.remove('history-seed',old);
      const previous=this.store.get('chat',old),canonical=this.store.get('chat',chat);if(previous){this.contact(chat,{...previous,...(canonical?.name&&canonical.name!=='WhatsApp contact'&&!canonical.name.startsWith('+')?{name:canonical.name}:{})});this.store.remove('chat',old);}
      this.revision(chat,'reset');
    });
  }
  trim(reserveBytes = 0) {
    const operations=this.store.totals('operation').bytes+this.store.totals('retry').bytes+this.store.totals('pending-edit').bytes+this.store.totals('encrypted-edit').bytes+this.store.totals('mutation').bytes;
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
    for(const {value:row} of this.store.list('encrypted-edit',{expired:true,limit:MAX_MESSAGES}))this.tombstone(row);
    for(const {value:row} of this.store.list('pending-edit',{expired:true,limit:MAX_MESSAGES}))this.tombstone(row);
    for(const {value:row} of this.store.list('message',{expired:true,limit:MAX_MESSAGES}))this.tombstone(row);
    this.store.prune();this.trim();this.store.checkpoint();
  }
  reset() { this.store.transaction(()=>{for(const ns of ['message','read-receipt','tombstone','pending-edit','encrypted-edit','mutation','mutation-identity','retry','change','operation','operation-identity','chat','alias','setting','history','history-seed','contact-binding','avatar'])this.store.clear(ns);this.revision(null,'reset');});if(!this.store.depth)this.store.checkpoint(); }
}
