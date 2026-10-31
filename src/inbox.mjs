import { WEEK, MAX_BYTES, MAX_MESSAGES, EDIT_WINDOW, messageId, messageActions, digest, extract, receipt, strongest, isGroup, peerPattern } from './content.mjs';

// Opaque IDs must use the same ordinal order as the cursor predicates below.
const descendingId = (a,b) => a===b?0:a<b?1:-1;
const compare = (a, b) => b.createdAt - a.createdAt || descendingId(a.id,b.id) || (isGroup(a.chat)?descendingId(messageId(a),messageId(b)):0);
const chatView = row => { const {sendRestriction,...value}=row; return {...value,kind:isGroup(row.id)?'group':'direct',archived:row.archived===true,canSend:isGroup(row.id)?row.canSend===true:row.canSend!==false,...(typeof sendRestriction==='string'&&sendRestriction?{sendRestriction}:{})}; };
const identity = key => ({chat:key.chat,id:key.id,fromMe:key.fromMe,...(key.participant?{participant:key.participant}:{})});
export const encodeCursor = row => Buffer.from(JSON.stringify([row.createdAt ?? row.updatedAt, row.id,...(isGroup(row.chat)?[row.fromMe,row.participant||'']:[])])).toString('base64url');
export function decodeCursor(value) {
  if (value == null) return null;
  if (typeof value !== 'string' || value.length > 512) throw Error('Invalid history cursor.');
  let result; try { result = JSON.parse(Buffer.from(value, 'base64url').toString('utf8')); } catch { throw Error('Invalid history cursor.'); }
  if (!Array.isArray(result) || ![2,4].includes(result.length) || !Number.isSafeInteger(result[0]) || typeof result[1] !== 'string' || result[1].length > 128 || result.length===4&&(typeof result[2]!=='boolean'||!peerPattern.test(result[3]))) throw Error('Invalid history cursor.');
  return result;
}
const pageLimit = (value, fallback) => { const limit = value ?? fallback; if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100) throw Error('Invalid page size.'); return limit; };

export class Inbox {
  constructor(store, changed, limits = {}, removed = () => {}) { this.store = store; this.changed = changed; this.removed = removed; this.maxMessages=Math.min(MAX_MESSAGES,limits.maxMessages??MAX_MESSAGES);this.maxBytes=Math.min(MAX_BYTES,limits.maxBytes??MAX_BYTES);
    // Older candidates sometimes passed a complete message to tombstone(). Strip
    // any retained content while preserving deletion identity on upgrade.
    if(!store.get('system','content-free-tombstones-v1')) store.transaction(()=>{
      for(const {id,value} of store.list('tombstone')) store.set('tombstone',id,{...identity(value),deleted:true},{expires:Date.now()+WEEK,created:Date.now(),tag:value.chat});
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
    const old = this.store.get('chat', chat) || { id: chat, name: chat.endsWith('@s.whatsapp.net') ? '+' + chat.split('@')[0] : isGroup(chat)?'WhatsApp group':'WhatsApp contact', kind:isGroup(chat)?'group':'direct', archived:false, updatedAt: 0 };
    const name = typeof value.name === 'string' && value.name.trim() ? value.name.slice(0, 120) : old.name;
    const row = { ...old, ...Object.fromEntries(Object.entries(value).filter(([,v])=>v!==undefined)), id: chat, kind:isGroup(chat)?'group':'direct', name, updatedAt: Math.max(old.updatedAt || 0, value.updatedAt || 0) };
    if (chat.endsWith('@s.whatsapp.net')) row.phone = '+' + chat.split('@')[0];
    this.store.set('chat', chat, row, { created: row.updatedAt }); return row;
  }
  ingest(clean, { history = false, operationId } = {}) {
    const id = messageId(clean), now = Date.now();
    if (clean.expiresAt <= now || clean.createdAt <= now - WEEK || this.store.get('tombstone', id)) return false;
    const expiredEdit=this.store.get('encrypted-edit',id,0);
    if(expiredEdit?.expiresAt<=now){this.tombstone(clean);return false;}
    const old = this.store.get('message', id), state = isGroup(clean.chat)&&clean.fromMe?'sent':receipt(clean.status) || 'sent';
    const pendingRead=this.store.get('read-receipt',id), knownRead=state==='read'||!!pendingRead;
    if (old) {
      if(!old.image&&clean.image&&!old.editedAt&&old.label==='Photo · open in WhatsApp'){old.image=clean.image;this.save(old);this.trim();}
      if(!old.editSecret&&clean.editSecret){old.editSecret=clean.editSecret;this.save(old);this.trim();}
      if(clean.editedAt)this.edit(old,{normalizedContent:clean},clean.editedAt);
      const current=this.store.get('message',id);if(!current)return false;
      const expiresAt = Math.min(current.expiresAt, clean.expiresAt), delivery = strongest(current.delivery, knownRead ? 'read' : state);
      const readAt=current.readAt||(!current.fromMe&&knownRead?now:null);
      if (current.expiresAt !== expiresAt || current.delivery !== delivery || current.readAt !== readAt) this.save({ ...current, expiresAt, delivery, readAt });
      if(pendingRead)this.store.remove('read-receipt',id);
      return false;
    }
    const row = { ...identity(clean), ...(clean.participantAlt?{participantAlt:clean.participantAlt}:{}), ...(clean.protocolKey?{protocolKey:clean.protocolKey}:{}), ...(isGroup(clean.chat)&&clean.name?{senderName:clean.name}:{}), createdAt: clean.createdAt, expiresAt: clean.expiresAt,
      kind: clean.kind, ...(clean.text !== undefined ? { text:clean.text } : {}), ...(clean.wire ? { wire:clean.wire } : {}), ...(clean.label ? { label:clean.label } : {}),
      image:clean.image, editSecret:clean.editSecret, delivery: knownRead ? 'read' : state, readAt: clean.fromMe || history || knownRead ? now : null, editedAt: clean.editedAt || null, ...(operationId ? { operationId } : {}) };
    this.store.transaction(() => { this.save(row); if(pendingRead)this.store.remove('read-receipt',id); this.contact(row.chat, { updatedAt: row.createdAt, ...(!isGroup(row.chat)&&!row.fromMe && clean.name ? { name:clean.name } : {}) });
      const pending = this.store.get('pending-edit', id);
      if (pending) { this.edit(row, { normalizedContent: pending.content }, pending.at); this.store.remove('pending-edit', id); }
    });
    this.trim(); return true;
  }
  save(row, metadata = false) {
    row.revision = this.revision(row.chat, metadata?'metadata':'upsert', { id:row.id,...(row.participant?{participant:row.participant}:{}) }, metadata);
    this.store.set('message', messageId(row), row, { expires:row.expiresAt, created:row.createdAt, tag:row.chat, unread:!row.fromMe&&!row.readAt });
  }
  tombstone(key, now = Date.now(), mutationId) {
    const id = messageId(key);
    this.store.transaction(() => {
      this.store.remove('photo', id); this.store.remove('message', id); this.store.remove('read-receipt', id); this.store.remove('retry', id); this.store.remove('pending-edit', id); this.store.remove('encrypted-edit', id);
      for(const {id:receiptId} of this.store.list('group-receipt',{tag:id,limit:10000}))this.store.remove('group-receipt',receiptId);
      this.store.set('tombstone', id, { ...identity(key), deleted:true }, { expires:now + WEEK, created:now, tag:key.chat });
      for (const {id:opId,value} of this.store.list('operation', {tag:key.id})) {
        if(!key.fromMe||value.chat!==key.chat)continue;
        const next={...value,request:undefined,deleted:true,state:value.state==='prepared'?'cancelled':value.state}; this.store.set('operation',opId,next,{expires:value.expiresAt,created:value.createdAt,tag:key.id});
      }
      this.resolveMutations(key, null, mutationId);this.removed(key);
      this.revision(key.chat, 'delete', { id:key.id,...(key.participant?{participant:key.participant}:{}) });
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
      this.store.set('pending-edit', id, { ...identity(key),content,at:editedAt,expiresAt:expires }, { expires, created:editedAt, tag:key.chat });
      return;
    }
    if (editedAt <= (old.editedAt || 0)) return;
    const expiresAt = Math.min(old.expiresAt, content.expiration ? old.createdAt + content.expiration * 1000 : Infinity, content.protectedCreatedAt ? content.protectedCreatedAt + WEEK : Infinity);
    if (expiresAt <= now) { this.tombstone(key); return; }
    this.store.transaction(()=>{
      this.store.remove('photo',id);this.store.remove('retry',id);this.store.remove('pending-edit',id);this.store.remove('encrypted-edit',id);
      // The original body/grants must not survive an edit in the recoverable outbox.
      for(const {id:opId,value}of this.store.list('operation',{tag:key.id}))if(key.fromMe&&value.chat===key.chat)this.store.set('operation',opId,{...value,request:undefined,state:value.state==='prepared'?'cancelled':value.state},{expires:Math.min(value.expiresAt,expiresAt),created:value.createdAt,tag:key.id});
      this.resolveMutations(key, content, mutationId);
      for(const {id:reactionId,value}of this.store.list('reaction',{tag:id,limit:2048}))if(value.expiresAt>expiresAt)this.store.set('reaction',reactionId,{...value,expiresAt},{expires:expiresAt,created:value.at,tag:id});
      this.save({ ...old, text:content.text, wire:content.wire, label:content.label, image:content.image||(content.label==='Photo · open in WhatsApp'?old.image:undefined), kind:content.kind, expiresAt, editedAt, editUnavailableAt:undefined });
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
    if(isGroup(key.chat)&&key.fromMe)state='sent';
    const id=messageId(key),old=this.store.get('message',id);
    if (old) {
      const delivery=strongest(old.delivery,state), readAt=old.readAt||(!old.fromMe&&state==='read'?Date.now():null);
      if(delivery!==old.delivery||readAt!==old.readAt)this.save({...old,delivery,readAt},!old.fromMe);
      if(!old.fromMe&&state==='read')this.store.remove('read-receipt',id);
    } else if(!key.fromMe&&state==='read'&&!this.store.get('tombstone',id)&&!this.store.get('read-receipt',id)) {
      // A phone receipt can precede the message or an alias mapping. Keep only
      // bounded, encrypted identity metadata until the message arrives.
      if(this.store.totals('read-receipt').count>=MAX_MESSAGES)throw Error('Read synchronization storage is full.');
      const readAt=Date.now();this.store.set('read-receipt',id,{...identity(key),readAt},{created:readAt,expires:readAt+WEEK,tag:key.chat});
    }
    for(const {id:opId,value} of this.store.list('operation',{tag:key.id})) {
      if(!key.fromMe||value.chat!==key.chat)continue;
      if (!['prepared','failed','cancelled'].includes(value.state)) {
        const delivery=strongest(value.state,state);
        this.store.set('operation',opId,{...value,state:delivery,...(['sent','delivered','read'].includes(delivery)?{request:undefined}:{})},{expires:value.expiresAt,created:value.createdAt,tag:key.id});
      }
    }
  }
  groupReceipt(key, recipient, state) {
    if(!isGroup(key.chat)||!key.fromMe||!peerPattern.test(key.participant||'')||!peerPattern.test(recipient)||!['delivered','read'].includes(state))return;
    const target=messageId(key);if(this.store.get('tombstone',target))return;
    const id=JSON.stringify([target,recipient]),old=this.store.get('group-receipt',id),message=this.store.get('message',target);
    if(!old&&this.store.totals('group-receipt').count>=MAX_MESSAGES)return;
    const expiresAt=Math.min(message?.expiresAt||Date.now()+WEEK,old?.expiresAt||Infinity),delivery=strongest(old?.delivery,state);
    if(old?.delivery===delivery)return;
    this.store.set('group-receipt',id,{...identity(key),recipient,delivery,expiresAt},{expires:expiresAt,created:Date.now(),tag:target});
    this.revision(key.chat,'metadata',{id:key.id,participant:key.participant},true);
  }
  receiptCounts(key) {
    if(!isGroup(key.chat)||!key.fromMe)return {};
    const rows=this.store.list('group-receipt',{tag:messageId(key),limit:MAX_MESSAGES});
    return {deliveredCount:rows.length,readCount:rows.filter(r=>r.value.delivery==='read').length};
  }
  messages(chat, args = {}) {
    const before=decodeCursor(args.before),limit=pageLimit(args.limit,20),now=Date.now();
    let rows=this.store.list('message',{tag:chat,now,limit:MAX_MESSAGES}).map(r=>r.value).filter(r=>r.createdAt>now-WEEK&&r.expiresAt>now).sort(compare);
    // Prepared protected sends survive a crash before publication/network transmission.
    for (const {value:op} of this.store.list('operation',{limit:MAX_MESSAGES})) {
      if (op.chat!==chat||!op.request||op.deleted||rows.some(m=>m.id===op.messageId)||!['prepared','sending','uncertain'].includes(op.state)) continue;
      rows.push({id:op.messageId,chat,fromMe:true,...(op.participant?{participant:op.participant}:{}),kind:op.request.kind,...(op.request.kind==='protected'?{wire:op.request.text}:{text:op.request.text}),createdAt:op.createdAt,expiresAt:op.expiresAt,delivery:op.state==='uncertain'?'uncertain':'queued',operationId:op.operationId});
    }
    rows.sort(compare);if(before)rows=rows.filter(r=>r.createdAt<before[0]||r.createdAt===before[0]&&(r.id<before[1]||before.length===4&&r.id===before[1]&&messageId(r)<messageId({chat,id:before[1],fromMe:before[2],participant:before[3]})));
    const page=rows.slice(0,limit);return {messages:page.map(({readAt,revision,editSecret,editUnavailableAt,protocolKey,participantAlt,image,...r})=>({...r,...(r.kind!=='protected'&&r.label==='Photo · open in WhatsApp'?{photo:{available:!!image}}:{}),...this.receiptCounts(r),...(editUnavailableAt?{editedAt:r.editedAt||editUnavailableAt}:{}),...(r.editedAt!==undefined?messageActions({...r,image}):{})})),cursor:rows.length>limit?encodeCursor(page.at(-1)):null};
  }
  chats(args = {}) {
    const view=args.view??'active';if(!['active','archived'].includes(view))throw Error('Invalid chat view.');
    const before=decodeCursor(args.before),limit=pageLimit(args.limit,10),rows=[],stats=this.store.messageStats();
    for(const {value:chat} of this.store.list('chat',{limit:MAX_MESSAGES})) {
      if((chat.archived===true)!==(view==='archived'))continue;
      const stat=stats.get(this.store.hash(chat.id)),updatedAt=Math.max(chat.updatedAt,stat?.createdAt||0);
      rows.push({...chat,updatedAt,unread:stat?.unread||0});
    }
    rows.sort((a,b)=>b.updatedAt-a.updatedAt||descendingId(a.id,b.id));
    const filtered=before?rows.filter(r=>r.updatedAt<before[0]||r.updatedAt===before[0]&&r.id<before[1]):rows,page=filtered.slice(0,limit);
    return {chats:page.map(chat=>{const last=this.store.list('message',{tag:chat.id,limit:1})[0]?.value,avatar=this.store.get('avatar',chat.id)?.avatar||null;return {...chatView(chat),avatar,...(last?{lastMessage:last.kind==='protected'?'CIFER protected message':last.text||last.label||'Message'}:{})};}),cursor:filtered.length>limit?encodeCursor(page.at(-1)):null};
  }
  chat(chat) {
    const row=this.store.get('chat',chat);if(!row)return null;
    const stat=this.store.messageStats().get(this.store.hash(chat)),last=this.store.list('message',{tag:chat,limit:1})[0]?.value;
    return {...chatView(row),unread:stat?.unread||0,updatedAt:Math.max(row.updatedAt,stat?.createdAt||0),avatar:this.store.get('avatar',chat)?.avatar||null,...(last?{lastMessage:last.kind==='protected'?'CIFER protected message':last.text||last.label||'Message'}:{})};
  }
  unread() {
    const stats=this.store.messageStats(),chats=[];let total=0,archivedCount=0,archivedUnreadTotal=0;
    for(const {value:chat} of this.store.list('chat',{limit:MAX_MESSAGES})){
      const stat=stats.get(this.store.hash(chat.id));
      if(chat.archived===true){archivedCount++;archivedUnreadTotal+=stat?.unread||0;continue;}
      if(stat?.unread){total+=stat.unread;if(chats.length<100)chats.push({chat:chat.id,count:stat.unread,createdAt:stat.createdAt});}
    }
    return {unreadTotal:total,unreadChats:chats,archivedCount,archivedUnreadTotal};
  }
  read(chat, ids) {
    const valid=x=>typeof x==='string'?!isGroup(chat):x&&typeof x==='object'&&typeof x.id==='string'&&typeof x.fromMe==='boolean'&&(!isGroup(chat)||peerPattern.test(x.participant||''));
    if(ids!==undefined&&(!Array.isArray(ids)||ids.length>100||ids.some(x=>!valid(x))))throw Error('Invalid message selection.');
    this.store.transaction(()=>{for(const {value:row} of this.store.list('message',{tag:chat,limit:MAX_MESSAGES}))if(!row.fromMe&&!row.readAt&&(!ids||ids.some(x=>typeof x==='string'?x===row.id:messageId({...x,chat})===messageId(row))))this.save({...row,readAt:Date.now()},true);});
    return {ok:true};
  }
  rebindParticipant(old, participant) {
    if(old===participant||!peerPattern.test(old)||!peerPattern.test(participant))return;
    this.store.transaction(()=>{
      for(const ns of ['tombstone','message','pending-edit','encrypted-edit','read-receipt'])for(const {id,value} of this.store.list(ns,{limit:30000})){
        if(!isGroup(value.chat)||value.participant!==old)continue;
        const next={...value,participant},target=messageId(next),present=this.store.get(ns,target);
        this.store.remove(ns,id);if(ns==='message')this.store.remove('photo',id);
        if(ns==='tombstone'){this.tombstone(next);continue;}
        if(this.store.get('tombstone',target))continue;
        if(ns==='message'){
          const merged=present?{...(Number(value.editedAt)>Number(present.editedAt)?next:present),createdAt:Math.min(value.createdAt,present.createdAt),expiresAt:Math.min(value.expiresAt,present.expiresAt),readAt:present.readAt||value.readAt||null,delivery:strongest(present.delivery,value.delivery),editSecret:present.editSecret||value.editSecret}:next;
          this.save(merged);if(this.store.get('read-receipt',target))this.delivery(merged,'read');
        }else{
          const picked=present&&(present.at||present.readAt)>=(value.at||value.readAt)?present:next;
          const expiresAt=Math.min(present?.expiresAt||Infinity,value.expiresAt||((value.readAt||Date.now())+WEEK));
          this.store.set(ns,target,{...picked,expiresAt},{expires:expiresAt,created:picked.at||picked.readAt||Date.now(),tag:value.chat});
          if(ns==='read-receipt')this.delivery(next,'read');
          if(ns==='pending-edit'&&this.store.get('message',target))this.edit(next,{normalizedContent:picked.content},picked.at);
        }
      }
      for(const {id,value} of this.store.list('group-receipt',{limit:MAX_MESSAGES})){
        if(value.participant!==old&&value.recipient!==old)continue;
        this.store.remove('group-receipt',id);
        this.groupReceipt({...value,participant:value.participant===old?participant:value.participant},value.recipient===old?participant:value.recipient,value.delivery);
      }
      for(const {id,value} of this.store.list('mutation',{limit:MAX_MESSAGES}))if(isGroup(value.chat)&&value.participant===old){
        const next={...value,participant,request:undefined,state:value.state==='prepared'?'cancelled':value.state==='sending'?'uncertain':value.state};
        this.store.set('mutation',id,next,{expires:value.expiresAt,created:value.createdAt,tag:messageId({chat:value.chat,id:value.targetId,fromMe:value.fromMe,participant})});
      }
      for(const {id,value}of this.store.list('group-pending-mutation',{limit:MAX_MESSAGES}))if(value.participant===old||value.actor===old){
        const next={...value,participant:value.participant===old?participant:value.participant,actor:value.actor===old?participant:value.actor};this.store.remove('group-pending-mutation',id);
        this.store.set('group-pending-mutation',messageId(next),next,{created:value.at,expires:value.expiresAt||Math.min(Date.now()+120000,value.at+WEEK),tag:value.chat});
      }
      for(const {id,value}of this.store.list('group-role',{limit:MAX_MESSAGES}))if(value.intervals.some(r=>r.admins.includes(old))){
        const intervals=value.intervals.map(r=>({...r,admins:[...new Set(r.admins.map(id=>id===old?participant:id))].sort()}));
        this.store.set('group-role',id,{...value,intervals},{created:value.checkedAt,expires:value.checkedAt+WEEK});
      }
      for(const {id,value}of this.store.list('group-roster',{limit:20}))if(value.participants.some(p=>p.id===old)){
        const people=new Map();for(const p of value.participants){const id=p.id===old?participant:p.id,prior=people.get(id);people.set(id,{...p,id,admin:!!(p.admin||prior?.admin)});}
        this.store.set('group-roster',id,{...value,participants:[...people.values()]},{created:value.checkedAt,expires:value.checkedAt+86400000});
      }
    });
  }
  rebind(old, chat) {
    if(old===chat)return;
    this.store.transaction(()=>{
      // Apply deletions first, including deletion of an already-existing canonical copy.
      for(const {id,value}of this.store.list('tombstone',{tag:old,limit:30000})){this.store.remove('tombstone',id);this.tombstone({...value,chat});}
      for(const {id,value}of this.store.list('encrypted-edit',{tag:old,limit:MAX_MESSAGES})){this.store.remove('encrypted-edit',id);const next={...value,chat},targetId=messageId(next);if(!this.store.get('tombstone',targetId)){const present=this.store.get('encrypted-edit',targetId),moved={...(present&&present.at>=value.at?present:next),expiresAt:Math.min(present?.expiresAt||Infinity,value.expiresAt)};this.store.set('encrypted-edit',targetId,moved,{expires:moved.expiresAt,created:moved.at,tag:chat});}}
      for(const {id,value}of this.store.list('pending-edit',{tag:old,limit:MAX_MESSAGES})){this.store.remove('pending-edit',id);this.edit({...value,chat},{normalizedContent:value.content},value.at);}
      for(const {id,value}of this.store.list('message',{tag:old,limit:MAX_MESSAGES})){
        const target={...value,chat},targetId=messageId(target);this.store.remove('photo',id);this.store.remove('message',id);this.store.remove('retry',id);
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
    const operations=this.store.totals('operation').bytes+this.store.totals('retry').bytes+this.store.totals('pending-edit').bytes+this.store.totals('encrypted-edit').bytes+this.store.totals('mutation').bytes+this.store.totals('reaction').bytes+this.store.totals('group-receipt').bytes+this.store.totals('group-roster').bytes+this.store.totals('group-role').bytes+this.store.totals('group-pending-mutation').bytes;
    if(operations+reserveBytes>this.maxBytes)throw Error('Local message and outbox storage is full.');
    let {count,bytes}=this.store.totals('message');bytes+=operations+reserveBytes+this.store.totals('photo').bytes;
    // Downloaded photos are replaceable cache; evict them before message bodies.
    if(bytes>this.maxBytes)for(const {id,value}of this.store.list('photo',{oldest:true,limit:MAX_MESSAGES})){if(bytes<=this.maxBytes)break;bytes-=Buffer.byteLength(JSON.stringify({id,value}));this.store.remove('photo',id);}
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
  reset() { this.store.transaction(()=>{for(const ns of ['reaction','photo','message','read-receipt','tombstone','pending-edit','encrypted-edit','mutation','mutation-identity','retry','change','operation','operation-identity','chat','alias','setting','history','history-seed','contact-binding','avatar','group-roster','group-role','group-receipt','group-pending-mutation'])this.store.clear(ns);this.revision(null,'reset');});if(!this.store.depth)this.store.checkpoint(); }
}
