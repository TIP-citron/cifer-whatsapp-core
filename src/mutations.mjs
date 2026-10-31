import { assertPeer, canonicalJSON, digest, exact, extract, WEEK, messageActions, messageId, protectedWire, uuidPattern } from './content.mjs';

const keyOfInput = input => ({chat:input.chat,id:input.messageId,fromMe:input.fromMe});
const copy = value => JSON.parse(JSON.stringify(value));
const pending = new Set(['prepared','sending','uncertain']);

/** Durable, owner-scoped mutations. Ambiguous network operations are never replayed. */
export class Mutations {
  constructor(engine) {
    this.engine=engine; this.store=engine.store; this.inbox=engine.inbox;
    for(const {id,value} of this.store.list('mutation')) if(value.state==='sending') this.save({...value,state:'uncertain',request:undefined});
  }
  input(value) {
    exact(value,['operationId','chat','messageId','fromMe','action','expectedVersion'],['text','kind','prepared']);
    if(!uuidPattern.test(value.operationId)||!/^[\w-]{1,128}$/.test(value.messageId)||typeof value.fromMe!=='boolean'||!['edit','delete-everyone','delete-local'].includes(value.action)||!/^[a-f0-9]{64}$/.test(value.expectedVersion)) throw Error('Invalid message action.');
    assertPeer(value.chat);
    if(value.action==='edit') {
      if(!value.fromMe) throw Error('Only your own messages can be edited.');
      this.engine.sendInput({operationId:value.operationId,chat:value.chat,text:value.text,kind:value.kind,...(value.prepared!==undefined?{prepared:value.prepared}:{})});
    } else if(value.text!==undefined||value.kind!==undefined||value.prepared!==undefined) throw Error('Invalid deletion.');
    return copy(value);
  }
  view(row) {
    return {operationId:row.operationId,chat:row.chat,messageId:row.targetId,fromMe:row.fromMe,action:row.action,state:row.state==='sending'?'queued':row.state,
      ...(row.state==='prepared'&&row.request?{request:copy(row.request)}:{})};
  }
  save(row) { this.store.set('mutation',row.operationId,row,{expires:row.expiresAt,created:row.createdAt,tag:messageId({chat:row.chat,id:row.targetId,fromMe:row.fromMe})}); }
  list(chat) { return this.store.list('mutation',{limit:10000}).map(r=>r.value).filter(r=>r.chat===chat&&pending.has(r.state)).slice(0,100).map(r=>this.view(r)); }
  target(input) {
    const row=this.store.get('message',messageId(keyOfInput(input))),now=Date.now();
    if(!row||row.expiresAt<=now||this.store.get('tombstone',messageId(row))) throw Error('This message is no longer available.');
    const actions=messageActions(row,now);
    if(actions.version!==input.expectedVersion) throw Error('This message changed. Open it again before editing or deleting.');
    if(input.action!=='delete-local'&&(!row.fromMe||!input.fromMe)) throw Error('Only your own messages can be changed for everyone.');
    if(input.action==='edit') {
      if(actions.editUntil<=now||row.kind!==input.kind) throw Error('This message can no longer be edited.');
      if(row.kind==='protected') {
        const before=protectedWire(row.wire),after=protectedWire(input.text);
        if(before.sender!==this.engine.account||after.contact!==before.contact||before.id===after.id) throw Error('The protected recipient changed. Send a new protected message instead.');
      }
    } else if(input.action==='delete-everyone'&&actions.deleteUntil<=now) throw Error('This message can no longer be deleted for everyone.');
    return row;
  }
  prepare(value) {
    const input=this.input(value),hash=digest(canonicalJSON(input)),old=this.store.get('mutation',input.operationId),prior=this.store.get('mutation-identity',input.operationId);
    if(old||prior) {
      if((old||prior).digest!==hash) throw Error('This action already refers to another change.');
      return old?this.view(old):{...prior.view,state:'cancelled'};
    }
    const target=this.target(input),tag=messageId(target);
    if(input.action!=='delete-local'&&this.store.list('mutation',{tag}).some(r=>pending.has(r.value.state))) throw Error('An earlier change is not confirmed. Check this message on your phone.');
    if(this.store.totals('mutation').count>=10000) throw Error('Local operation storage is full.');
    const row={operationId:input.operationId,chat:input.chat,targetId:input.messageId,fromMe:input.fromMe,action:input.action,digest:hash,...(input.action==='edit'?{contentDigest:digest(input.text)}:{}),request:input,state:'prepared',
      networkId:'3EB0'+digest('mutation:'+input.operationId).slice(0,28).toUpperCase(),createdAt:Date.now(),expiresAt:target.expiresAt};
    this.inbox.trim(Buffer.byteLength(JSON.stringify(row))+512);
    this.store.transaction(()=>{this.store.set('mutation-identity',row.operationId,{digest:hash,view:this.view({...row,request:undefined})},{expires:row.expiresAt+WEEK});this.save(row);});
    return this.view(row);
  }
  async run(value) {
    const input=this.input(value),prepared=this.prepare(input);
    if(prepared.state!=='prepared') return prepared;
    let row=this.store.get('mutation',input.operationId);
    const engine=this.engine,epoch=engine.epoch,socket=engine.socket;
    try { this.target(input); }
    catch { row={...row,state:'cancelled',request:undefined};this.save(row);engine.changed(row.chat);return this.view(row); }
    if(input.action!=='delete-local'&&(engine.state!=='connected'||!socket)) throw Error('Reconnect WhatsApp before changing this message.');
    row={...row,state:'sending'};this.save(row);engine.changed(row.chat);
    if(input.action==='delete-local') {
      this.store.transaction(()=>{this.inbox.tombstone(keyOfInput(input));row={...row,state:'sent',request:undefined};this.save(row);});
      return this.view(row);
    }
    const key={remoteJid:row.chat,id:row.targetId,fromMe:true};
    const job=(async()=>{
      try {
        const options={messageId:row.networkId},setting=this.store.get('setting',row.chat);
        if(input.action==='edit'&&setting) options.ephemeralExpiration=setting.seconds;
        await socket.sendMessage(row.chat,input.action==='edit'?{text:input.text,linkPreview:null,edit:key}:{delete:key},options);
        if(engine.closed||engine.stopping||engine.closing||epoch!==engine.epoch) throw Error('WhatsApp connection changed.');
        const current=this.store.get('mutation',row.operationId);
        if(current&&current.chat!==row.chat)return this.view(current);
        if(!current||current.state==='cancelled'){row={...row,state:'cancelled',request:undefined};return this.view(row);}
        this.store.transaction(()=>{
          if(input.action==='edit') this.inbox.edit(keyOfInput(input),{normalizedContent:extract({conversation:input.text})},row.createdAt,row.operationId);
          else this.inbox.tombstone(keyOfInput(input),Date.now(),row.operationId);
          row={...row,state:'sent',request:undefined};this.save(row);
        });
      } catch {
        if(!engine.closed&&!engine.stopping&&!engine.closing) {
          const latest=this.store.get('mutation',row.operationId);
          if(latest) {row={...latest,state:latest.state==='sent'?'sent':'uncertain',request:undefined};this.save(row);}
        }
      }
      if(engine.closed||engine.stopping||engine.closing||epoch!==engine.epoch) throw Error('WhatsApp connection changed.');
      engine.changed(row.chat);return this.view(row);
    })();
    engine.inflight.add(job);try{return await job;}finally{engine.inflight.delete(job);}
  }
}
