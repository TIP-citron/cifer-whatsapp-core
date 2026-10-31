import test from 'node:test';
import assert from 'node:assert/strict';
import pino from 'pino';
import { makeEventBuffer } from 'baileys/lib/Utils/event-buffer.js';
import processMessage from 'baileys/lib/Utils/process-message.js';
import { proto } from 'baileys';
import { randomUUID } from 'node:crypto';
import { Store } from '../src/store.mjs';
import { EDIT_WINDOW, DELETE_WINDOW, messageId, extract } from '../src/content.mjs';
import { fixture, fixtureMessage, drain, PEER, SELF, wire, ACCOUNT } from './helpers.mjs';

for (const history of [false, true]) for (const revoke of [false, true]) {
  test(`buffered ${history ? 'history' : 'upsert'} preserves ${revoke ? 'revocation' : 'edit'} of an existing message`, async t => {
    const f = await fixture(t), socket = await f.connect();
    const original = () => fixtureMessage('BUFFERED', 'before edit', { messageTimestamp: Math.floor(Date.now()/1000)-30 });
    socket.ev.emit('messages.upsert', { messages: [original()] }); await drain();
    const ev = makeEventBuffer(pino({level:'silent'}));
    for (const name of ['messages.upsert','messages.update','messaging-history.set']) ev.on(name, data => socket.ev.emit(name,data));
    ev.buffer();
    if (history) ev.emit('messaging-history.set', {chats:[],contacts:[],messages:[original()],isLatest:false});
    else ev.emit('messages.upsert', {messages:[original()],type:'append'});
    const mutation = fixtureMessage('MUTATION','',{message:{protocolMessage:{
      type:revoke ? proto.Message.ProtocolMessage.Type.REVOKE : proto.Message.ProtocolMessage.Type.MESSAGE_EDIT,
      key:{id:'BUFFERED'},editedMessage:{conversation:'after edit'},timestampMs:Date.now()
    }}});
    // Match Baileys Socket/chats upsertMessage: enqueue the envelope before processing it.
    ev.emit('messages.upsert',{messages:[mutation],type:'notify'});
    await processMessage(mutation, {ev,creds:{me:{id:SELF}}});
    ev.flush(); await drain();
    const rows = (await f.engine.request('messages',{chat:PEER})).messages;
    if (revoke) assert.equal(rows.length,0);
    else { assert.equal(rows[0].text,'after edit'); assert.ok(rows[0].editedAt); }
  });
}

const outgoing = (id,text='original',extra={}) => fixtureMessage(id,text,{key:{id,remoteJid:PEER,fromMe:true},status:3,...extra});
const read = async f => (await f.engine.request('messages',{chat:PEER})).messages;
const input = (row,action='edit',extra={}) => ({operationId:randomUUID(),chat:row.chat,messageId:row.id,fromMe:row.fromMe,expectedVersion:row.version,action,...(action==='edit'?{text:'replacement',kind:row.kind}:{}),...extra});

test('decoded protobuf extended text and edits do not mistake default empty conversation for the body',()=>{
  const raw=proto.Message.decode(proto.Message.encode({extendedTextMessage:{text:'actual text'}}).finish());
  assert.equal(extract(raw).text,'actual text');
});
test('edits arriving before originals survive restart and replay cannot restore the former text',async t=>{
  const f=await fixture(t),socket=await f.connect(),at=Date.now();
  socket.ev.emit('messages.update',[{key:{id:'LATEBASE',remoteJid:PEER,fromMe:false},update:{message:{editedMessage:{message:{conversation:'new text'}}},messageTimestamp:Math.floor(at/1000)}}]);await drain();
  await f.reopen();f.sockets.at(-1).ev.emit('connection.update',{connection:'open'});
  const original=fixtureMessage('LATEBASE','old text',{messageTimestamp:Math.floor(at/1000)-20});
  f.sockets.at(-1).ev.emit('messages.upsert',{messages:[original]});await drain();
  assert.equal((await read(f))[0].text,'new text');assert.equal((await read(f))[0].createdAt,original.messageTimestamp*1000);
  f.sockets.at(-1).ev.emit('messages.upsert',{messages:[original]});await drain();assert.equal((await read(f))[0].text,'new text');
});
test('durable own edit sends one protocol mutation, keeps creation/expiry and survives restart',async t=>{
  const f=await fixture(t),socket=await f.connect();socket.ev.emit('messages.upsert',{messages:[outgoing('OWN')]});await drain();
  const before=(await read(f))[0],args=input(before);assert.equal((await f.engine.request('prepareMutation',args)).state,'prepared');assert.equal(socket.sent.length,0);
  await f.reopen();const next=f.sockets.at(-1);next.ev.emit('connection.update',{connection:'open'});
  assert.equal((await f.engine.request('mutate',args)).state,'sent');assert.equal(next.sent.length,1);assert.deepEqual(next.sent[0].content.edit,{id:'OWN',remoteJid:PEER,fromMe:true});
  let after=(await read(f))[0];assert.equal(after.text,'replacement');assert.ok(after.editedAt);assert.equal(after.createdAt,before.createdAt);assert.equal(after.expiresAt,before.expiresAt);
  await f.engine.request('mutate',args);assert.equal(next.sent.length,1);
  await assert.rejects(f.engine.request('mutate',{...args,text:'other'}),/another change/);
});
test('ownership, expiry, media, stale version and changing protection are enforced before I/O',async t=>{
  const f=await fixture(t),socket=await f.connect();
  socket.ev.emit('messages.upsert',{messages:[fixtureMessage('THEIRS'),outgoing('OLD','old',{messageTimestamp:Math.floor((Date.now()-EDIT_WINDOW-1000)/1000)}),outgoing('MEDIA','',{message:{imageMessage:{caption:'caption'}}}),outgoing('OWN')]});await drain();
  const rows=await read(f);
  for(const id of ['THEIRS','OLD','MEDIA']) await assert.rejects(f.engine.request('mutate',input(rows.find(r=>r.id===id))));
  const own=rows.find(r=>r.id==='OWN');await assert.rejects(f.engine.request('mutate',input(own,'edit',{expectedVersion:'a'.repeat(64)})),/changed/);
  await assert.rejects(f.engine.request('mutate',input(rows.find(r=>r.id==='THEIRS'),'delete-everyone')),/own messages/);
  assert.equal(socket.sent.length,0);
});
test('delete for everyone uses a revoke, erases caches/outbox and blocks history resurrection',async t=>{
  const f=await fixture(t),socket=await f.connect();socket.ev.emit('messages.upsert',{messages:[outgoing('DELETE')]});await drain();
  const args=input((await read(f))[0],'delete-everyone');assert.equal((await f.engine.request('mutate',args)).state,'sent');assert.equal(socket.sent.length,1);assert.equal(socket.sent[0].content.delete.id,'DELETE');assert.equal((await read(f)).length,0);
  socket.ev.emit('messages.upsert',{messages:[outgoing('DELETE')]});await drain();assert.equal((await read(f)).length,0);
  await f.engine.request('mutate',args);assert.equal(socket.sent.length,1);
});
test('delete on this Mac is available for received messages while offline and never sends a revoke',async t=>{
  const f=await fixture(t),socket=await f.connect();socket.ev.emit('messages.upsert',{messages:[fixtureMessage('LOCAL')]});await drain();
  const args=input((await read(f))[0],'delete-local');socket.ev.emit('connection.update',{connection:'close'});await drain();
  assert.equal((await f.engine.request('mutate',args)).state,'sent');assert.equal(socket.sent.length,0);assert.equal((await read(f)).length,0);
});
test('remote deletion window is rechecked after durable preparation',async t=>{
  const f=await fixture(t),socket=await f.connect();socket.ev.emit('messages.upsert',{messages:[outgoing('WINDOW','x',{messageTimestamp:Math.floor((Date.now()-DELETE_WINDOW+5000)/1000)})]});await drain();
  const args=input((await read(f))[0],'delete-everyone');await f.engine.request('prepareMutation',args);
  const real=Date.now,now=real();Date.now=()=>now+10000;try{assert.equal((await f.engine.request('mutate',args)).state,'cancelled');assert.equal(socket.sent.length,0);}finally{Date.now=real;}
});
test('ambiguous edit is uncertain after restart and never automatically or manually repeats the same operation',async t=>{
  const f=await fixture(t),socket=await f.connect();socket.ev.emit('messages.upsert',{messages:[outgoing('AMBIGUOUS')]});await drain();
  const args=input((await read(f))[0]);let calls=0;socket.sendMessage=async()=>{calls++;throw Error('network interrupted');};
  assert.equal((await f.engine.request('mutate',args)).state,'uncertain');assert.equal(calls,1);assert.equal((await read(f))[0].text,'original');
  await f.reopen();f.sockets.at(-1).ev.emit('connection.update',{connection:'open'});
  assert.equal((await f.engine.request('mutate',args)).state,'uncertain');assert.equal(f.sockets.at(-1).sent.length,0);
  await assert.rejects(f.engine.request('mutate',input((await read(f))[0])),/not confirmed/);
});
test('a phone deletion cancels prepared edits and a delayed send cannot recreate the row',async t=>{
  const f=await fixture(t),socket=await f.connect();socket.ev.emit('messages.upsert',{messages:[outgoing('RACE')]});await drain();
  const args=input((await read(f))[0]);let finish;socket.sendMessage=()=>new Promise(resolve=>{finish=resolve;});
  const sending=f.engine.request('mutate',args);await drain();socket.ev.emit('messages.delete',{keys:[{id:'RACE',remoteJid:PEER,fromMe:true}]});await drain();finish({});
  assert.equal((await sending).state,'cancelled');assert.equal((await read(f)).length,0);
});
test('a close fences mutation completion and reopening recovers an uncertain journal',async t=>{
  const f=await fixture(t),socket=await f.connect();socket.ev.emit('messages.upsert',{messages:[outgoing('LOCK')]});await drain();
  const args=input((await read(f))[0]);let finish;socket.sendMessage=()=>new Promise(resolve=>{finish=resolve;});const sending=f.engine.request('mutate',args);await drain();
  const rejected=assert.rejects(sending,/connection changed/),closing=f.engine.close();finish({});await rejected;await closing;
  await f.reopen();assert.equal((await f.engine.request('mutations',{chat:PEER})).mutations[0].state,'uncertain');
});
test('protected edits require new ciphertext for the same verified recipient, never ordinary downgrade',async t=>{
  const f=await fixture(t),socket=await f.connect(),oldWire=wire(),newWire=wire();socket.ev.emit('messages.upsert',{messages:[outgoing('PROTECTED',oldWire)]});await drain();
  const row=(await read(f))[0];await assert.rejects(f.engine.request('mutate',input(row,'edit',{kind:'ordinary'})),/no longer be edited/);
  const owner={principal:ACCOUNT,secretId:'101',publicKey:Buffer.alloc(1184).toString('base64')};
  const prepared={version:1,principal:ACCOUNT,chat:PEER,owner,binding:null,wire:newWire,recovery:{encrypted:'11'.repeat(128),cifer:'22'.repeat(1104)},recipientGrant:null};
  const args=input(row,'edit',{text:newWire,kind:'protected',prepared});assert.equal((await f.engine.request('mutate',args)).state,'sent');assert.equal((await read(f))[0].wire,newWire);assert.equal(socket.sent[0].content.text,newWire);
});

test('mutation protocol constants match the pinned Baileys schema',()=>{assert.equal(proto.Message.ProtocolMessage.Type.MESSAGE_EDIT,14);assert.equal(proto.Message.ProtocolMessage.Type.REVOKE,0);});
test('authenticated envelope direction and peer override hostile nested mutation keys',async t=>{
 const f=await fixture(t),socket=await f.connect();socket.ev.emit('messages.upsert',{messages:[outgoing('COLLISION'),fixtureMessage('COLLISION','their original')]});await drain();
 socket.ev.emit('messages.upsert',{messages:[fixtureMessage('EVIL','',{message:{protocolMessage:{type:14,key:{id:'COLLISION',fromMe:true,remoteJid:'15559999999@s.whatsapp.net'},editedMessage:{conversation:'their replacement'},timestampMs:Date.now()}}})]});await drain();
 const rows=await read(f);assert.equal(rows.find(r=>r.fromMe).text,'original');assert.equal(rows.find(r=>!r.fromMe).text,'their replacement');
});
test('deletion metadata never retains message bodies, including legacy tombstone cleanup on upgrade',async t=>{
 const f=await fixture(t),socket=await f.connect();socket.ev.emit('messages.upsert',{messages:[outgoing('SCRUB','private original')]});await drain();const row=(await read(f))[0];
 await f.engine.request('mutate',input(row,'delete-local'));await f.engine.close();
 let store=new Store(f.directory,f.config.key,{account:f.config.account,connection:f.config.connection});assert.equal(JSON.stringify(store.list('tombstone')).includes('private original'),false);
 store.set('tombstone',messageId(row),{...row,deleted:true},{expires:Date.now()+60000,tag:PEER});store.remove('system','content-free-tombstones-v1');store.close();
 await f.reopen();await f.engine.close();store=new Store(f.directory,f.config.key,{account:f.config.account,connection:f.config.connection});assert.equal(JSON.stringify(store.list('tombstone')).includes('private original'),false);store.close();
});

test('first history sync renders an already edited message with a conservative retention deadline',async t=>{
 const f=await fixture(t),socket=await f.connect(),at=Math.floor(Date.now()/1000)*1000;
 socket.ev.emit('messaging-history.set',{contacts:[],chats:[],messages:[fixtureMessage('EDITED-HISTORY','',{messageTimestamp:at/1000,message:{editedMessage:{message:{conversation:'current history text'}}}})]});await drain();
 const row=(await read(f))[0];assert.equal(row.text,'current history text');assert.equal(row.editedAt,at);assert.ok(row.expiresAt<=at-EDIT_WINDOW+604800000);
});

test('identity reconciliation fences an in-flight alias mutation without recreating alias content',async t=>{
 const f=await fixture(t),socket=await f.connect(),lid='1234567890@lid';socket.ev.emit('messages.upsert',{messages:[outgoing('ALIAS-RACE','original',{key:{id:'ALIAS-RACE',remoteJid:lid,fromMe:true}})]});await drain();
 const row=(await f.engine.request('messages',{chat:lid})).messages[0],args=input(row);let finish;socket.sendMessage=()=>new Promise(resolve=>{finish=resolve;});
 const sending=f.engine.request('mutate',args);await drain();socket.ev.emit('contacts.upsert',[{id:lid,phoneNumber:PEER}]);await drain();finish({});
 const result=await sending;assert.equal(result.state,'uncertain');assert.equal(result.chat,PEER);assert.equal((await read(f))[0].text,'original');
 const changes=(await f.engine.request('mutations',{chat:PEER})).mutations;assert.equal(changes[0].state,'uncertain');assert.equal(changes[0].request,undefined);
});
