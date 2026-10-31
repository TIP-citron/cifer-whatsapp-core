import test from 'node:test';
import assert from 'node:assert/strict';
import { createCipheriv, hkdfSync } from 'node:crypto';
import { proto } from 'baileys';
import { fixture, fixtureMessage, drain, SELF } from './helpers.mjs';

const GROUP='120363000000001@g.us',SECRET=Buffer.alloc(32,41),IV=Buffer.alloc(12,42);
const contacts = count => Array.from({length:count},(_,i)=>({id:`${10000000000+i}@lid`,phoneNumber:`${15000000000+i}@s.whatsapp.net`,name:'Synthetic alias'}));
const original = (id,participant) => fixtureMessage(id,'original group body',{key:{id,remoteJid:GROUP,fromMe:false,participant},message:{conversation:'original group body',messageContextInfo:{messageSecret:SECRET}}});
function edit(id,participant,author) {
  const plain=proto.Message.encode({protocolMessage:{type:14,key:{remoteJid:GROUP,id,participant:author},editedMessage:{conversation:'corrected group body'},timestampMs:Date.now()}}).finish();
  const derived=hkdfSync('sha256',SECRET,Buffer.alloc(32),Buffer.from(id+author+author+'Message Edit'),32),cipher=createCipheriv('aes-256-gcm',derived,IV);cipher.setAAD(Buffer.alloc(0));
  const encPayload=Buffer.concat([cipher.update(plain),cipher.final(),cipher.getAuthTag()]);
  return fixtureMessage('ENVELOPE-'+id,'',{key:{id:'ENVELOPE-'+id,remoteJid:GROUP,fromMe:false,participant},message:{secretEncryptedMessage:{secretEncType:2,targetMessageKey:{remoteJid:GROUP,id,participant:author},encPayload,encIv:IV}}});
}
async function allMessages(engine) {
  const rows=[];let before;
  do{const page=await engine.request('messages',{chat:GROUP,limit:100,...(before?{before}:{})});rows.push(...page.messages);before=page.cursor;}while(before);
  return rows;
}

test('one contacts batch with more than 128 new aliases stays connected without pending edits',async t=>{
  const f=await fixture(t),socket=await f.connect(),batch=contacts(256);
  socket.ev.emit('contacts.upsert',batch);await drain();
  assert.equal((await f.engine.request('status')).state,'connected');assert.equal(socket.ended,false);
  assert.equal((await f.engine.request('chat',{chat:batch.at(-1).phoneNumber})).name,'Synthetic alias');
});

test('one discovered group roster with 160 new aliases does not overflow its own event queue',async t=>{
  const f=await fixture(t);await f.engine.request('pair');const socket=f.sockets[0],batch=contacts(160);
  socket.groupFetchAllParticipating=async()=>({[GROUP]:{id:GROUP,subject:'Large synthetic group',participants:[{id:SELF},...batch]}});
  socket.ev.emit('connection.update',{connection:'open'});await drain();
  assert.equal((await f.engine.request('status')).state,'connected');assert.equal(socket.ended,false);
  assert.equal((await f.engine.request('chat',{chat:GROUP})).participantCount,161);
});

test('alias-triggered encrypted edit replay coalesces every affected participant without dropping edits',async t=>{
  const f=await fixture(t),socket=await f.connect(),batch=contacts(160);
  socket.ev.emit('messages.upsert',{messages:batch.map((row,i)=>original('MESSAGE-'+i,row.phoneNumber))});await drain();
  socket.ev.emit('messages.upsert',{messages:batch.map((row,i)=>edit('MESSAGE-'+i,row.id,row.phoneNumber))});await drain();
  socket.ev.emit('contacts.upsert',batch);await drain();await drain();
  assert.equal((await f.engine.request('status')).state,'connected');assert.equal(socket.ended,false);
  const rows=await allMessages(f.engine);assert.equal(rows.length,batch.length);assert.ok(rows.every(row=>row.text==='corrected group body'));assert.ok(rows.every(row=>row.participant.endsWith('@s.whatsapp.net')));
});

test('delayed coalesced alias replay cannot apply across reconnect and the next generation can finish it',async t=>{
  const f=await fixture(t),socket=await f.connect(),[contact]=contacts(1),id='DELAYED';
  socket.ev.emit('messages.upsert',{messages:[original(id,contact.phoneNumber),edit(id,contact.id,contact.phoneNumber)]});await drain();
  let release;
  socket.signalRepository.lidMapping.getLIDForPN=()=>new Promise(resolve=>{release=resolve;});
  socket.ev.emit('contacts.upsert',[contact]);await drain();assert.equal(typeof release,'function');
  socket.ev.emit('connection.update',{connection:'close',lastDisconnect:{error:{output:{statusCode:428}}}});
  await f.engine.request('pair');const next=f.sockets.at(-1);next.ev.emit('connection.update',{connection:'open'});release(contact.id);await drain();
  assert.equal((await allMessages(f.engine))[0].text,'original group body');
  next.ev.emit('messages.upsert',{messages:[original(id,contact.phoneNumber)]});await drain();
  assert.equal((await allMessages(f.engine))[0].text,'corrected group body');assert.equal((await f.engine.request('status')).state,'connected');
});

test('blocked discovery network query does not stop 129 incremental inbound chat events',async t=>{
  const f=await fixture(t);await f.engine.request('pair');const socket=f.sockets[0];let release;
  socket.groupFetchAllParticipating=()=>new Promise(resolve=>{release=resolve;});
  socket.ev.emit('connection.update',{connection:'open'});await drain();
  try{
    for(let i=0;i<129;i++){socket.ev.emit('chats.update',[{id:`${17000000000+i}@s.whatsapp.net`,name:'Inbound during discovery'}]);await new Promise(resolve=>setImmediate(resolve));}
    assert.equal((await f.engine.request('status')).state,'connected');assert.equal(socket.ended,false);
    assert.equal((await f.engine.request('chat',{chat:'17000000128@s.whatsapp.net'})).name,'Inbound during discovery');
  }finally{release({});await drain();}
});

for(const shallowCopy of [false,true])test(`discovery echo ${shallowCopy?'through shallow buffer':'directly emitted'} cannot overwrite a genuine concurrent full roster`,async t=>{
  const f=await fixture(t);await f.engine.request('pair');const socket=f.sockets[0];let release,refreshes=0;
  const stale={id:GROUP,subject:'Old subject',announce:false,participants:[{id:SELF,admin:'admin'},{id:'15550000001@s.whatsapp.net'}]};
  const current={id:GROUP,subject:'Updated subject',announce:true,participants:[{id:SELF},{id:'15550000001@s.whatsapp.net',admin:'admin'}]};
  socket.groupFetchAllParticipating=async()=>{await new Promise(resolve=>{release=resolve;});socket.ev.emit('groups.update',[shallowCopy?{...stale}:stale]);return {[GROUP]:stale};};
  socket.groupMetadata=async()=>{refreshes++;return current;};
  socket.ev.emit('connection.update',{connection:'open'});await drain();
  socket.ev.emit('groups.update',[current]);await drain();
  let chat=await f.engine.request('chat',{chat:GROUP});assert.equal(chat.name,current.subject);assert.equal(chat.sendRestriction,'admins-only');
  release();await drain();await drain();
  chat=await f.engine.request('chat',{chat:GROUP});assert.equal(chat.name,current.subject);assert.equal(chat.canSend,false);assert.equal(chat.sendRestriction,'admins-only');assert.equal(refreshes,1);
});

test('a delayed discovery response cannot undo an observed removal or fresh authorization result',async t=>{
  const f=await fixture(t);await f.engine.request('pair');const socket=f.sockets[0];let release;
  const stale={id:GROUP,subject:'Old membership',participants:[{id:SELF,admin:'admin'},{id:'15550000001@s.whatsapp.net'}]};
  socket.groupFetchAllParticipating=async()=>{await new Promise(resolve=>{release=resolve;});socket.ev.emit('groups.update',[{...stale}]);return {[GROUP]:stale};};
  socket.groupMetadata=async()=>({id:GROUP,subject:'Current membership',participants:[{id:'15550000001@s.whatsapp.net',admin:'admin'}]});
  socket.ev.emit('connection.update',{connection:'open'});await drain();
  socket.ev.emit('group-participants.update',{id:GROUP,action:'remove',participants:[{id:SELF}]});await drain();await drain();
  assert.equal((await f.engine.request('chat',{chat:GROUP})).sendRestriction,'left');
  release();await drain();await drain();
  const chat=await f.engine.request('chat',{chat:GROUP});assert.equal(chat.canSend,false);assert.equal(chat.sendRestriction,'left');assert.equal(socket.ended,false);
});

test('a genuine admin-only update merged into the roster echo is preserved despite shared array identity',async t=>{
  const f=await fixture(t);await f.engine.request('pair');const socket=f.sockets[0];let refreshes=0;
  const stale={id:GROUP,subject:'Synthetic announcements',announce:false,participants:[{id:SELF},{id:'15550000001@s.whatsapp.net',admin:'admin'}]};
  const merged={...stale,announce:true};
  socket.groupFetchAllParticipating=async()=>{socket.ev.emit('groups.update',[merged]);return {[GROUP]:stale};};
  socket.groupMetadata=async()=>{refreshes++;return merged;};
  socket.ev.emit('connection.update',{connection:'open'});await drain();await drain();
  const chat=await f.engine.request('chat',{chat:GROUP});assert.equal(chat.canSend,false);assert.equal(chat.sendRestriction,'admins-only');assert.equal(refreshes,1);
});

test('an exact discovery echo is applied once without redundant per-group refresh',async t=>{
  const f=await fixture(t);await f.engine.request('pair');const socket=f.sockets[0];let refreshes=0;
  const metadata={id:GROUP,subject:'Exact echo',participants:[{id:SELF}]};
  socket.groupFetchAllParticipating=async()=>{socket.ev.emit('groups.update',[{...metadata}]);return {[GROUP]:metadata};};
  socket.groupMetadata=async()=>{refreshes++;return metadata;};
  socket.ev.emit('connection.update',{connection:'open'});await drain();await drain();
  assert.equal((await f.engine.request('chat',{chat:GROUP})).canSend,true);assert.equal(refreshes,0);
});

test('merged discovery announcement cannot restore an observed removal when fresh metadata is blocked',async t=>{
  const f=await fixture(t);await f.engine.request('pair');const socket=f.sockets[0];let release;
  const stale={id:GROUP,subject:'Old admin roster',announce:false,participants:[{id:SELF,admin:'admin'},{id:'15550000001@s.whatsapp.net'}]};
  socket.groupFetchAllParticipating=async()=>{await new Promise(resolve=>{release=resolve;});socket.ev.emit('groups.update',[{...stale,announce:true}]);return {[GROUP]:stale};};
  socket.groupMetadata=()=>new Promise(()=>{});
  socket.ev.emit('connection.update',{connection:'open'});await drain();
  socket.ev.emit('group-participants.update',{id:GROUP,action:'remove',participants:[{id:SELF}]});await drain();
  assert.equal((await f.engine.request('chat',{chat:GROUP})).sendRestriction,'left');
  release();await drain();await drain();
  const chat=await f.engine.request('chat',{chat:GROUP});assert.equal(chat.announce,true);assert.equal(chat.member,false);assert.equal(chat.ownAdmin,false);assert.equal(chat.canSend,false);assert.equal(chat.sendRestriction,'left');
});
