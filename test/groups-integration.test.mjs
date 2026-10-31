import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { fixture, fixtureMessage, drain, SELF, PEER } from './helpers.mjs';

const GROUP='120363000000001@g.us', OTHER='15550000003@s.whatsapp.net';
const groupMessage=(id,text,participant=PEER,extra={})=>fixtureMessage(id,text,{key:{remoteJid:GROUP,id,fromMe:participant===SELF,participant},pushName:participant===PEER?'Member A':'Member B',...extra});
async function connected(t){
  const f=await fixture(t);await f.engine.request('pair');const socket=f.sockets[0];
  const metadata={id:GROUP,subject:'Controlled test group',announce:false,participants:[{id:SELF,admin:'admin'},{id:PEER},{id:OTHER}]};
  socket.groupFetchAllParticipating=async()=>({[GROUP]:metadata});socket.groupMetadata=async()=>metadata;
  socket.readKeys=[];socket.patches=[];socket.readMessages=async keys=>socket.readKeys.push(...keys);
  socket.chatModify=async(mod,chat)=>{socket.patches.push({mod,chat});socket.ev.emit('chats.update',[{id:chat,...('archive' in mod?{archived:mod.archive}:{}),...('markRead' in mod?{unreadCount:0}:{})}]);};
  socket.config.auth.creds.myAppStateKeyId='fixture-key';
  socket.ev.emit('connection.update',{connection:'open',receivedPendingNotifications:true});await drain();await drain();
  return {...f,engine:f.engine,socket,metadata};
}
test('group discovery, author collision, phone aliases, receipts and own edits stay isolated',async t=>{
  const f=await connected(t),{socket,engine}=f;
  assert.equal((await engine.request('chat',{chat:GROUP})).name,'Controlled test group');
  socket.ev.emit('messages.upsert',{messages:[groupMessage('COLLISION','from A'),groupMessage('COLLISION','from B',OTHER)]});await drain();
  let messages=(await engine.request('messages',{chat:GROUP})).messages;
  assert.equal(messages.length,2);assert.deepEqual(new Set(messages.map(m=>m.participant)),new Set([PEER,OTHER]));
  assert.equal((await engine.request('chat',{chat:GROUP})).name,'Controlled test group');
  const sent=await engine.request('send',{chat:GROUP,text:'ordinary group canary',kind:'ordinary',operationId:randomUUID()});
  assert.equal(sent.state,'sent');
  socket.ev.emit('message-receipt.update',[{key:{remoteJid:GROUP,id:sent.messageId,fromMe:true,participant:PEER},receipt:{userJid:PEER,readTimestamp:Math.floor(Date.now()/1000)}}]);await drain();
  let own=(await engine.request('messages',{chat:GROUP})).messages.find(m=>m.id===sent.messageId);
  assert.equal(own.delivery,'sent');assert.equal(own.readCount,1);assert.equal(own.deliveredCount,1);
  const lid='19998887777@lid';socket.ev.emit('contacts.upsert',[{id:lid,phoneNumber:PEER}]);await drain();
  socket.ev.emit('message-receipt.update',[{key:{remoteJid:GROUP,id:sent.messageId,fromMe:true,participant:lid},receipt:{userJid:lid,readTimestamp:Math.floor(Date.now()/1000)}}]);await drain();
  own=(await engine.request('messages',{chat:GROUP})).messages.find(m=>m.id===sent.messageId);assert.equal(own.readCount,1);
  await assert.rejects(engine.request('send',{chat:GROUP,text:'CIFER:WA:2:invalid',kind:'protected',operationId:randomUUID()}),/protection/);
  socket.ev.emit('messages.update',[{key:{remoteJid:GROUP,id:'COLLISION',participant:OTHER,fromMe:false},update:{message:null}}]);await drain();
  assert.equal((await engine.request('messages',{chat:GROUP})).messages.filter(m=>m.id==='COLLISION').length,2,'lossy group revoke must not delete by ID');
  socket.ev.emit('messages.upsert',{messages:[groupMessage('EDIT','',PEER,{message:{protocolMessage:{type:14,key:{remoteJid:GROUP,id:'COLLISION',participant:PEER},editedMessage:{conversation:'A edited'},timestampMs:Date.now()}}})]});await drain();
  messages=(await engine.request('messages',{chat:GROUP})).messages;
  assert.equal(messages.find(m=>m.id==='COLLISION'&&m.participant===PEER).text,'A edited');
  assert.equal(messages.find(m=>m.id==='COLLISION'&&m.participant===OTHER).text,'from B');
});
test('group archive is quiet, keeps selected summary, and round-trips through native read sync',async t=>{
  const {engine,socket}=await connected(t);
  socket.ev.emit('messages.upsert',{messages:[groupMessage('ARCHIVE','unread group canary')]});await drain();
  assert.equal((await engine.request('status')).unreadTotal,1);
  const result=await engine.request('setArchived',{chat:GROUP,archived:true,operationId:randomUUID()});await drain();
  assert.equal(result.state,'sent');assert.equal((await engine.request('status')).unreadTotal,0);
  assert.equal((await engine.request('status')).archivedUnreadTotal,1);
  assert.equal((await engine.request('chats')).chats.some(c=>c.id===GROUP),false);
  assert.equal((await engine.request('chats',{view:'archived'})).chats[0].id,GROUP);
  assert.equal((await engine.request('chat',{chat:GROUP})).unread,1);
  await engine.request('markRead',{chat:GROUP,onlyIfUnarchived:true});
  assert.equal((await engine.request('chat',{chat:GROUP})).unread,1);
  await engine.request('markRead',{chat:GROUP,messages:[{id:'ARCHIVE',fromMe:false,participant:PEER}]});await drain();
  assert.ok(socket.readKeys.some(k=>k.id==='ARCHIVE'&&k.participant===PEER));
  assert.equal((await engine.request('status')).archivedUnreadTotal,0);
  socket.ev.emit('chats.update',[{id:GROUP,archived:false}]);await drain();
  assert.equal((await engine.request('chat',{chat:GROUP})).archived,false);
});
test('group discovery does not restore permission or write after helper close',async t=>{
  const f=await fixture(t);await f.engine.request('pair');const socket=f.sockets[0];let resolve;
  socket.groupFetchAllParticipating=()=>new Promise(r=>{resolve=r;});
  socket.ev.emit('connection.update',{connection:'open'});await drain();
  const closing=f.engine.close();await drain();resolve({[GROUP]:{id:GROUP,subject:'Late group',participants:[{id:SELF}]}});await closing;
  await f.reopen();
  assert.equal(await f.engine.request('chat',{chat:GROUP}),null);
});

for(const type of ['messages.upsert','messaging-history.set'])test(`old ${type} cannot continue ingestion after an awaited alias lookup crosses reconnect`,async t=>{
  const f=await fixture(t),socket=await f.connect();let resolve;
  const lid='18887776666@lid';socket.signalRepository.lidMapping.getPNForLID=()=>new Promise(r=>{resolve=r;});
  const old=fixtureMessage('STALE_AFTER_AWAIT','must never appear',{key:{remoteJid:lid,id:'STALE_AFTER_AWAIT',fromMe:false}});
  socket.ev.emit(type,{messages:[old]});await drain();assert.equal(typeof resolve,'function');
  socket.ev.emit('connection.update',{connection:'close',lastDisconnect:{error:{output:{statusCode:428}}}});
  await f.engine.request('pair');f.sockets.at(-1).ev.emit('connection.update',{connection:'open'});
  resolve(PEER);await drain();await drain();
  assert.equal((await f.engine.request('messages',{chat:PEER})).messages.length,0);
  assert.equal((await f.engine.request('messages',{chat:lid})).messages.length,0);
});

test('reconnect readiness cannot archive a stale range while offline captures wait on group discovery',async t=>{
  const f=await fixture(t),first=await f.connect(),now=Math.floor(Date.now()/1000);
  first.config.auth.creds.myAppStateKeyId='fixture-key';
  first.ev.emit('creds.update',{});
  first.ev.emit('connection.update',{receivedPendingNotifications:true});
  first.ev.emit('messages.upsert',{type:'notify',messages:[fixtureMessage('BEFORE','before reconnect',{messageTimestamp:now-100})]});
  await drain();await drain();await f.reopen();
  const socket=f.sockets.at(-1);let release;
  socket.groupFetchAllParticipating=()=>new Promise(resolve=>{release=()=>resolve({});});
  socket.patches=[];socket.chatModify=async(mod,chat)=>socket.patches.push({mod,chat});
  socket.ev.emit('connection.update',{connection:'open'});await drain();
  socket.ev.emit('messages.upsert',{type:'append',messages:[fixtureMessage('OFFLINE-NEW','received while offline',{messageTimestamp:now})]});
  socket.ev.emit('connection.update',{receivedPendingNotifications:true});
  const args={chat:PEER,archived:true,operationId:randomUUID()};
  try{
    await assert.rejects(f.engine.request('setArchived',args),/synchronization/);
    assert.equal(socket.patches.length,0);
  }finally{release();}
  await drain();await drain();
  assert.equal((await f.engine.request('setArchived',args)).state,'sent');
  assert.deepEqual(socket.patches[0].mod.lastMessages.messages.map(row=>row.key.id),['OFFLINE-NEW']);
});
