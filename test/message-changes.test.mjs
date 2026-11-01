import test from 'node:test';
import assert from 'node:assert/strict';
import { fixture, fixtureMessage, drain, PEER } from './helpers.mjs';
test('committed changes distinguish append, metadata and deletion with complete identity',async t=>{
  const f=await fixture(t),socket=await f.connect();
  socket.ev.emit('messages.upsert',{messages:[fixtureMessage('ONE')]});await drain();
  const snapshot=await f.engine.request('messages',{chat:PEER});assert.ok(snapshot.revision>0);
  socket.ev.emit('messages.upsert',{messages:[fixtureMessage('TWO')]});await drain();
  const diff=await f.engine.request('changes',{chat:PEER,after:snapshot.revision});
  assert.equal(diff.reset,false);assert.equal(diff.changes[0].type,'upsert');assert.equal(diff.changes[0].fromMe,false);
  socket.ev.emit('messages.delete',{keys:[{remoteJid:PEER,id:'ONE',fromMe:false}]});await drain();
  const event=f.events.filter(e=>e.type==='changed').at(-1);assert.equal(event.changes[0].type,'delete');assert.equal(event.changes[0].id,'ONE');
  assert.equal((await f.engine.request('changes',{chat:PEER,after:99999999})).reset,true);
});
test('automatic history recovery is reused within a connection and requested after reconnect',async t=>{
  const f=await fixture(t),socket=await f.connect();
  socket.ev.emit('messages.upsert',{messages:[fixtureMessage('RECENT')]});await drain();
  await f.engine.request('history',{chat:PEER,recent:true,automatic:true});
  socket.ev.emit('messaging-history.set',{peerDataRequestSessionId:socket.history[0].id,messages:[fixtureMessage('RECENT')],contacts:[],chats:[],progress:100});await drain();
  await f.engine.request('history',{chat:PEER,recent:true,automatic:true});assert.equal(socket.history.length,1);
  await f.reopen();const reconnected=await f.connect();
  await f.engine.request('history',{chat:PEER,recent:true,automatic:true});assert.equal(reconnected.history.length,1);
});
