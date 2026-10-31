import test from 'node:test';
import assert from 'node:assert/strict';
import { createCipheriv, hkdfSync } from 'node:crypto';
import { proto } from 'baileys';
import { Groups } from '../src/groups.mjs';
import { Store } from '../src/store.mjs';
import { fixture, fixtureMessage, drain, SELF, PEER } from './helpers.mjs';

const GROUP='120363099999999@g.us',SECRET=Buffer.alloc(32,41),IV=Buffer.alloc(12,42);
function originalsAndEdits(count){
  const originals=[],edits=[];
  for(let i=0;i<count;i++){
    const id=`DISCOVERY-DIRECT-${i}`;
    originals.push(fixtureMessage(id,'synthetic original',{message:{conversation:'synthetic original',messageContextInfo:{messageSecret:SECRET}}}));
    const plain=proto.Message.encode({protocolMessage:{type:14,key:{remoteJid:PEER,id},editedMessage:{conversation:'synthetic edited'},timestampMs:Date.now()}}).finish();
    const derived=hkdfSync('sha256',SECRET,Buffer.alloc(32),Buffer.from(id+PEER+PEER+'Message Edit'),32),cipher=createCipheriv('aes-256-gcm',derived,IV);cipher.setAAD(Buffer.alloc(0));
    edits.push(fixtureMessage(`DISCOVERY-ENVELOPE-${i}`,'',{message:{secretEncryptedMessage:{secretEncType:2,targetMessageKey:{remoteJid:PEER,id},encPayload:Buffer.concat([cipher.update(plain),cipher.final(),cipher.getAuthTag()]),encIv:IV}}}));
  }
  return {originals,edits};
}
async function startWaitingDiscovery(t,count,mapping){
  const f=await fixture(t);await f.engine.request('pair');const socket=f.sockets[0];let release;
  const row={id:GROUP,subject:'Synthetic discovered group',participants:[{id:SELF},{id:PEER}]};
  socket.groupFetchAllParticipating=async()=>{await new Promise(resolve=>{release=resolve;});socket.ev.emit('groups.update',[row]);return {[GROUP]:row};};
  socket.signalRepository.lidMapping.getLIDForPN=mapping;
  t.mock.timers.enable({apis:['setTimeout','setInterval']});
  socket.ev.emit('connection.update',{connection:'open'});await drain();
  const {originals,edits}=originalsAndEdits(count);
  socket.ev.emit('messages.upsert',{messages:originals});await drain();
  socket.ev.emit('messages.upsert',{messages:edits});await drain();
  release();await drain();return {...f,socket};
}

test('discovery apply survives queue wait behind nine valid encrypted-edit author lookups',async t=>{
  let lookups=0;
  const f=await startWaitingDiscovery(t,9,()=>{lookups++;return new Promise(()=>{});});
  for(let i=0;i<9;i++){assert.equal(lookups,i+1);t.mock.timers.tick(1001);await drain();}
  const chat=await f.engine.request('chat',{chat:GROUP});
  assert.equal(chat?.name,'Synthetic discovered group');assert.equal(chat.participantCount,2);
  const {messages}=await f.engine.request('messages',{chat:PEER,limit:100});assert.equal(messages.filter(m=>m.text==='synthetic edited').length,9);
  assert.equal((await f.engine.request('status')).state,'connected');assert.equal(f.socket.ended,false);
});

test('closing while discovery apply waits cancels skipped callbacks without late writes or unhandled rejection',async t=>{
  let finishLookup;const unhandled=[];
  const onUnhandled=error=>unhandled.push(error);process.on('unhandledRejection',onUnhandled);t.after(()=>process.off('unhandledRejection',onUnhandled));
  const f=await startWaitingDiscovery(t,1,()=>new Promise(resolve=>{finishLookup=resolve;}));
  const closing=f.engine.close();t.mock.timers.tick(50);await drain();
  finishLookup(null);await drain();await closing;await drain();
  const store=new Store(f.directory,f.config.key,{account:f.config.account,connection:f.config.connection});
  try{assert.equal(store.get('chat',GROUP),null);assert.equal(store.totals('group-role').count,0);}finally{store.close();}
  t.mock.timers.tick(9000);await drain();assert.deepEqual(unhandled,[]);
});

function queuedGroups(){
  const callbacks=[],engine={epoch:1,closed:false,socket:{},enqueue(epoch,job){callbacks.push(job);return true;}};
  return {engine,callbacks,groups:new Groups(engine)};
}
test('queued apply has lifecycle cancellation even when its callback is never run',async t=>{
  t.mock.timers.enable({apis:['setTimeout','setInterval']});
  const {groups,engine,callbacks}=queuedGroups();let writes=0;
  const pending=groups.applyQueued(1,engine.socket,()=>{writes++;}),rejected=assert.rejects(pending,/connection changed/);
  engine.epoch++;t.mock.timers.tick(50);await rejected;
  assert.equal(writes,0);await callbacks[0]();assert.equal(writes,0);
});
test('local apply keeps its eight-second budget after queue wait and fences late completion',async t=>{
  t.mock.timers.enable({apis:['setTimeout','setInterval']});
  const {groups,engine,callbacks}=queuedGroups();let release,writes=0,settled=false;
  const pending=groups.applyQueued(1,engine.socket,async valid=>{await new Promise(resolve=>{release=resolve;});if(valid())writes++;});
  const rejected=assert.rejects(pending,/timed out/).then(()=>{settled=true;});
  t.mock.timers.tick(9000);await drain();assert.equal(settled,false);
  let queueAdvanced=false;const running=callbacks[0](),following=running.then(()=>{queueAdvanced=true;});
  await drain();t.mock.timers.tick(7999);await drain();assert.equal(settled,false);
  t.mock.timers.tick(1);await rejected;assert.equal(queueAdvanced,false);release();await following;assert.equal(writes,0);
});
test('group discovery network query still times out at eight seconds',async t=>{
  t.mock.timers.enable({apis:['setTimeout','setInterval']});
  const {groups,engine,callbacks}=queuedGroups();groups.discoveryEpoch=1;engine.socket.groupFetchAllParticipating=()=>new Promise(()=>{});
  let settled=false;const rejected=assert.rejects(groups.discover(),/timed out/).then(()=>{settled=true;});
  t.mock.timers.tick(7999);await drain();assert.equal(settled,false);
  t.mock.timers.tick(1);await rejected;assert.equal(callbacks.length,0);
});

test('timed-out roster canonicalization retains the real inbound queue until lookup settles',async t=>{
  const f=await fixture(t);await f.engine.request('pair');const socket=f.sockets[0];let release;
  const row={id:GROUP,subject:'Slow synthetic roster',participants:[{id:'19000000001@lid'},{id:SELF}]};
  socket.groupFetchAllParticipating=async()=>{socket.ev.emit('groups.update',[row]);return {[GROUP]:row};};
  socket.signalRepository.lidMapping.getPNForLID=()=>new Promise(resolve=>{release=resolve;});
  t.mock.timers.enable({apis:['setTimeout','setInterval']});socket.ev.emit('connection.update',{connection:'open'});await drain();
  assert.equal(typeof release,'function');t.mock.timers.tick(8000);await drain();
  socket.ev.emit('chats.update',[{id:PEER,name:'After timed-out discovery'}]);await drain();
  assert.equal(await f.engine.request('chat',{chat:PEER}),null);
  release(PEER);await drain();
  assert.equal((await f.engine.request('chat',{chat:PEER})).name,'After timed-out discovery');
  assert.equal(await f.engine.request('chat',{chat:GROUP}),null);assert.equal((await f.engine.request('status')).state,'connected');
});
