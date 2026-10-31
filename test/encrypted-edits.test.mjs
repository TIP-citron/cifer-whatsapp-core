import test from 'node:test';
import assert from 'node:assert/strict';
import { createCipheriv, hkdfSync } from 'node:crypto';
import pino from 'pino';
import { proto } from 'baileys';
import { decryptMessageNode } from 'baileys/lib/Utils/decode-wa-message.js';
import { Store } from '../src/store.mjs';
import { fixture, fixtureMessage, drain, PEER, SELF } from './helpers.mjs';

const SECRET = Buffer.alloc(32, 7), IV = Buffer.alloc(12, 8), ID = 'NATIVE-ORIGINAL';
const original = (fromMe=false, extra={}) => fixtureMessage(ID, '', {key:{id:ID,remoteJid:PEER,fromMe},message:{conversation:'native original',messageContextInfo:{messageSecret:SECRET}},...extra});
function encryptedEdit({fromMe=false, author=fromMe?SELF:PEER, editor=author, secret=SECRET, chat=PEER, id=ID, embeddedId=id, timestampMs=Date.now(), text='native correction', extra={}}={}) {
  const key = {id:embeddedId,remoteJid:fromMe?PEER:SELF,fromMe:true};
  const plain=proto.Message.encode({protocolMessage:{type:14,key,editedMessage:{extendedTextMessage:{text}},timestampMs}}).finish();
  // Independent standard HKDF/AES-GCM fixture, matching the native client wire format.
  const info=Buffer.from(id+author+editor+'Message Edit'),derived=hkdfSync('sha256',secret,Buffer.alloc(32),info,32);
  const cipher=createCipheriv('aes-256-gcm',derived,IV);cipher.setAAD(Buffer.alloc(0));
  const encPayload=Buffer.concat([cipher.update(plain),cipher.final(),cipher.getAuthTag()]);
  return fixtureMessage('NATIVE-EDIT-ENVELOPE','',{key:{id:'NATIVE-EDIT-ENVELOPE',remoteJid:chat,fromMe},message:proto.Message.decode(proto.Message.encode({secretEncryptedMessage:{secretEncType:2,targetMessageKey:{...key,id},encIv:IV,encPayload,...extra}}).finish())});
}
const rows=async(f,chat=PEER)=>(await f.engine.request('messages',{chat})).messages;
for(const fromMe of [false,true]) test(`native encrypted edit updates ${fromMe?'own linked-device':'received'} message after restart without exposing its secret`,async t=>{
  const f=await fixture(t),socket=await f.connect();socket.ev.emit('messages.upsert',{messages:[original(fromMe)]});await drain();
  assert.equal(JSON.stringify(await rows(f)).includes(SECRET.toString('hex')),false);
  await f.reopen();const next=f.sockets.at(-1);next.ev.emit('connection.update',{connection:'open'});
  next.ev.emit('messages.upsert',{messages:[encryptedEdit({fromMe})]});await drain();
  const result=await rows(f);assert.equal(result.length,1);assert.equal(result[0].text,'native correction');assert.ok(result[0].editedAt);
  assert.equal(JSON.stringify(result).includes(SECRET.toString('hex')),false);
});
test('linked-device decoder retains the original outer message secret',async()=>{
  const node={tag:'message',attrs:{from:SELF,recipient:PEER,id:ID,t:String(Math.floor(Date.now()/1000))},content:[{tag:'plaintext',attrs:{},content:proto.Message.encode({deviceSentMessage:{destinationJid:PEER,message:{conversation:'native original'}},messageContextInfo:{messageSecret:SECRET}}).finish()}]};
  const decoded=decryptMessageNode(node,SELF,undefined,{lidMapping:{getLIDForPN:async()=>null}},pino({level:'silent'}));await decoded.decrypt();
  assert.deepEqual(Buffer.from(decoded.fullMessage.message.messageContextInfo?.messageSecret||[]),SECRET);
});

test('native encrypted edit before original is durable and history restores only the new version',async t=>{
  const f=await fixture(t),socket=await f.connect();socket.ev.emit('messages.upsert',{messages:[encryptedEdit()]});await drain();
  await f.reopen();const next=f.sockets.at(-1);next.ev.emit('connection.update',{connection:'open'});
  next.ev.emit('messaging-history.set',{contacts:[],chats:[],messages:[original()]});await drain();
  assert.equal((await rows(f))[0].text,'native correction');
  next.ev.emit('messages.upsert',{messages:[original()]});await drain();assert.equal((await rows(f))[0].text,'native correction');
});
test('history can restore a missing message secret without exposing the stale body while waiting',async t=>{
  const f=await fixture(t),socket=await f.connect();socket.ev.emit('messages.upsert',{messages:[original(false,{message:{conversation:'native original'}})]});await drain();
  socket.ev.emit('messages.upsert',{messages:[encryptedEdit()]});await drain();
  assert.equal((await rows(f))[0].text,'Edited message unavailable · open in WhatsApp');
  socket.ev.emit('messaging-history.set',{contacts:[],chats:[],messages:[original(false,{message:{conversation:'native original'},messageSecret:SECRET})]});await drain();
  assert.equal((await rows(f))[0].text,'native correction');
});
test('expired encrypted pending edits cannot resurrect their original even before periodic pruning',async t=>{
  const f=await fixture(t),socket=await f.connect();socket.ev.emit('messages.upsert',{messages:[encryptedEdit()]});await drain();
  const real=Date.now,at=real();Date.now=()=>at+120001;
  try{socket.ev.emit('messages.upsert',{messages:[original()]});await drain();assert.equal((await rows(f)).length,0);}finally{Date.now=real;}
});
test('late envelopes, forged targets, wrong direction and tampering cannot alter another record',async t=>{
  const f=await fixture(t),socket=await f.connect();socket.ev.emit('messages.upsert',{messages:[original(),original(true)]});await drain();
  const other='15550000003@s.whatsapp.net';
  const bad=[encryptedEdit({embeddedId:'DIFFERENT'}),encryptedEdit({secret:Buffer.alloc(32,99)}),encryptedEdit({editor:other}),encryptedEdit({fromMe:true,author:PEER}),encryptedEdit({chat:other}),encryptedEdit({timestampMs:Date.now()+120000}),encryptedEdit({extra:{encIv:Buffer.alloc(11)}}),encryptedEdit({extra:{encPayload:Buffer.alloc(65537)}})];
  for(const message of bad){socket.ev.emit('messages.upsert',{messages:[message]});await drain();}
  assert.equal((await rows(f)).filter(r=>r.text==='native original').length,2);
  const valid=encryptedEdit();valid.message.secretEncryptedMessage.targetMessageKey.remoteJid=other;valid.message.secretEncryptedMessage.targetMessageKey.fromMe=false;
  socket.ev.emit('messages.upsert',{messages:[valid]});await drain();
  assert.equal((await rows(f)).find(r=>r.fromMe).text,'native original');assert.equal((await rows(f)).find(r=>!r.fromMe).text,'native correction');
});
test('PN/LID aliases decrypt only within the stored peer and survive alias reconciliation',async t=>{
  const f=await fixture(t),socket=await f.connect(),lid='1234567890@lid';
  socket.signalRepository.lidMapping={getPNForLID:async v=>v===lid?PEER:null,getLIDForPN:async v=>v===PEER?lid:null};
  socket.ev.emit('messages.upsert',{messages:[original()]});await drain();
  socket.ev.emit('messages.upsert',{messages:[encryptedEdit({chat:lid,author:lid})]});await drain();assert.equal((await rows(f))[0].text,'native correction');
});
test('deleting a message erases its edit secret and prevents ciphertext replay and history resurrection',async t=>{
  const f=await fixture(t),socket=await f.connect();socket.ev.emit('messages.upsert',{messages:[original()]});await drain();
  socket.ev.emit('messages.delete',{keys:[original().key]});await drain();
  socket.ev.emit('messages.upsert',{messages:[encryptedEdit(),original()]});await drain();assert.equal((await rows(f)).length,0);
  await f.engine.close();const store=new Store(f.directory,f.config.key,{account:f.config.account,connection:f.config.connection});
  try{for(const ns of ['message','encrypted-edit','tombstone','pending-edit','change','retry','operation'])assert.equal(JSON.stringify(store.list(ns,{now:0})).includes(SECRET.toString('hex')),false);}finally{store.close();}
});
test('message secrets are not available to renderer methods or retry callbacks',async t=>{
  const f=await fixture(t),socket=await f.connect();socket.ev.emit('messages.upsert',{messages:[original(),original(true)]});await drain();
  for(const data of [await rows(f),await f.engine.request('chats'),await f.engine.request('status'),f.events,await socket.config.getMessage(original(true).key)]){
    assert.equal(JSON.stringify(data).includes('editSecret'),false);assert.equal(JSON.stringify(data).includes(SECRET.toString('hex')),false);
  }
  assert.equal(await socket.config.getMessage(original().key),undefined);
});
test('disappearing expiry removes edit secrets and rejects delayed mutation replay',async t=>{
  const f=await fixture(t),socket=await f.connect(),at=Math.floor(Date.now()/1000)*1000;
  socket.ev.emit('messages.upsert',{messages:[original(false,{messageTimestamp:at/1000,message:{extendedTextMessage:{text:'short lived',contextInfo:{expiration:2}},messageContextInfo:{messageSecret:SECRET}}})]});await drain();
  const real=Date.now;Date.now=()=>at+3000;
  try{await f.reopen();const next=f.sockets.at(-1);next.ev.emit('connection.update',{connection:'open'});next.ev.emit('messages.upsert',{messages:[encryptedEdit()]});await drain();assert.equal((await rows(f)).length,0);}finally{Date.now=real;}
});
test('delayed mapping lookup after pause cannot apply an encrypted edit',async t=>{
  const f=await fixture(t),socket=await f.connect();socket.ev.emit('messages.upsert',{messages:[original()]});await drain();let finish;
  socket.signalRepository.lidMapping.getLIDForPN=()=>new Promise(resolve=>{finish=resolve;});
  socket.ev.emit('messages.upsert',{messages:[encryptedEdit()]});await drain();assert.equal(typeof finish,'function');
  socket.ev.emit('connection.update',{connection:'close'});finish(null);await drain();assert.equal((await rows(f))[0].text,'native original');
});
test('two account stores with the same chat and message IDs never share edit secrets',async t=>{
  const a=await fixture(t),b=await fixture(t,{account:'10000000-0000-4000-8000-000000000002'}),sa=await a.connect(),sb=await b.connect();
  sa.ev.emit('messages.upsert',{messages:[original()]});sb.ev.emit('messages.upsert',{messages:[original(false,{message:{conversation:'B original',messageContextInfo:{messageSecret:Buffer.alloc(32,6)}}})]});await drain();
  const edit=encryptedEdit();sa.ev.emit('messages.upsert',{messages:[edit]});sb.ev.emit('messages.upsert',{messages:[edit]});await drain();
  assert.equal((await rows(a))[0].text,'native correction');assert.equal((await rows(b))[0].text,'B original');
});

test('an original without its secret arriving after a queued edit never exposes its stale text',async t=>{
  const f=await fixture(t),socket=await f.connect();socket.ev.emit('messages.upsert',{messages:[encryptedEdit()]});await drain();
  socket.ev.emit('messages.upsert',{messages:[original(false,{message:{conversation:'stale native original'}})]});await drain();
  assert.equal((await rows(f))[0].text,'Edited message unavailable · open in WhatsApp');
  socket.ev.emit('messages.upsert',{messages:[original()]});await drain();assert.equal((await rows(f))[0].text,'native correction');
});
test('late identity reconciliation applies the queued encrypted edit to its matching PN original',async t=>{
  const f=await fixture(t),socket=await f.connect(),lid='1234567890@lid';
  socket.ev.emit('messages.upsert',{messages:[encryptedEdit({chat:lid,author:lid}),original()]});await drain();assert.equal((await rows(f))[0].text,'native original');
  socket.ev.emit('contacts.upsert',[{id:lid,phoneNumber:PEER}]);await drain();assert.equal((await rows(f))[0].text,'native correction');
});
test('linked-device decoder prefers an inner secret when both contexts are present',async()=>{
  const inner=Buffer.alloc(32,11),node={tag:'message',attrs:{from:SELF,recipient:PEER,id:ID,t:String(Math.floor(Date.now()/1000))},content:[{tag:'plaintext',attrs:{},content:proto.Message.encode({deviceSentMessage:{destinationJid:PEER,message:{conversation:'native original',messageContextInfo:{messageSecret:inner}}},messageContextInfo:{messageSecret:SECRET}}).finish()}]};
  const decoded=decryptMessageNode(node,SELF,undefined,{lidMapping:{getLIDForPN:async()=>null}},pino({level:'silent'}));await decoded.decrypt();
  assert.deepEqual(Buffer.from(decoded.fullMessage.message.messageContextInfo.messageSecret),inner);
});
