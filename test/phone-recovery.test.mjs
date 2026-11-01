import test from 'node:test';
import assert from 'node:assert/strict';
import { proto } from 'baileys';
import pino from 'pino';
import processMessage from 'baileys/lib/Utils/process-message.js';
import { makeEventBuffer } from 'baileys/lib/Utils/event-buffer.js';
import { fixture, fixtureMessage, drain, PEER, SELF } from './helpers.mjs';
const OTHER='15550000003@s.whatsapp.net',GROUP='120363000000001@g.us';
const own=()=>fixtureMessage('LAST','Latest synthetic message',{key:{id:'LAST',remoteJid:PEER,fromMe:true},messageTimestamp:Math.floor(Date.now()/1000)-20});
async function response(socket,cache,message){
 const ev=makeEventBuffer(pino({level:'silent'}));ev.on('messages.upsert',e=>socket.ev.emit('messages.upsert',e));
 try{ev.buffer();await processMessage(fixtureMessage('PDO-RESPONSE','',{key:{id:'PDO-RESPONSE',remoteJid:SELF,fromMe:true},message:{protocolMessage:{type:17,peerDataOperationRequestResponseMessage:{stanzaId:'PDO',peerDataOperationResult:[{placeholderMessageResendResponse:{webMessageInfoBytes:proto.WebMessageInfo.encode(message).finish()}}]}}}}),{ev,creds:{me:{id:SELF}},placeholderResendCache:{get:async()=>cache,del:async()=>{}},logger:pino({level:'silent'})});ev.flush();await drain();}finally{ev.destroy();}
}
test('real phone resend processing preserves compact latest-message reactions, known key/time and secret',async t=>{
 const f=await fixture(t),s=await f.connect(),original=own();let cache;
 s.requestPlaceholderResend=async(key,metadata)=>{cache=metadata;return 'PDO';};
 s.ev.emit('messages.upsert',{messages:[original]});await drain();await f.engine.request('history',{chat:PEER,recent:true});
 assert.deepEqual(cache?.key,original.key);assert.equal(cache?.messageTimestamp,original.messageTimestamp);
 await response(s,cache,{key:{id:'LAST',fromMe:true},message:original.message,messageSecret:Buffer.alloc(32,7),reactions:[{key:{id:'COMPACT-REACTION',fromMe:false},text:'😂',senderTimestampMs:Date.now()}]});
 const row=(await f.engine.request('messages',{chat:PEER})).messages[0];assert.equal(row.text,original.message.conversation);assert.deepEqual(row.reactions,[{emoji:'😂',count:1,mine:false}]);assert.equal(s.sent.length,0);
 await f.reopen();assert.deepEqual((await f.engine.request('messages',{chat:PEER})).messages[0].reactions,[{emoji:'😂',count:1,mine:false}]);
});
test('phone resend metadata cannot be borrowed by a conflicting chat or direction',async t=>{
 const f=await fixture(t),s=await f.connect(),original=own(),cache={key:original.key,messageTimestamp:original.messageTimestamp};
 for(const key of [{id:'LAST',remoteJid:OTHER,fromMe:true},{id:'LAST',remoteJid:PEER,fromMe:false}])await response(s,cache,{key,message:{conversation:'Must not borrow metadata'}});
 assert.deepEqual((await f.engine.request('messages',{chat:PEER})).messages,[]);assert.deepEqual((await f.engine.request('messages',{chat:OTHER})).messages,[]);
});
test('compact aggregate reactions inherit only the enclosing chat; groups still require an actor',async t=>{
 const f=await fixture(t),s=await f.connect(),now=Date.now();
 s.ev.emit('messaging-history.set',{chats:[],contacts:[],messages:[{...own(),reactions:[{key:{id:'GOOD',fromMe:false},text:'❤️',senderTimestampMs:now},{key:{id:'BAD',remoteJid:OTHER,fromMe:false},text:'🔥',senderTimestampMs:now}]},fixtureMessage('GROUP','Group synthetic message',{key:{remoteJid:GROUP,id:'GROUP',fromMe:false,participant:PEER},reactions:[{key:{id:'NO-ACTOR',fromMe:false},text:'😂',senderTimestampMs:now},{key:{id:'ACTOR',fromMe:false,participant:OTHER},text:'👍',senderTimestampMs:now}]})]});await drain();
 assert.deepEqual((await f.engine.request('messages',{chat:PEER})).messages[0].reactions,[{emoji:'❤️',count:1,mine:false}]);assert.deepEqual((await f.engine.request('messages',{chat:GROUP})).messages[0].reactions,[{emoji:'👍',count:1,mine:false}]);
});
