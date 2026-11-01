import test from 'node:test';import assert from 'node:assert/strict';import { proto } from 'baileys';
import { fixture,fixtureMessage,drain,PEER } from './helpers.mjs';
test('content-free recovery observations exclude message text, identifiers and secrets and clear on pause',async t=>{
 const observed=[],f=await fixture(t,{onDiagnostic:row=>observed.push(row)}),s=await f.connect();s.requestPlaceholderResend=async()=> 'PRIVATE-RESEND-ID';
 const original=fixtureMessage('PRIVATE-MESSAGE-ID','PRIVATE-TEXT',{key:{id:'PRIVATE-MESSAGE-ID',remoteJid:PEER,fromMe:true}});s.ev.emit('messages.upsert',{messages:[original]});await drain();await f.engine.request('history',{chat:PEER,recent:true});await drain();
 s.ev.emit('messages.upsert',{messages:[fixtureMessage('PRIVATE-PDO-ID','',{key:{id:'PRIVATE-PDO-ID',remoteJid:PEER,fromMe:true},message:{protocolMessage:{type:17,peerDataOperationRequestResponseMessage:{stanzaId:'PRIVATE-RESEND-ID',peerDataOperationResult:[{placeholderMessageResendResponse:{webMessageInfoBytes:proto.WebMessageInfo.encode({...original,reactions:[{key:{id:'PRIVATE-RX',remoteJid:PEER,fromMe:false},text:'😂',senderTimestampMs:Date.now()}],messageSecret:Buffer.alloc(32,7)}).finish()}}]}}}})]});await drain();
 const result=observed.at(-1);assert.equal(result.traces[0].pdoAnchorReactions,1);assert.equal(result.traces[0].pdoReplies,1);const json=JSON.stringify(observed);for(const forbidden of ['PRIVATE-',PEER,'😂','07070707'])assert.equal(json.includes(forbidden),false);
 s.ev.emit('connection.update',{connection:'close'});await drain();assert.deepEqual(observed.at(-1).traces,[]);
});
