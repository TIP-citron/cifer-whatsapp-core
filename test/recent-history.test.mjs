import test from 'node:test';
import assert from 'node:assert/strict';
import { fixture, fixtureMessage, drain, PEER, SELF } from './helpers.mjs';
import { WEEK } from '../src/content.mjs';
const own=fixtureMessage('OWN','Synthetic own message',{key:{remoteJid:PEER,id:'OWN',fromMe:true},messageTimestamp:Math.floor(Date.now()/1000)-30});
const reaction={key:{remoteJid:PEER,id:'REACTION',fromMe:false},text:'😂',senderTimestampMs:Date.now()};
test('recent refresh restores pre-upgrade reactions and the excluded latest business card without new alerts',async t=>{
 const f=await fixture(t),s=await f.connect(),resends=[];s.requestPlaceholderResend=async key=>{resends.push(key);return 'RESEND';};
 s.ev.emit('messages.upsert',{messages:[own,fixtureMessage('MISSING','',{message:{futureBusinessMessage:{}}})]});await drain();const unread=(await f.engine.request('status')).unreadTotal;
 await Promise.all([f.engine.request('history',{chat:PEER,recent:true}),f.engine.request('history',{chat:PEER,recent:true})]);assert.equal(s.history.length,1);assert.equal(s.history[0].count,50);assert.equal(s.history[0].key.id,'OWN');assert.deepEqual(resends.map(key=>key.id),['OWN','MISSING']);
 s.ev.emit('messaging-history.set',{peerDataRequestSessionId:s.history[0].id,messages:[{...own,reactions:[reaction]}],contacts:[],chats:[],progress:100});
 s.ev.emit('messages.upsert',{requestId:'RESEND',type:'notify',messages:[fixtureMessage('MISSING','',{message:{templateMessage:{hydratedTemplate:{hydratedContentText:'Synthetic recovered business card'}}}})]});await drain();
 const messages=(await f.engine.request('messages',{chat:PEER})).messages;assert.deepEqual(messages.find(m=>m.id==='OWN').reactions,[{emoji:'😂',count:1,mine:false}]);assert.equal(messages.find(m=>m.id==='MISSING').text,'Synthetic recovered business card');assert.equal((await f.engine.request('status')).unreadTotal,unread);assert.equal(s.sent.length,0);
 assert.equal((await f.engine.request('history',{chat:PEER,recent:true})).state,'ready');assert.equal(s.history.length,1);
});
test('latest own message resend restores its reactions without replacing the body or draft protocol',async t=>{
 const f=await fixture(t),s=await f.connect();let key;s.requestPlaceholderResend=async value=>{key=value;};s.ev.emit('messages.upsert',{messages:[own]});await drain();await f.engine.request('history',{chat:PEER,recent:true});assert.equal(key.id,'OWN');assert.equal(key.fromMe,true);
 s.ev.emit('messages.upsert',{type:'notify',requestId:'RESEND',messages:[{...own,reactions:[reaction]}]});await drain();const row=(await f.engine.request('messages',{chat:PEER})).messages[0];assert.equal(row.text,own.message.conversation);assert.deepEqual(row.reactions,[{emoji:'😂',count:1,mine:false}]);
});
test('expired anchors and invalid combinations cannot fetch; restart fences delayed history requests',async t=>{
 const f=await fixture(t),s=await f.connect();await assert.rejects(f.engine.request('history',{chat:PEER,recent:true,before:'invalid'}));await assert.rejects(f.engine.request('history',{chat:PEER,recent:'true'}));
 s.ev.emit('messages.upsert',{messages:[fixtureMessage('OLD','old',{messageTimestamp:Math.floor((Date.now()-WEEK-1000)/1000)})]});await drain();assert.equal((await f.engine.request('history',{chat:PEER,recent:true})).hasMore,false);assert.equal(s.history.length,0);
 s.ev.emit('messages.upsert',{messages:[own]});await drain();let release;s.fetchMessageHistory=()=>new Promise(resolve=>{release=resolve;});const pending=f.engine.request('history',{chat:PEER,recent:true});const rejected=assert.rejects(pending,/connection changed/);await drain();s.ev.emit('connection.update',{connection:'close'});release('LATE');await rejected;await f.reopen();const next=f.sockets.at(-1);next.ev.emit('connection.update',{connection:'open'});await f.engine.request('history',{chat:PEER,recent:true});assert.equal(next.history.length,1);
});

test('phone recovery uses the visible message when newer reaction metadata exists',async t=>{
 const f=await fixture(t),s=await f.connect(),at=Math.floor(Date.now()/1000)-10,resends=[];s.requestPlaceholderResend=async key=>{resends.push(key);return 'RESEND-'+key.id;};
 s.ev.emit('messages.upsert',{messages:[own,fixtureMessage('LATER-REACTION','',{messageTimestamp:at,message:{reactionMessage:{key:own.key,text:'😂',senderTimestampMs:Date.now()}}})]});await drain();
 await f.engine.request('history',{chat:PEER,recent:true});assert.equal(s.history[0].key.id,'OWN');assert.equal(s.history[0].at,Number(own.messageTimestamp));assert.deepEqual(resends.map(key=>key.id),['OWN','LATER-REACTION']);
 s.ev.emit('messages.upsert',{requestId:'RESEND-OWN',type:'notify',messages:[{...own,reactions:[reaction]}]});await drain();assert.deepEqual((await f.engine.request('messages',{chat:PEER})).messages.find(row=>row.id==='OWN').reactions,[{emoji:'😂',count:1,mine:false}]);
 await f.engine.request('history',{chat:PEER});assert.equal(s.history[1].key.id,'OWN');assert.equal(s.history[1].at,Number(own.messageTimestamp));
});
