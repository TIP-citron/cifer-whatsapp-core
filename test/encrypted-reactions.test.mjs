import test from 'node:test';
import assert from 'node:assert/strict';
import { createCipheriv, hkdfSync, randomUUID } from 'node:crypto';
import pino from 'pino';
import { proto } from 'baileys';
import { makeEventBuffer } from 'baileys/lib/Utils/event-buffer.js';
import { Store } from '../src/store.mjs';
import { fixture, fixtureMessage, drain, PEER, SELF } from './helpers.mjs';
const SECRET=Buffer.alloc(32,17),IV=Buffer.alloc(12,19),GROUP='120363000000001@g.us',OTHER='15550000003@s.whatsapp.net';
const original=(id='TARGET',chat=PEER,author=SELF,secret=SECRET)=>fixtureMessage(id,'Encrypted-reaction canary',{key:{remoteJid:chat,id,fromMe:author===SELF,...(chat===GROUP?{participant:author}:{})},message:{conversation:'Encrypted-reaction canary',messageContextInfo:{messageSecret:secret}}});
function sealed({id='TARGET',chat=PEER,creator=SELF,actor=PEER,modifier=actor,text='😂',at=Date.now(),secret=SECRET,networkId=randomUUID(),targetParticipant=creator}={}){
 const plain=proto.Message.ReactionMessage.encode({text,senderTimestampMs:at}).finish(),key=hkdfSync('sha256',secret,Buffer.alloc(32),Buffer.from(id+creator+modifier+'Enc Reaction'),32),cipher=createCipheriv('aes-256-gcm',key,IV);cipher.setAAD(Buffer.alloc(0));
 return fixtureMessage(networkId,'',{key:{id:networkId,remoteJid:chat,fromMe:actor===SELF,...(chat===GROUP?{participant:actor}:{})},message:{encReactionMessage:{targetMessageKey:{id,remoteJid:chat===GROUP?chat:creator===actor?chat:creator,fromMe:creator===actor,...(chat===GROUP?{participant:targetParticipant}:{})},encIv:IV,encPayload:Buffer.concat([cipher.update(plain),cipher.final(),cipher.getAuthTag()])}}});
}
const messages=async(f,chat=PEER)=>(await f.engine.request('messages',{chat})).messages;
for(const creator of [SELF,PEER])test('authenticated encrypted reaction on '+(creator===SELF?'own':'incoming')+' message changes and removes without exposing secrets',async t=>{
 const f=await fixture(t),s=await f.connect();s.ev.emit('messages.upsert',{messages:[original('TARGET',PEER,creator)]});await drain();const at=Date.now(),before=(await messages(f))[0],status=await f.engine.request('status');
 for(const [i,text]of ['😂','❤️',''].entries()){s.ev.emit('messages.upsert',{messages:[sealed({creator,text,at:at+i})]});await drain();assert.deepEqual((await messages(f))[0].reactions,text?[{emoji:text,count:1,mine:false}]:[]);}
 assert.equal((await messages(f))[0].version,before.version);assert.equal((await f.engine.request('status')).unreadTotal,status.unreadTotal);assert.equal(JSON.stringify(await messages(f)).includes(SECRET.toString('hex')),false);assert.equal(s.sent.length,0);
});
test('encrypted reactions received before the original survive restart and retain authenticated timestamp order',async t=>{
 const f=await fixture(t),s=await f.connect(),at=Date.now();s.ev.emit('messages.upsert',{messages:[sealed({at,text:'❤️',networkId:'Z'}),sealed({at:at+1,text:'😂',networkId:'A'})]});await drain();await f.reopen();const next=f.sockets.at(-1);next.ev.emit('connection.update',{connection:'open'});next.ev.emit('messaging-history.set',{messages:[original()],contacts:[],chats:[]});await drain();assert.deepEqual((await messages(f))[0].reactions,[{emoji:'😂',count:1,mine:false}]);
 const row=(await messages(f))[0];await f.engine.request('mutate',{operationId:randomUUID(),action:'delete-local',chat:PEER,messageId:row.id,fromMe:row.fromMe,expectedVersion:row.version});next.ev.emit('messages.upsert',{messages:[sealed()]});await drain();const store=new Store(f.directory,f.config.key,{account:f.config.account,connection:f.config.connection});try{assert.equal(store.totals('reaction-pending').count,0);assert.equal(store.totals('reaction').count,0);}finally{store.close();}
});
test('encrypted group collisions bind original author and reacting actor; tampered and cross-chat envelopes fail closed',async t=>{
 const f=await fixture(t),s=await f.connect();s.ev.emit('messages.upsert',{messages:[original('SAME',GROUP,PEER),original('SAME',GROUP,SELF)]});await drain();s.ev.emit('messages.upsert',{messages:[sealed({id:'SAME',chat:GROUP,creator:PEER,actor:OTHER}),sealed({id:'SAME',chat:GROUP,creator:SELF,actor:PEER,modifier:OTHER}),sealed({id:'SAME',chat:GROUP,creator:SELF,actor:OTHER,secret:Buffer.alloc(32,4)})]});await drain();let rows=await messages(f,GROUP);assert.deepEqual(rows.find(r=>r.participant===PEER).reactions,[{emoji:'😂',count:1,mine:false}]);assert.deepEqual(rows.find(r=>r.participant===SELF).reactions,[]);
 const wrong=sealed({id:'SAME',chat:GROUP,creator:SELF,actor:OTHER});wrong.key.remoteJid='120363000000002@g.us';s.ev.emit('messages.upsert',{messages:[wrong]});await drain();assert.deepEqual((await messages(f,GROUP)).find(r=>r.participant===SELF).reactions,[]);
});
test('encrypted PN/LID reactions use authenticated aliases and fence a delayed resolver on lock',async t=>{
 const f=await fixture(t),s=await f.connect(),lid='19998887777@lid';s.signalRepository.lidMapping={getPNForLID:async id=>id===lid?PEER:null,getLIDForPN:async id=>id===PEER?lid:null};s.ev.emit('messages.upsert',{messages:[original()]});await drain();s.ev.emit('messages.upsert',{messages:[sealed({modifier:lid})]});await drain();assert.equal((await messages(f))[0].reactions[0].emoji,'😂');
 let resume;s.signalRepository.lidMapping.getLIDForPN=()=>new Promise(resolve=>{resume=resolve;});s.ev.emit('messages.upsert',{messages:[sealed({text:'❤️',at:Date.now()+1})]});await drain();s.ev.emit('connection.update',{connection:'close'});resume(lid);await drain();assert.equal((await messages(f))[0].reactions[0].emoji,'😂');
});
test('requested history snapshots bypass lifetime deduplication but keep participant keys and newest reactions',async t=>{
 const f=await fixture(t),s=await f.connect(),ev=makeEventBuffer(pino({level:'silent'}));t.after(()=>ev.destroy());ev.on('messaging-history.set',e=>s.ev.emit('messaging-history.set',e));const base=original(),at=Date.now();
 for(const [i,text]of ['','❤️','😂'].entries()){ev.buffer();ev.emit('messaging-history.set',{messages:[{...base,reactions:text?[{key:{id:'RX'+i,remoteJid:PEER,fromMe:false},text,senderTimestampMs:at+i}]:[]}],chats:[],contacts:[],syncType:6,peerDataRequestSessionId:'REQUEST'+i});ev.flush();await drain();}
 assert.deepEqual((await messages(f))[0].reactions,[{emoji:'😂',count:1,mine:false}]);
});
test('buffered latest-message recovery without requestId stays quiet',async t=>{
 const f=await fixture(t),s=await f.connect(),ev=makeEventBuffer(pino({level:'silent'}));t.after(()=>ev.destroy());s.requestPlaceholderResend=async()=>{};ev.on('messages.upsert',e=>s.ev.emit('messages.upsert',e));s.ev.emit('messages.upsert',{messages:[fixtureMessage('RECOVER','',{message:{futureBusinessMessage:{}}})]});await drain();await f.engine.request('history',{chat:PEER,recent:true});const before=(await f.engine.request('status')).unreadTotal;
 ev.buffer();ev.emit('messages.upsert',{type:'notify',requestId:'PDO',messages:[fixtureMessage('RECOVER','',{message:{templateMessage:{hydratedTemplate:{hydratedContentText:'Recovered synthetic business message'}}}})]});ev.flush();await drain();assert.equal((await messages(f)).length,1);assert.equal((await f.engine.request('status')).unreadTotal,before);
});
