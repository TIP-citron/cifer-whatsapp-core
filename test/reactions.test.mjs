import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import pino from 'pino';
import { makeEventBuffer } from 'baileys/lib/Utils/event-buffer.js';
import processMessage from 'baileys/lib/Utils/process-message.js';
import { Store } from '../src/store.mjs';
import { validReaction } from '../src/reactions.mjs';
import { messageId, WEEK } from '../src/content.mjs';
import { fixture, fixtureMessage, drain, PEER, SELF, wire } from './helpers.mjs';
const GROUP='120363000000001@g.us',OTHER='15550000003@s.whatsapp.net',LID='19998887777@lid';
const target=(id='TARGET',chat=PEER,participant)=>({id,remoteJid:chat,fromMe:participant===SELF,...(participant?{participant}:{})});
const base=(id='TARGET',chat=PEER,participant)=>fixtureMessage(id,'Reaction canary',{key:target(id,chat,participant)});
const event=(key,text='👍',actor=PEER,at=Date.now(),id=randomUUID())=>({key,reaction:{key:{id,remoteJid:key.remoteJid,fromMe:actor===SELF,...(key.remoteJid===GROUP?{participant:actor}:{})},text,senderTimestampMs:at}});
const rows=async(f,chat=PEER)=>(await f.engine.request('messages',{chat})).messages;
const args=(row,text='👍')=>({operationId:randomUUID(),action:'react',chat:row.chat,messageId:row.id,fromMe:row.fromMe,...(row.participant?{participant:row.participant}:{}),expectedVersion:row.version,text});
const emit=async(socket,events)=>{socket.ev.emit('messages.reaction',events);await drain();};
async function setup(t,group=false){const f=await fixture(t);await f.engine.request('pair');const socket=f.sockets[0],metadata={id:GROUP,subject:'Synthetic group',announce:false,participants:[{id:SELF},{id:PEER,admin:'admin'},{id:OTHER}]};if(group){socket.groupFetchAllParticipating=async()=>({[GROUP]:metadata});socket.groupMetadata=async()=>metadata;}socket.ev.emit('connection.update',{connection:'open'});await drain();return {f,socket,metadata};}

test('only one emoji sequence or empty removal is accepted',()=>{
 for(const value of ['','👍','👍🏽','❤','❤️','👩🏽‍💻','🇫🇷','1️⃣','🏳️‍🌈'])assert.equal(validReaction(value),true,value);
 for(const value of ['text','👍👍','👍 text',' ',null,{},'1','x'.repeat(65)])assert.equal(validReaction(value),false);
});
test('incoming add/change/remove is ordered, encrypted and survives restart without unread/content changes',async t=>{
 const {f,socket}=await setup(t);socket.ev.emit('messages.upsert',{messages:[base()]});await drain();const before=(await rows(f))[0],chatBefore=await f.engine.request('chat',{chat:PEER});const at=Date.now();
 await emit(socket,[event(target(),'👍',PEER,at)]);let after=(await rows(f))[0];assert.deepEqual(after.reactions,[{emoji:'👍',count:1,mine:false}]);assert.equal(after.version,before.version);assert.equal(after.createdAt,before.createdAt);assert.deepEqual(await f.engine.request('chat',{chat:PEER}),chatBefore);
 await emit(socket,[event(target(),'❤️',PEER,at+1)]);await emit(socket,[event(target(),'👍',PEER,at)]);assert.equal((await rows(f))[0].reactions[0].emoji,'❤️');
 await emit(socket,[event(target(),'',PEER,at+2)]);await f.reopen();await emit(f.sockets.at(-1),[event(target(),'👍',PEER,at)]);assert.deepEqual((await rows(f))[0].reactions,[]);
 const store=new Store(f.directory,f.config.key,{account:f.config.account,connection:f.config.connection});try{assert.equal(store.totals('reaction').count,1);assert.equal(store.list('reaction')[0].value.text,'');}finally{store.close();}
});
for(const history of [false,true])test(`Baileys buffered ${history?'history':'upsert'} reactions and raw envelopes reach the correct message`,async t=>{
 const {f,socket}=await setup(t);const ev=makeEventBuffer(pino({level:'silent'}));for(const name of ['messages.upsert','messages.reaction','messaging-history.set'])ev.on(name,data=>socket.ev.emit(name,data));
 ev.buffer();const original=base();ev.emit(history?'messaging-history.set':'messages.upsert',history?{messages:[original],contacts:[],chats:[]}:{messages:[original],type:'notify'});
 const change=fixtureMessage('ENVELOPE','',{message:{reactionMessage:{key:original.key,text:'🎉',senderTimestampMs:Date.now()}}});
 ev.emit('messages.upsert',{messages:[change],type:'notify'});await processMessage(change,{ev,creds:{me:{id:SELF}}});ev.flush();await drain();
 assert.equal((await rows(f)).length,1);assert.deepEqual((await rows(f))[0].reactions,[{emoji:'🎉',count:1,mine:false}]);
});
test('participant-aware Baileys buffering keeps identical group IDs and their reactions separate',async t=>{
 const {f,socket}=await setup(t,true),ev=makeEventBuffer(pino({level:'silent'}));for(const name of ['messages.upsert','messages.reaction'])ev.on(name,data=>socket.ev.emit(name,data));ev.buffer();
 ev.emit('messages.upsert',{messages:[base('SAME',GROUP,PEER),base('SAME',GROUP,OTHER)],type:'notify'});
 ev.emit('messages.reaction',[event(target('SAME',GROUP,PEER),'👍',OTHER),event(target('SAME',GROUP,OTHER),'❤️',PEER)]);ev.flush();await drain();
 const messages=await rows(f,GROUP);assert.equal(messages.length,2);assert.equal(messages.find(m=>m.participant===PEER).reactions[0].emoji,'👍');assert.equal(messages.find(m=>m.participant===OTHER).reactions[0].emoji,'❤️');
});
test('PN/LID actor and original-sender aliases merge without duplicate counts',async t=>{
 const {f,socket}=await setup(t,true);socket.ev.emit('messages.upsert',{messages:[base('ALIAS',GROUP,LID)]});await drain();const at=Date.now();
 await emit(socket,[event(target('ALIAS',GROUP,LID),'👍',LID,at),event(target('ALIAS',GROUP,LID),'❤️',PEER,at+1)]);
 socket.ev.emit('contacts.upsert',[{id:LID,phoneNumber:PEER}]);await drain();const messages=await rows(f,GROUP);assert.equal(messages.length,1);assert.equal(messages[0].participant,PEER);assert.deepEqual(messages[0].reactions,[{emoji:'❤️',count:1,mine:false}]);
 await emit(socket,[event(target('ALIAS',GROUP,PEER),'🔥',LID,at+2)]);assert.deepEqual((await rows(f,GROUP))[0].reactions,[{emoji:'🔥',count:1,mine:false}]);
});
test('reactions before the original replay after restart, but deletion and expiry erase them',async t=>{
 const {f,socket}=await setup(t);await emit(socket,[event(target(),'👍')]);await f.reopen();let next=f.sockets.at(-1);next.ev.emit('connection.update',{connection:'open'});next.ev.emit('messages.upsert',{messages:[base()]});await drain();assert.equal((await rows(f))[0].reactions.length,1);
 await f.engine.request('mutate',{...args((await rows(f))[0]),action:'delete-local',text:undefined});await emit(next,[event(target(),'❤️')]);assert.equal((await rows(f)).length,0);
 next.ev.emit('messages.upsert',{messages:[fixtureMessage('EXPIRE','',{messageTimestamp:Math.floor(Date.now()/1000)-1,message:{extendedTextMessage:{text:'Ephemeral',contextInfo:{expiration:3}}}})]});await drain();await emit(next,[event(target('EXPIRE'),'👍')]);
 const real=Date.now,now=real();Date.now=()=>now+4000;try{await f.reopen();assert.equal((await rows(f)).length,0);}finally{Date.now=real;}
 const store=new Store(f.directory,f.config.key,{account:f.config.account,connection:f.config.connection});try{assert.equal(store.totals('reaction').count,0);}finally{store.close();}
});
test('outgoing add/change/remove uses the original key, persists and never resends an operation',async t=>{
 const {f,socket}=await setup(t);socket.ev.emit('messages.upsert',{messages:[base()]});await drain();let row=(await rows(f))[0];
 for(const emoji of ['👍','❤️','']){const input=args(row,emoji);assert.equal((await f.engine.request('prepareMutation',input)).state,'prepared');assert.equal((await f.engine.request('mutate',input)).state,'sent');const count=socket.sent.length;await f.engine.request('mutate',input);assert.equal(socket.sent.length,count);assert.deepEqual(socket.sent.at(-1).content.react.key,target());assert.equal(socket.sent.at(-1).content.react.text,emoji);row=(await rows(f))[0];assert.deepEqual(row.reactions,emoji?[{emoji,count:1,mine:true}]:[]);}
 assert.equal(socket.sent.length,3);await f.reopen();assert.deepEqual((await rows(f))[0].reactions,[]);
});
test('ambiguous reaction stays uncertain without replay until its own authenticated echo confirms it',async t=>{
 const {f,socket}=await setup(t);socket.ev.emit('messages.upsert',{messages:[base()]});await drain();let sent;socket.sendMessage=async(peer,content,options)=>{sent={content,options};throw Error('network interrupted');};const input=args((await rows(f))[0]);
 assert.equal((await f.engine.request('mutate',input)).state,'uncertain');assert.deepEqual((await rows(f))[0].reactions,[]);await f.reopen();const next=f.sockets.at(-1);next.ev.emit('connection.update',{connection:'open'});assert.equal((await f.engine.request('mutate',input)).state,'uncertain');assert.equal(next.sent.length,0);
 await emit(next,[event(target(),'👍',PEER,sent.content.react.senderTimestampMs)]);assert.equal((await f.engine.request('mutations',{chat:PEER})).mutations.length,1);
 await emit(next,[event(target(),'👍',SELF,sent.content.react.senderTimestampMs,sent.options.messageId)]);assert.equal((await f.engine.request('mutate',input)).state,'sent');assert.equal(next.sent.length,0);
});
test('announcement-group members may react; removed members cannot send prepared reactions',async t=>{
 const {f,socket,metadata}=await setup(t,true);metadata.announce=true;socket.ev.emit('messages.upsert',{messages:[base('GROUP',GROUP,PEER)]});await drain();let row=(await rows(f,GROUP))[0];assert.equal((await f.engine.request('mutate',args(row))).state,'sent');assert.deepEqual(socket.sent[0].content.react.key,target('GROUP',GROUP,PEER));
 const input=args(row,'❤️');await f.engine.request('prepareMutation',input);metadata.participants=metadata.participants.filter(p=>p.id!==SELF);await assert.rejects(f.engine.request('mutate',input),/read-only/);assert.equal(socket.sent.length,1);
});
test('protected and unsupported messages can receive reactions without changing their content',async t=>{
 const {f,socket}=await setup(t);const protectedText=wire();socket.ev.emit('messages.upsert',{messages:[fixtureMessage('SECRET',protectedText),fixtureMessage('VIDEO','',{message:{videoMessage:{}}})]});await drain();for(const row of await rows(f))await f.engine.request('mutate',args(row));const messages=await rows(f);assert.equal(messages.find(r=>r.id==='SECRET').wire,protectedText);assert.equal(messages.find(r=>r.id==='VIDEO').kind,'unsupported');assert.equal(socket.sent.every(s=>Object.keys(s.content).join()==='react'),true);
});
test('cross-chat targets, malformed emoji and stale callbacks cannot change reaction state',async t=>{
 const {f,socket}=await setup(t);socket.ev.emit('messages.upsert',{messages:[base()]});await drain();const wrong=event(target());wrong.reaction.key.remoteJid=OTHER;await emit(socket,[wrong,event(target(),'not an emoji')]);assert.deepEqual((await rows(f))[0].reactions,[]);
 await assert.rejects(f.engine.request('mutate',args((await rows(f))[0],'hello')));assert.equal(socket.sent.length,0);
 let resolve;socket.signalRepository.lidMapping.getPNForLID=()=>new Promise(r=>{resolve=r;});const stale=event(target('TARGET',LID));socket.ev.emit('messages.reaction',[stale]);await drain();socket.ev.emit('connection.update',{connection:'close'});await f.engine.request('pair');f.sockets.at(-1).ev.emit('connection.update',{connection:'open'});resolve(PEER);await drain();assert.deepEqual((await rows(f))[0].reactions,[]);
});

test('same-clock own reaction changes remain monotonic and survive a delayed earlier echo',async t=>{
 const {f,socket}=await setup(t);socket.ev.emit('messages.upsert',{messages:[base()]});await drain();const row=(await rows(f))[0],real=Date.now,now=real();Date.now=()=>now;
 try{await f.engine.request('mutate',args(row,'👍'));await f.engine.request('mutate',args(row,'❤️'));assert.ok(socket.sent[1].content.react.senderTimestampMs>socket.sent[0].content.react.senderTimestampMs);await emit(socket,[event(target(),'👍',SELF,now,socket.sent[0].options.messageId)]);assert.deepEqual((await rows(f))[0].reactions,[{emoji:'❤️',count:1,mine:true}]);}finally{Date.now=real;}
});
