import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import processMessage from 'baileys/lib/Utils/process-message.js';
import { proto } from 'baileys';
import { fixture,fixtureMessage,drain,PEER,SELF,ACCOUNT,wire } from './helpers.mjs';
import { Store } from '../src/store.mjs';
import { sanitizeMessage,WEEK } from '../src/content.mjs';

test('pairing, QR/code, contacts, unread selection and persistent reconnect remain local',async t=>{
 const f=await fixture(t);assert.equal((await f.engine.request('status')).state,'disconnected');await f.engine.request('pair');const socket=f.sockets[0];
 socket.ev.emit('connection.update',{qr:'fixture-qr'});assert.equal((await f.engine.request('pair',{phone:'15550000002'})).pairCode,'ABCD1234');
 socket.ev.emit('connection.update',{connection:'open'});socket.ev.emit('contacts.upsert',[{id:PEER,name:'Alice'}]);socket.ev.emit('messages.upsert',{messages:[fixtureMessage('A'),fixtureMessage('B')]});await drain();
 assert.equal((await f.engine.request('status')).unreadTotal,2);assert.equal((await f.engine.request('chats')).chats[0].name,'Alice');
 await f.engine.request('read',{chat:PEER,ids:['A']});assert.equal((await f.engine.request('status')).unreadTotal,1);
 await f.reopen();assert.equal(f.sockets.length,2);f.sockets.at(-1).ev.emit('connection.update',{connection:'open'});assert.equal((await f.engine.request('messages',{chat:PEER})).messages.length,2);
 const out=await f.engine.disconnect();assert.deepEqual(out,{disconnected:true,remoteUnlinked:true});assert.equal((await f.engine.request('messages',{chat:PEER})).messages.length,0);
});
test('local seen markers refresh metadata only while deletion still invalidates message content',async t=>{
 const f=await fixture(t),socket=await f.connect();socket.ev.emit('messages.upsert',{messages:[fixtureMessage('VISIBLE')]});await drain();f.events.length=0;
 await f.engine.request('markRead',{chat:PEER,ids:['VISIBLE']});await drain();
 assert.deepEqual(f.events.map(event=>event.type),['metadata']);assert.equal((await f.engine.request('status')).unreadTotal,0);
 f.events.length=0;await f.engine.request('markRead',{chat:PEER,ids:['VISIBLE']});await drain();assert.equal(f.events.length,0);
 socket.ev.emit('messages.delete',{keys:[{remoteJid:PEER,id:'VISIBLE',fromMe:false}]});await drain();assert.ok(f.events.some(event=>event.type==='changed'));assert.equal((await f.engine.request('messages',{chat:PEER})).messages.length,0);
});
test('first phone request awaits asynchronous readiness and expired QR refresh uses a fresh socket',async t=>{
 const f=await fixture(t),first=f.engine.request('pair',{phone:'15550000002'});await drain();
 let calls=0;f.sockets[0].requestPairingCode=async phone=>{assert.equal(phone,'15550000002');calls++;return 'ABCD1234';};
 f.sockets[0].ev.emit('connection.update',{qr:'async-qr'});assert.equal((await first).pairCode,'ABCD1234');assert.equal(calls,1);
 const real=Date.now,now=real();Date.now=()=>now+180000;
 try{
   assert.equal((await f.engine.request('pair')).qr,null);assert.equal(f.sockets.length,2);assert.equal(f.sockets[0].ended,true);
   await drain();f.sockets[1].ev.emit('connection.update',{qr:'fresh-qr'});assert.equal((await f.engine.request('status')).qr,'fresh-qr');
   f.sockets[0].ev.emit('connection.update',{qr:'stale-qr'});assert.equal((await f.engine.request('status')).qr,'fresh-qr');
 }finally{Date.now=real;}
});
test('phone readiness wait is cancelled promptly by close',async t=>{
 const f=await fixture(t),pair=f.engine.request('pair',{phone:'15550000002'});await drain();
 const rejected=assert.rejects(pair,/pairing is not ready/);await f.engine.close();await rejected;
});
test('real Baileys edit/revoke event shapes update local history and block replay resurrection',async t=>{
 const f=await fixture(t),socket=await f.connect();socket.ev.emit('messages.upsert',{messages:[fixtureMessage('ORIGINAL','old')]});await drain();
 const emit=async protocolMessage=>{await processMessage(fixtureMessage('EVENT','',{message:{protocolMessage}}),{ev:socket.ev,creds:{me:{id:SELF}}});await drain();};
 await emit({type:proto.Message.ProtocolMessage.Type.MESSAGE_EDIT,key:{id:'ORIGINAL'},editedMessage:{conversation:'edited'},timestampMs:Date.now()});
 assert.equal((await f.engine.request('messages',{chat:PEER})).messages[0].text,'edited');
 await emit({type:proto.Message.ProtocolMessage.Type.REVOKE,key:{id:'ORIGINAL'}});socket.ev.emit('messages.upsert',{messages:[fixtureMessage('ORIGINAL','stale')]});await drain();
 assert.equal((await f.engine.request('messages',{chat:PEER})).messages.length,0);await f.reopen();f.sockets.at(-1).ev.emit('connection.update',{connection:'open'});f.sockets.at(-1).ev.emit('messages.upsert',{messages:[fixtureMessage('ORIGINAL','stale')]});await drain();assert.equal((await f.engine.request('messages',{chat:PEER})).messages.length,0);
});
test('view-once, old content and media bytes never enter history; expiry gates reads immediately',async t=>{
 const f=await fixture(t),socket=await f.connect(),now=Date.now();
 assert.equal(sanitizeMessage({...fixtureMessage('OLD'),messageTimestamp:Math.floor((now-WEEK)/1000),get message(){throw Error('content touched');}}),null);
 socket.ev.emit('messages.upsert',{messages:[fixtureMessage('ONCE','',{message:{viewOnceMessage:{message:{conversation:'private'}}}}),fixtureMessage('CAP','',{message:{imageMessage:{caption:'caption',url:'SHOULD_NOT_STORE',jpegThumbnail:Buffer.alloc(100),contextInfo:{expiration:2,quotedMessage:{conversation:'QUOTE'}}}}})]});await drain();
 let page=await f.engine.request('messages',{chat:PEER});assert.equal(page.messages.length,1);assert.equal(page.messages[0].text,'caption');assert.ok(!JSON.stringify(page).includes('SHOULD_NOT_STORE'));assert.ok(!JSON.stringify(page).includes('QUOTE'));
 const real=Date.now;Date.now=()=>now+4000;try{assert.equal((await f.engine.request('messages',{chat:PEER})).messages.length,0);assert.equal((await f.engine.request('status')).unreadTotal,0);}finally{Date.now=real;}
});
test('sends are durable/idempotent, preserve disappearance setting, and reconcile receipts',async t=>{
 const f=await fixture(t),socket=await f.connect();socket.ev.emit('chats.update',[{id:PEER,ephemeralExpiration:86400}]);await drain();
 const args={operationId:randomUUID(),chat:PEER,text:'hello',kind:'ordinary'};
 const first=await f.engine.request('send',args);await f.engine.request('send',args);assert.equal(socket.sent.length,1);assert.equal(socket.sent[0].options.ephemeralExpiration,86400);
 await assert.rejects(f.engine.request('send',{...args,text:'different'}));socket.ev.emit('messages.update',[{key:{remoteJid:PEER,id:first.messageId,fromMe:true},update:{status:4}}]);await drain();assert.equal((await f.engine.request('operation',{id:args.operationId})).state,'read');
 const uncertain={operationId:randomUUID(),chat:PEER,text:'network fails',kind:'ordinary'};socket.sendMessage=async()=>{throw Error('synthetic failure');};assert.equal((await f.engine.request('send',uncertain)).state,'uncertain');await f.reopen();f.sockets.at(-1).ev.emit('connection.update',{connection:'open'});assert.equal((await f.engine.request('send',uncertain)).state,'uncertain');assert.equal(f.sockets.at(-1).sent.length,0);
});
test('shortened expiration and deleted prepared operations cancel before any network send',async t=>{
 const f=await fixture(t),socket=await f.connect(),args={operationId:randomUUID(),chat:PEER,text:'unsent private text',kind:'ordinary'};
 await f.engine.request('prepareSend',args);const real=Date.now,now=real();Date.now=()=>now+10000;
 try{
   socket.ev.emit('chats.update',[{id:PEER,ephemeralExpiration:1}]);await drain();
   assert.equal((await f.engine.request('send',args)).state,'cancelled');assert.equal((await f.engine.request('operation',{id:args.operationId})).state,'cancelled');assert.equal(socket.sent.length,0);
   assert.equal((await f.engine.request('messages',{chat:PEER})).messages.length,0);
 }finally{Date.now=real;}
 const deleted={...args,operationId:randomUUID()},pending=await f.engine.request('prepareSend',deleted);
 socket.ev.emit('messages.delete',{keys:[{id:pending.messageId,remoteJid:PEER,fromMe:true}]});await drain();
 assert.equal((await f.engine.request('send',deleted)).state,'cancelled');assert.equal(socket.sent.length,0);
});
test('ordinary send limit is enforced in UTF-8 bytes at the engine boundary',async t=>{
 const f=await fixture(t),socket=await f.connect(),args={operationId:randomUUID(),chat:PEER,text:'é'.repeat(1000),kind:'ordinary'};
 await f.engine.request('send',args);assert.equal(socket.sent.length,1);
 await assert.rejects(f.engine.request('prepareSend',{...args,operationId:randomUUID(),text:args.text+'a'}),/ordinary message/);assert.equal(socket.sent.length,1);
});
test('PN/LID mapping merges chats and deletion tombstones follow the alias',async t=>{
 const f=await fixture(t),socket=await f.connect(),lid='1234567890@lid';socket.ev.emit('messages.upsert',{messages:[fixtureMessage('ALIAS','text',{key:{id:'ALIAS',remoteJid:lid,fromMe:false}})]});await drain();
 socket.ev.emit('contacts.upsert',[{id:lid,phoneNumber:PEER,name:'Alice'}]);await drain();assert.equal((await f.engine.request('messages',{chat:PEER})).messages.length,1);
 socket.ev.emit('messages.delete',{keys:[{remoteJid:lid,id:'ALIAS',fromMe:false}]});await drain();socket.ev.emit('messages.upsert',{messages:[fixtureMessage('ALIAS')]});await drain();assert.equal((await f.engine.request('messages',{chat:PEER})).messages.length,0);
});
test('history pages use bounded phone fetches and omit new-message unread state',async t=>{
 const f=await fixture(t),socket=await f.connect();socket.ev.emit('messages.upsert',{messages:[fixtureMessage('LATEST')]});await drain();const first=await f.engine.request('history',{chat:PEER});assert.equal(first.state,'loading');assert.equal(socket.history[0].count,20);
 socket.ev.emit('messaging-history.set',{peerDataRequestSessionId:socket.history[0].id,messages:[fixtureMessage('OLDER','old',{messageTimestamp:Math.floor(Date.now()/1000)-60})],contacts:[],chats:[]});await drain();assert.equal((await f.engine.request('history',{chat:PEER})).state,'ready');assert.equal((await f.engine.request('status')).unreadTotal,1);
});
test('prepared protected operations persist exact ciphertext metadata; no implicit network send',async t=>{
 const f=await fixture(t),socket=await f.connect(),text=wire(),owner={principal:ACCOUNT,secretId:'101',publicKey:Buffer.alloc(1184).toString('base64')};
 const prepared={version:1,principal:ACCOUNT,chat:PEER,owner,binding:null,wire:text,recovery:{encrypted:'11'.repeat(128),cifer:'22'.repeat(1104)},recipientGrant:null};
 const args={operationId:randomUUID(),chat:PEER,text,kind:'protected',prepared};
 assert.equal((await f.engine.request('prepareSend',args)).state,'prepared');assert.equal(socket.sent.length,0);await f.reopen();assert.deepEqual((await f.engine.request('operation',{id:args.operationId})).prepared,prepared);f.sockets.at(-1).ev.emit('connection.update',{connection:'open'});await f.engine.request('send',args);assert.equal(f.sockets.at(-1).sent.length,1);
 await assert.rejects(f.engine.request('prepareSend',{...args,operationId:randomUUID(),prepared:{...prepared,key:'plaintext key'}}));
});
test('close fences late callbacks and account mismatch does not open another local database',async t=>{
 const f=await fixture(t),socket=await f.connect();await f.engine.close();socket.ev.emit('messages.upsert',{messages:[fixtureMessage('LATE')]});socket.ev.emit('creds.update',{});await drain();
 assert.throws(()=>new Store(f.directory,f.config.key,{account:'different',connection:f.config.connection}));
 await f.reopen();f.sockets.at(-1).ev.emit('connection.update',{connection:'open'});assert.equal((await f.engine.request('messages',{chat:PEER})).messages.length,0);
});
test('edits erase original recoverable outgoing bodies and expiry never permits an old operation resend',async t=>{
 const f=await fixture(t),socket=await f.connect(),args={operationId:randomUUID(),chat:PEER,text:'original private text',kind:'ordinary'};
 const sent=await f.engine.request('send',args);socket.ev.emit('messages.update',[{key:{id:sent.messageId,remoteJid:PEER,fromMe:true},update:{message:{editedMessage:{message:{conversation:'replacement'}}},messageTimestamp:Math.floor(Date.now()/1000)}}]);await drain();
 const op=await f.engine.request('operation',{id:args.operationId});assert.equal(op.request,undefined);assert.equal((await f.engine.request('messages',{chat:PEER})).messages[0].text,'replacement');
 const real=Date.now,now=real();Date.now=()=>now+WEEK+1000;try{assert.equal((await f.engine.request('operation',{id:args.operationId})).state,'cancelled');assert.equal((await f.engine.request('send',args)).state,'cancelled');assert.equal(socket.sent.length,1);}finally{Date.now=real;}
});
test('alias tombstones remove existing canonical copies and cancel old alias drafts without retargeting grants',async t=>{
 const f=await fixture(t),socket=await f.connect(),lid='1234567890@lid';
 socket.ev.emit('messages.upsert',{messages:[fixtureMessage('REVOKED')]});socket.ev.emit('messages.delete',{keys:[{remoteJid:lid,id:'REVOKED',fromMe:false}]});await drain();
 const args={operationId:randomUUID(),chat:lid,text:'unsent alias draft',kind:'ordinary'};await f.engine.request('prepareSend',args);
 socket.ev.emit('contacts.upsert',[{id:lid,phoneNumber:PEER,name:'Alice'}]);await drain();assert.equal((await f.engine.request('messages',{chat:PEER})).messages.length,0);
 const op=await f.engine.request('operation',{id:args.operationId});assert.equal(op.state,'cancelled');assert.equal(op.request,undefined);assert.equal((await f.engine.request('send',args)).state,'cancelled');assert.equal(socket.sent.length,0);
});
test('alias duplicate merges keep shortest expiry, existing read marker and strongest delivery',async t=>{
 const f=await fixture(t),socket=await f.connect(),lid='1234567890@lid',at=Math.floor(Date.now()/1000);
 socket.ev.emit('messages.upsert',{messages:[fixtureMessage('DUP','canonical',{messageTimestamp:at,message:{extendedTextMessage:{text:'canonical',contextInfo:{expiration:86400}}}}),fixtureMessage('DUP','alias',{messageTimestamp:at,key:{remoteJid:lid,id:'DUP',fromMe:false}})]});await drain();await f.engine.request('markRead',{chat:PEER,ids:['DUP']});
 socket.ev.emit('contacts.upsert',[{id:lid,phoneNumber:PEER}]);await drain();const page=await f.engine.request('messages',{chat:PEER});assert.equal(page.messages.length,1);assert.equal(page.messages[0].expiresAt,at*1000+86400000);assert.equal((await f.engine.request('status')).unreadTotal,0);
});
test('every UUID bit contributes to WhatsApp ID and echoes cannot redirect into another operation chat',async t=>{
 const f=await fixture(t),socket=await f.connect(),other='15550000003@s.whatsapp.net';
 const a={operationId:'10000000-0000-4000-8000-000000000001',chat:PEER,text:'FIRST',kind:'ordinary'},b={operationId:'10000000-0000-4000-8000-000000000002',chat:other,text:'SECOND',kind:'ordinary'};
 const pa=await f.engine.request('prepareSend',a),pb=await f.engine.request('prepareSend',b);assert.notEqual(pa.messageId,pb.messageId);await f.engine.request('send',a);
 assert.equal((await f.engine.request('messages',{chat:PEER})).messages[0].text,'FIRST');assert.ok(!(await f.engine.request('messages',{chat:other})).messages.some(m=>m.text==='FIRST'));
 socket.ev.emit('messages.upsert',{messages:[fixtureMessage(pa.messageId,'unrelated other chat',{key:{id:pa.messageId,remoteJid:other,fromMe:true}})]});await drain();
 assert.equal((await f.engine.request('messages',{chat:PEER})).messages[0].text,'FIRST');assert.ok((await f.engine.request('messages',{chat:other})).messages.some(m=>m.text==='unrelated other chat'&&!m.operationId));
 socket.ev.emit('messages.upsert',{messages:[fixtureMessage(pa.messageId,'incoming reused id',{key:{id:pa.messageId,remoteJid:other,fromMe:false},status:4})]});
 socket.ev.emit('messages.update',[{key:{id:pa.messageId,remoteJid:other,fromMe:false},update:{status:4}}]);await drain();
 assert.equal((await f.engine.request('operation',{id:a.operationId})).state,'sent');assert.equal((await f.engine.request('messages',{chat:PEER})).messages[0].text,'FIRST');assert.ok((await f.engine.request('messages',{chat:other})).messages.some(m=>m.text==='incoming reused id'&&!m.operationId));
});
