import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { Store } from '../src/store.mjs';
import { Inbox } from '../src/inbox.mjs';
import { WEEK,sanitizeMessage } from '../src/content.mjs';
import { fixture,fixtureMessage,drain,PEER,ACCOUNT,wire } from './helpers.mjs';

test('byte/count eviction is oldest-first, durable and cannot resurrect evicted history',async t=>{
 const f=await fixture(t);await f.engine.close();const store=new Store(f.directory,f.config.key,{account:ACCOUNT,connection:f.config.connection});
 try{const inbox=new Inbox(store,()=>{},{maxMessages:2,maxBytes:2000});for(let i=0;i<3;i++)inbox.ingest(sanitizeMessage(fixtureMessage('MSG'+i,'text',{messageTimestamp:Math.floor(Date.now()/1000)-10+i})),{history:true});
 assert.equal(store.totals('message').count,2);assert.equal(store.totals('tombstone').count,1);inbox.ingest(sanitizeMessage(fixtureMessage('MSG0','replayed',{messageTimestamp:Math.floor(Date.now()/1000)-10})));assert.equal(store.totals('message').count,2);
 inbox.ingest(sanitizeMessage(fixtureMessage('BIG','x'.repeat(3000))));assert.ok(store.totals('message').bytes<=2000);
 }finally{store.close();}
});
test('shared content budget includes prepared outbox bodies and does not evict an unsent operation',async t=>{
 const f=await fixture(t);await f.engine.close();const store=new Store(f.directory,f.config.key,{account:ACCOUNT,connection:f.config.connection});
 try{const inbox=new Inbox(store,()=>{},{maxBytes:1600});inbox.ingest(sanitizeMessage(fixtureMessage('BODY','x'.repeat(600))));store.set('operation','pending',{text:'y'.repeat(900)});inbox.trim();assert.equal(store.totals('message').count,0);assert.ok(store.get('operation','pending'));assert.throws(()=>inbox.trim(1000),/storage is full/);assert.ok(store.get('operation','pending'));}finally{store.close();}
});
test('close during remote unlink awaits erasure and closes database without leaked callbacks',async t=>{
 const f=await fixture(t),socket=await f.connect();let release;socket.logout=()=>new Promise(resolve=>{release=resolve;});
 const unlink=f.engine.disconnect(),closed=f.engine.close();release();assert.equal((await unlink).remoteUnlinked,true);await closed;
 socket.ev.emit('messages.upsert',{messages:[fixtureMessage('LATE')]});await drain();await assert.rejects(f.engine.request('status'));
 const store=new Store(f.directory,f.config.key,{account:ACCOUNT,connection:f.config.connection});assert.equal(store.get('wa-auth','creds'),null);store.close();
});
test('crashed sending journal restores as uncertain and never automatically repeats WhatsApp I/O',async t=>{
 const f=await fixture(t);await f.engine.close();const store=new Store(f.directory,f.config.key,{account:ACCOUNT,connection:f.config.connection});
 const operationId=randomUUID(),createdAt=Date.now(),record={operationId,chat:PEER,digest:'fixture',state:'sending',messageId:'FIXTURE_SEND',createdAt,expiresAt:createdAt+WEEK};
 store.set('operation',operationId,record,{created:createdAt,expires:record.expiresAt,tag:record.messageId});store.close();await f.reopen();assert.equal((await f.engine.request('operation',{id:operationId})).state,'uncertain');assert.equal(f.sockets.length,0);
});
test('protected preparation requires current explicit contact binding and rejects changed mappings',async t=>{
 const f=await fixture(t);await f.connect();const owner={principal:ACCOUNT,secretId:'101',publicKey:Buffer.alloc(1184).toString('base64')},peer={...owner,principal:'20000000-0000-4000-8000-000000000002',secretId:'102'};
 const binding={version:1,principal:ACCOUNT,chat:PEER,contactId:randomUUID(),revision:randomUUID(),owner,peer,confirmedAt:Date.now()};
 const text=wire({contact:binding.contactId}),prepared={version:1,principal:ACCOUNT,chat:PEER,owner,binding,wire:text,recovery:{encrypted:'11'.repeat(128),cifer:'22'.repeat(1104)},recipientGrant:{encrypted:'33'.repeat(128),cifer:'44'.repeat(1104)}};
 const args={operationId:randomUUID(),chat:PEER,text,kind:'protected',prepared};
 await assert.rejects(f.engine.request('prepareSend',args));await f.engine.request('bindContact',{chat:PEER,binding});assert.equal((await f.engine.request('prepareSend',args)).state,'prepared');
 await f.engine.request('bindContact',{chat:PEER,binding:null});await assert.rejects(f.engine.request('send',args));await assert.rejects(f.engine.request('send',{operationId:randomUUID(),chat:PEER,text:wire(),kind:'protected'}));assert.equal(f.sockets[0].sent.length,0);
});
