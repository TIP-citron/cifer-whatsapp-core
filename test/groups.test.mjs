import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { Store } from '../src/store.mjs';
import { Inbox } from '../src/inbox.mjs';
import { Groups } from '../src/groups.mjs';
import { Mutations } from '../src/mutations.mjs';
import { keyOf, messageId, sanitizeMessage, sanitizeMutation, messageVersion, WEEK } from '../src/content.mjs';
import { encryptedEdit } from '../src/encrypted-edits.mjs';

const GROUP='120363000000001@g.us',OTHER='120363000000002@g.us',SELF='15550000002@s.whatsapp.net',ALICE='15550000001@s.whatsapp.net',BOB='15550000003@s.whatsapp.net',LID='1234567890@lid';
const raw=(id,participant=ALICE,extra={})=>({key:{remoteJid:GROUP,id,fromMe:participant===SELF,participant},messageTimestamp:Math.floor(Date.now()/1000),pushName:participant===ALICE?'Alice':'Bob',message:{conversation:'synthetic group text'},...extra});
const clean=(id,participant=ALICE,extra={})=>sanitizeMessage(raw(id,participant,extra));
async function setup(t){
  const directory=await mkdtemp(join(tmpdir(),'cifer-group-test-')),key=Buffer.alloc(32,71),store=new Store(directory,key,{account:'fixture-account',connection:'fixture-link'}),inbox=new Inbox(store,()=>{});
  const engine={store,inbox,epoch:1,state:'connected',identity:SELF,closed:false,inflight:new Set(),metadata(){},changed(){},async canonical(value,alternate){const mapped=value===LID?alternate||store.get('alias',value):value;if(value===LID&&mapped){store.set('alias',value,mapped);inbox.rebindParticipant(value,mapped);}return mapped||value;},socket:{user:{id:SELF},async groupMetadata(id){return {id,subject:'Fixture group',participants:[{id:SELF},{id:ALICE,admin:'admin'},{id:BOB}]};}},sendInput(value){return value;}};
  const groups=engine.groups=new Groups(engine);
  t.after(async()=>{engine.closed=true;store.close();await rm(directory,{recursive:true,force:true});});
  return {directory,key,store,inbox,engine,groups};
}

test('group keys preserve author and protocol identity while direct record IDs stay unchanged',()=>{
  const direct=keyOf({remoteJid:ALICE,id:'SAME',fromMe:false});assert.equal(messageId(direct),JSON.stringify([ALICE,'SAME',false]));
  const alice=keyOf(raw('SAME').key),bob=keyOf(raw('SAME',BOB).key);assert.notEqual(messageId(alice),messageId(bob));assert.equal(alice.protocolKey.participant,ALICE);
  assert.equal(keyOf({remoteJid:GROUP,id:'SAME',fromMe:false}),null);assert.equal(keyOf({remoteJid:'status@broadcast',id:'SAME',fromMe:false,participant:ALICE}),null);
});

test('group timeline keeps duplicate IDs distinct, labels senders, pages fully and targets reads',async t=>{
  const {inbox}=await setup(t);inbox.contact(GROUP,{name:'Group subject'});inbox.ingest(clean('SAME',ALICE));inbox.ingest(clean('SAME',BOB));
  assert.equal(inbox.chat(GROUP).name,'Group subject');const first=inbox.messages(GROUP,{limit:1}),second=inbox.messages(GROUP,{limit:1,before:first.cursor});
  assert.equal(first.messages.length,1);assert.equal(second.messages.length,1);assert.notEqual(first.messages[0].participant,second.messages[0].participant);
  assert.equal(first.messages[0].protocolKey,undefined);assert.ok(first.messages[0].senderName);
  assert.throws(()=>inbox.read(GROUP,['SAME']));inbox.read(GROUP,[{id:'SAME',fromMe:false,participant:ALICE}]);assert.equal(inbox.chat(GROUP).unread,1);
});

test('archive filtering precedes pagination and archived unread stays quiet',async t=>{
  const {inbox}=await setup(t);inbox.ingest(clean('ONE'));inbox.ingest(clean('TWO',BOB,{key:{...raw('TWO',BOB).key,remoteJid:OTHER}}));
  inbox.contact(GROUP,{archived:true});assert.equal(inbox.chats({view:'active',limit:1}).chats[0].id,OTHER);assert.equal(inbox.chats({view:'archived'}).chats[0].id,GROUP);
  assert.deepEqual({...inbox.unread(),unreadChats:undefined},{unreadTotal:1,unreadChats:undefined,archivedCount:1,archivedUnreadTotal:1});assert.equal(inbox.chat(GROUP).unread,1);
  inbox.contact(GROUP,{name:'Renamed'});assert.equal(inbox.chat(GROUP).archived,true);
});

test('group receipt counts deduplicate users, include reads in delivery, never imply everyone read',async t=>{
  const {inbox}=await setup(t),message=clean('SENT',SELF);inbox.ingest(message);
  inbox.groupReceipt(message,ALICE,'delivered');inbox.groupReceipt(message,ALICE,'read');inbox.groupReceipt(message,ALICE,'read');inbox.groupReceipt(message,BOB,'delivered');inbox.delivery(message,'read');
  const row=inbox.messages(GROUP).messages[0];assert.equal(row.delivery,'sent');assert.equal(row.deliveredCount,2);assert.equal(row.readCount,1);
  inbox.tombstone(message);assert.equal(inbox.receiptCounts(message).deliveredCount,0);
});

test('late participant alias reconciliation preserves tombstones, receipts and read state',async t=>{
  const {inbox,store}=await setup(t);inbox.ingest(clean('ONE',LID));inbox.ingest(clean('ONE',ALICE));inbox.read(GROUP,[{id:'ONE',fromMe:false,participant:LID}]);
  inbox.rebindParticipant(LID,ALICE);assert.equal(inbox.messages(GROUP).messages.length,1);assert.equal(inbox.chat(GROUP).unread,0);
  inbox.tombstone({chat:GROUP,id:'GONE',fromMe:false,participant:LID});inbox.ingest(clean('GONE',ALICE));inbox.rebindParticipant(LID,ALICE);assert.ok(!inbox.messages(GROUP).messages.some(r=>r.id==='GONE'));
  const message=clean('SENT',SELF);inbox.ingest(message);inbox.groupReceipt(message,LID,'read');inbox.groupReceipt(message,ALICE,'delivered');inbox.rebindParticipant(LID,ALICE);assert.deepEqual(inbox.receiptCounts(message),{deliveredCount:1,readCount:1});
  assert.equal(store.get('message',messageId({chat:GROUP,id:'ONE',fromMe:false,participant:LID})),null);
});

test('discovery includes old existing groups without fetching histories; metadata cannot rename from sender',async t=>{
  const {groups,engine,inbox,store}=await setup(t);let calls=0,release;
  engine.socket.groupFetchAllParticipating=async()=>{calls++;await new Promise(resolve=>{release=resolve;});return {[GROUP]:await engine.socket.groupMetadata(GROUP),[OTHER]:await engine.socket.groupMetadata(OTHER)};};
  const a=groups.discover(),b=groups.discover();release();await Promise.all([a,b]);assert.equal(calls,1);assert.equal(inbox.chats().chats.length,2);assert.equal(inbox.messages(GROUP).messages.length,0);assert.equal(store.totals('group-roster').count,0);
  await groups.authorizeSend(GROUP);assert.equal(store.totals('group-roster').count,1);
});

test('fresh send authorization applies announcement, membership, alias-self and metadata limits',async t=>{
  const {groups,engine,inbox}=await setup(t);
  engine.socket.groupMetadata=async id=>({id,subject:'Announcements',announce:true,participants:[{id:SELF},{id:ALICE,admin:'admin'}]});await assert.rejects(groups.authorizeSend(GROUP),/admins/);
  engine.socket.user.lid=LID;engine.socket.groupMetadata=async id=>({id,participants:[{id:LID,admin:'admin'}],announce:true});await groups.authorizeSend(GROUP);
  await groups.participants({id:GROUP,action:'remove',participants:[{id:LID}]});assert.equal(inbox.chat(GROUP).canSend,false);assert.equal(inbox.chat(GROUP).sendRestriction,'left');
  await new Promise(resolve=>setImmediate(resolve));engine.socket.groupMetadata=async id=>({id,participants:[{id:ALICE,admin:'admin'}]});await assert.rejects(groups.authorizeSend(GROUP),/read-only/);
  engine.socket.groupMetadata=async id=>({id,participants:Array.from({length:2049},()=>({id:SELF}))});await assert.rejects(groups.authorizeSend(GROUP));
});

test('membership change while fresh metadata is loading prevents a stale send authorization',async t=>{
  const {groups,engine}=await setup(t);const finishes=[];engine.socket.groupMetadata=id=>new Promise(resolve=>{finishes.push(()=>resolve({id,participants:[{id:SELF}]}));});
  const promise=groups.authorizeSend(GROUP);await groups.participants({id:GROUP,action:'remove',participants:[{id:SELF}]});for(const finish of finishes)finish();await assert.rejects(promise,/changed/);
});

test('incoming mutations reject wrong-author edits; admin revokes require role evidence at envelope time',async t=>{
  const {groups,inbox,store,engine}=await setup(t);inbox.ingest(clean('TARGET',BOB));await groups.refresh(GROUP);
  const from=store.get('group-role',GROUP).intervals.at(-1).from;
  const base={chat:GROUP,id:'TARGET',fromMe:false,participant:ALICE,targetParticipant:BOB,envelope:true,envelopeAt:from,at:from};
  assert.equal(await groups.authorizeMutation({...base,removed:false}),null);
  const valid=await groups.authorizeMutation({...base,removed:true});assert.equal(valid.participant,BOB);assert.equal(valid.actor,ALICE);
  assert.equal(await groups.authorizeMutation({...base,removed:true,envelopeAt:from-1}),null);
  await groups.participants({id:GROUP,action:'demote',participants:[{id:ALICE}]});assert.equal(await groups.authorizeMutation({...base,removed:true,envelopeAt:Date.now()+1}),null);
  const restarted=new Groups(engine);assert.equal(await restarted.authorizeMutation({...base,removed:true}),null);
});

test('group mutation normalization cannot redirect chat; encrypted edits derive from participant',()=>{
  const envelope=raw('EDIT',ALICE,{message:{protocolMessage:{type:14,key:{id:'TARGET',remoteJid:OTHER,participant:BOB},editedMessage:{conversation:'forged'}}}});assert.equal(sanitizeMutation(envelope),null);
  const sealed=raw('ENC',ALICE,{message:{secretEncryptedMessage:{secretEncType:2,targetMessageKey:{id:'TARGET'},encPayload:Buffer.alloc(17),encIv:Buffer.alloc(12)}}});assert.deepEqual(encryptedEdit(sealed).authors,[ALICE]);
});

test('outgoing group mutations require participant and never permit changing another author for everyone',async t=>{
  const {engine,inbox}=await setup(t);inbox.ingest(clean('OWN',SELF));inbox.ingest(clean('THEIRS',ALICE));const mutations=new Mutations(engine),own=inbox.messages(GROUP).messages.find(r=>r.fromMe);
  const input={operationId:randomUUID(),chat:GROUP,messageId:'OWN',fromMe:true,action:'edit',expectedVersion:own.version,text:'edit',kind:'ordinary'};
  assert.throws(()=>mutations.prepare(input),/author/);const prepared=mutations.prepare({...input,participant:SELF});assert.equal(prepared.participant,SELF);
  const theirs=inbox.messages(GROUP).messages.find(r=>!r.fromMe);assert.throws(()=>mutations.prepare({...input,operationId:randomUUID(),messageId:theirs.id,fromMe:false,participant:ALICE,action:'delete-everyone',text:undefined,kind:undefined,expectedVersion:theirs.version}),/own messages/);
});

test('group metadata and sender identifiers are encrypted and account-bound',async t=>{
  const {groups,inbox,store,directory,key}=await setup(t);await groups.refresh(GROUP);inbox.ingest(clean('PRIVATE'));store.checkpoint();
  const bytes=await readFile(join(directory,'whatsapp.sqlite'));for(const value of [GROUP,ALICE,'Fixture group','synthetic group text'])assert.equal(bytes.includes(Buffer.from(value)),false);
  assert.throws(()=>new Store(directory,key,{account:'another',connection:'fixture-link'}),/account changed/);
});

test('verified admin revoke before original reconciles only after its matching author arrives',async t=>{
  const {groups,inbox,store}=await setup(t);await groups.refresh(GROUP);const at=store.get('group-role',GROUP).intervals.at(-1).from;
  const mutation={chat:GROUP,id:'LATE',fromMe:false,participant:ALICE,targetParticipant:BOB,envelope:true,envelopeAt:at,at,removed:true};
  assert.equal(await groups.authorizeMutation(mutation),null);assert.equal(store.totals('group-pending-mutation').count,1);
  inbox.ingest(clean('LATE',ALICE));groups.reconcilePending(GROUP);assert.equal(inbox.messages(GROUP).messages.length,1);
  inbox.ingest(clean('LATE',BOB));groups.reconcilePending(GROUP);assert.deepEqual(inbox.messages(GROUP).messages.map(r=>r.participant),[ALICE]);assert.equal(store.totals('group-pending-mutation').count,0);
});

test('reconnect invalidates old role evidence, roster and writable status; close cancels stuck discovery',async t=>{
  const {groups,engine,inbox,store}=await setup(t);await groups.refresh(GROUP);const at=store.get('group-role',GROUP).intervals.at(-1).from;assert.ok(groups.roleAt(GROUP,ALICE,at));
  engine.epoch++;engine.socket.groupFetchAllParticipating=()=>new Promise(()=>{});const pending=groups.discover();assert.equal(inbox.chat(GROUP).canSend,false);assert.equal(store.totals('group-roster').count,0);assert.equal(groups.roleAt(GROUP,ALICE,at),false);
  engine.closing=true;await assert.rejects(pending,/changed/);assert.ok(inbox.chat(GROUP));
});

test('known removal wipes prepared requests and leaves in-flight operations uncertain',async t=>{
  const {groups,store}=await setup(t);await groups.refresh(GROUP);const now=Date.now();
  for(const state of ['prepared','sending'])store.set('operation',state,{operationId:state,chat:GROUP,messageId:state,participant:SELF,state,request:{text:'SECRET_DRAFT'},createdAt:now,expiresAt:now+WEEK},{created:now,expires:now+WEEK,tag:state});
  await groups.participants({id:GROUP,action:'remove',participants:[{id:SELF}]});assert.equal(store.get('operation','prepared').state,'cancelled');assert.equal(store.get('operation','sending').state,'uncertain');assert.equal(store.get('operation','prepared').request,undefined);assert.equal(store.get('operation','sending').request,undefined);
});

test('folded group lifecycle records cannot masquerade as authenticated originals',async t=>{
  const {groups}=await setup(t);assert.equal(sanitizeMessage(raw('EDIT',ALICE,{message:{editedMessage:{message:{conversation:'unverified'}}}})),null);
  assert.equal(await groups.authorizeMutation({chat:GROUP,id:'ID',participant:ALICE,removed:true,envelope:false}),null);
});

test('legacy direct chat projections supply defaults and omit nullable group restrictions',async t=>{
  const {inbox,store}=await setup(t);store.set('chat',ALICE,{id:ALICE,name:'Legacy direct',updatedAt:1});inbox.contact(GROUP,{name:'Group',canSend:true,sendRestriction:null});
  const direct=inbox.chat(ALICE);assert.equal(direct.kind,'direct');assert.equal(direct.archived,false);assert.equal(direct.canSend,true);
  const rows=inbox.chats().chats;assert.equal(rows.find(r=>r.id===ALICE).canSend,true);assert.equal('sendRestriction' in rows.find(r=>r.id===GROUP),false);assert.equal('sendRestriction' in inbox.chat(GROUP),false);
});

test('pending participant reads merge into already-stored canonical messages',async t=>{
  const {inbox}=await setup(t);inbox.delivery({chat:GROUP,id:'EARLY',fromMe:false,participant:LID},'read');inbox.ingest(clean('EARLY',ALICE));assert.equal(inbox.chat(GROUP).unread,1);
  inbox.rebindParticipant(LID,ALICE);assert.equal(inbox.chat(GROUP).unread,0);
});

test('participant changes refresh metadata once and restore legitimate sending without reconnect',async t=>{
  const {groups,engine,inbox}=await setup(t);await groups.refresh(GROUP);let calls=0;
  engine.socket.groupMetadata=async id=>{calls++;return {id,participants:[{id:SELF},{id:ALICE},{id:BOB}]};};
  await groups.participants({id:GROUP,action:'add',participants:[{id:BOB}]});await new Promise(resolve=>setImmediate(resolve));
  assert.equal(calls,1);assert.equal(inbox.chat(GROUP).canSend,true);
});

test('fresh authoritative membership clears stale left/read-only state after rejoining',async t=>{
  const {groups,inbox}=await setup(t);inbox.contact(GROUP,{member:false,readOnly:true,canSend:false,sendRestriction:'left'});await groups.refresh(GROUP);assert.equal(inbox.chat(GROUP).canSend,true);assert.equal(inbox.chat(GROUP).readOnly,false);
});
