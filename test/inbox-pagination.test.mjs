import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Store } from '../src/store.mjs';
import { Inbox } from '../src/inbox.mjs';
import { sanitizeMessage, messageId } from '../src/content.mjs';

const GROUP='120363000000001@g.us',PEER='15550000001@s.whatsapp.net';
async function setup(t){
  const directory=await mkdtemp(join(tmpdir(),'cifer-pagination-test-'));
  const store=new Store(directory,Buffer.alloc(32,73),{account:'synthetic-pagination',connection:'fixture-link'}),inbox=new Inbox(store,()=>{});
  t.after(async()=>{store.close();await rm(directory,{recursive:true,force:true});});return {store,inbox};
}
function collect(page,key){
  const values=[];let before;
  do{const result=page(before);values.push(...result[key]);before=result.cursor;assert.ok(values.length<1000,'pagination terminates');}while(before);
  return values;
}
for(const view of ['active','archived'])test(`${view} chat cursors retain prefix IDs and every old discovered row beyond 200`,async t=>{
  const {inbox}=await setup(t),ids=['120363000000000001@g.us','12036300000000000@g.us',...Array.from({length:205},(_,i)=>`${120363000100000+i}@g.us`)];
  for(const id of ids)inbox.contact(id,{name:'Synthetic old group',archived:view==='archived'});
  inbox.contact('120363000999999@g.us',{name:'Other view',archived:view!=='archived'});
  const expected=[...ids].sort().reverse();
  for(const limit of [1,20])assert.deepEqual(collect(before=>inbox.chats({view,limit,before}),'chats').map(row=>row.id),expected);
});
for(const chat of [PEER,GROUP])test(`${chat===GROUP?'group':'direct'} equal-time message cursors retain case and punctuation ties`,async t=>{
  const {inbox}=await setup(t),timestamp=Math.floor(Date.now()/1000),ids=['CASE','case','A-B','A_B','AB','AB0'];
  const participants=chat===GROUP?['155500000001@s.whatsapp.net','15550000000@s.whatsapp.net']:[undefined],expected=[];
  for(const id of ids)for(const participant of participants){
    const row=sanitizeMessage({key:{remoteJid:chat,id,fromMe:false,...(participant?{participant}:{})},messageTimestamp:timestamp,message:{conversation:'Synthetic pagination text'}});
    inbox.ingest(row);expected.push(messageId(row));
  }
  const full=inbox.messages(chat,{limit:100}).messages.map(messageId);
  assert.equal(new Set(full).size,expected.length);
  for(const limit of [1,3])assert.deepEqual(collect(before=>inbox.messages(chat,{limit,before}),'messages').map(messageId),full);
});
