import { Store } from '../src/store.mjs';
import test from 'node:test';
import assert from 'node:assert/strict';
import { createCipheriv, createHash, createHmac } from 'node:crypto';
import { getMediaKeys } from 'baileys';
import { fixture, fixtureMessage, PEER, drain } from './helpers.mjs';
import { MEDIA_CHUNK, mediaDescriptor } from '../src/media-content.mjs';
import { messageId, WEEK } from '../src/content.mjs';
function inspect(f){const store=new Store(f.directory,f.config.key,{account:f.config.account,connection:f.config.connection});return store;}
const hash=b=>createHash('sha256').update(b).digest();
async function attachment(type='audio',length=MEDIA_CHUNK*2+17) {
  const plain=Buffer.alloc(length,7);plain.write(type==='sticker'?'RIFF':'OggS');
  if(type==='sticker'){plain.write('WEBP',8);plain.write('VP8X',12);plain.fill(0,24,30);}
  const key=Buffer.alloc(32,18),keys=await getMediaKeys(key,type==='sticker'?'image':type),cipher=createCipheriv('aes-256-cbc',keys.cipherKey,keys.iv);
  const encrypted=Buffer.concat([cipher.update(plain),cipher.final()]);
  const bytes=Buffer.concat([encrypted,createHmac('sha256',keys.macKey).update(keys.iv).update(encrypted).digest().subarray(0,10)]);
  return {plain,bytes,body:{[`${type}Message`]:{url:'https://mmg.whatsapp.net/synthetic',mimetype:type==='sticker'?'image/webp':'audio/ogg; codecs=opus',fileLength:plain.length,mediaKey:key,fileSha256:hash(plain),fileEncSha256:hash(bytes),ptt:type==='audio'}}};
}
const target=row=>({chat:row.chat,messageId:row.id,fromMe:row.fromMe,expectedVersion:row.version});
test('authenticated media is encrypted in bounded chunks and supports range reads until deletion',async t=>{
  const media=await attachment();const f=await fixture(t,{photoFetch:async()=>new Response(media.bytes)});const socket=await f.connect();
  socket.ev.emit('messages.upsert',{messages:[fixtureMessage('AUDIO','',{message:media.body})]});await drain();
  const page=await f.engine.request('messages',{chat:PEER}),row=page.messages[0];
  assert.equal(row.media.type,'audio');assert.equal(row.media.voice,true);assert.equal(JSON.stringify(page).includes('mediaKey'),false);assert.equal(JSON.stringify(page).includes('mmg.whatsapp'),false);
  const ready=await f.engine.request('mediaOpen',target(row));assert.equal(ready.state,'ready');
  let offset=0;const chunks=[];
  while(offset<media.plain.length){const part=await f.engine.request('mediaRead',{token:ready.token,offset,length:MEDIA_CHUNK});const bytes=Buffer.from(part.bytes,'base64');offset+=bytes.length;chunks.push(bytes);}
  assert.deepEqual(Buffer.concat(chunks),media.plain);
  let store=inspect(f);assert.equal(store.totals('media-chunk').count,3);store.close();
  await assert.rejects(f.engine.request('mediaRead',{token:ready.token,offset:0,length:MEDIA_CHUNK+1}));
  socket.ev.emit('messages.delete',{keys:[{remoteJid:PEER,id:row.id,fromMe:false}]});await drain();
  await assert.rejects(f.engine.request('mediaRead',{token:ready.token,offset:0,length:1}));
  {const store=inspect(f);assert.equal(store.totals('media-chunk').count,0);store.close();}
});
test('corrupted, oversized or mismatched media never produces a readable cache',async t=>{
  const media=await attachment();media.bytes[35]^=1;
  const f=await fixture(t,{photoFetch:async()=>new Response(media.bytes)}),socket=await f.connect();
  socket.ev.emit('messages.upsert',{messages:[fixtureMessage('BAD','',{message:media.body})]});await drain();
  const row=(await f.engine.request('messages',{chat:PEER})).messages[0];
  assert.equal((await f.engine.request('mediaOpen',target(row))).state,'unavailable');{const store=inspect(f);assert.equal(store.totals('media-chunk').count,0);store.close();}
  assert.equal(mediaDescriptor({audioMessage:{...media.body.audioMessage,fileLength:257*1024**2}}),undefined);
});
test('media capabilities cannot survive pause, restart or clearing the cache',async t=>{
  const media=await attachment('sticker',100),f=await fixture(t,{photoFetch:async()=>new Response(media.bytes)}),socket=await f.connect();
  socket.ev.emit('messages.upsert',{messages:[fixtureMessage('STICKER','',{message:media.body})]});await drain();
  const row=(await f.engine.request('messages',{chat:PEER})).messages[0],ready=await f.engine.request('mediaOpen',target(row));assert.equal(ready.state,'ready');
  await f.reopen();await assert.rejects(f.engine.request('mediaRead',{token:ready.token,offset:0,length:1}));
  {const store=inspect(f);assert.equal(store.totals('media-chunk').count,1);store.close();}
  await f.engine.request('storageSettings',{clearMedia:true});{const store=inspect(f);assert.equal(store.totals('media-chunk').count,0);store.close();}
});
test('new policy never revives expired records or extends protected deadlines',async t=>{
  const f=await fixture(t),socket=await f.connect();
  socket.ev.emit('messages.upsert',{messages:[fixtureMessage('OLD')]});await drain();
  const old=(await f.engine.request('messages',{chat:PEER})).messages[0];
  socket.ev.emit('messages.delete',{keys:[{remoteJid:PEER,id:old.id,fromMe:false}]});await drain();
  const settings=await f.engine.request('storageSettings',{historyDays:90});assert.equal(settings.historyDays,90);
  socket.ev.emit('messages.upsert',{messages:[fixtureMessage('OLD'),fixtureMessage('NEW')]});await drain();
  const rows=(await f.engine.request('messages',{chat:PEER})).messages;assert.deepEqual(rows.map(r=>r.id),['NEW']);assert.ok(rows[0].expiresAt>Date.now()+WEEK);
  const key={chat:PEER,id:'NEW',fromMe:false};
  await f.engine.request('storageSettings',{historyDays:7});const store=inspect(f);assert.ok(store.get('message',messageId(key)).expiresAt<=Date.now()+WEEK);
  assert.ok(store.get('system','binding'));store.close();
});
test('played receipts require playback request, incoming voice media and enabled privacy',async t=>{
  const media=await attachment(),f=await fixture(t,{photoFetch:async()=>new Response(media.bytes)}),socket=await f.connect(),receipts=[];
  socket.fetchPrivacySettings=async()=>({readreceipts:'all'});socket.sendReceipts=async(...args)=>receipts.push(args);
  socket.ev.emit('messages.upsert',{messages:[fixtureMessage('VOICE','',{message:media.body})]});await drain();
  const row=(await f.engine.request('messages',{chat:PEER})).messages[0],ready=await f.engine.request('mediaOpen',target(row));
  assert.equal(receipts.length,0);await f.engine.request('mediaPlayed',{token:ready.token});await f.engine.request('mediaPlayed',{token:ready.token});assert.equal(receipts.length,1);assert.equal(receipts[0][1],'played');
});
test('explicit media retry renews only the locator for the same authenticated attachment',async t=>{
  const media=await attachment(),f=await fixture(t,{photoFetch:async url=>url.endsWith('/renewed')?new Response(media.bytes):new Response(null,{status:410})}),socket=await f.connect();
  socket.updateMediaMessage=async()=>({message:{audioMessage:{...media.body.audioMessage,url:'https://mmg.whatsapp.net/renewed'}}});
  socket.ev.emit('messages.upsert',{messages:[fixtureMessage('RENEW','',{message:media.body})]});await drain();
  const row=(await f.engine.request('messages',{chat:PEER})).messages[0];
  assert.equal((await f.engine.request('mediaOpen',target(row))).state,'unavailable');
  assert.equal((await f.engine.request('mediaOpen',{...target(row),retry:true})).state,'ready');
});
