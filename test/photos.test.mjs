import test from 'node:test';
import assert from 'node:assert/strict';
import { createCipheriv, createHmac, createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { getMediaKeys } from 'baileys';
import { fixture, fixtureMessage, drain, PEER } from './helpers.mjs';
import { messageId } from '../src/content.mjs';
import { imageDescriptor, photoURL, PHOTO_BYTES } from '../src/photo-content.mjs';
import { Store } from '../src/store.mjs';
const png=Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+ip1sAAAAASUVORK5CYII=','base64');
const group='120363000000000000@g.us',other='15550000003@s.whatsapp.net';
const hash=x=>createHash('sha256').update(x).digest();
async function attachment(bytes=png){const mediaKey=Buffer.alloc(32,7),keys=await getMediaKeys(mediaKey,'image'),c=createCipheriv('aes-256-cbc',keys.cipherKey,keys.iv),enc=Buffer.concat([c.update(bytes),c.final()]),mac=createHmac('sha256',keys.macKey).update(keys.iv).update(enc).digest().subarray(0,10),encrypted=Buffer.concat([enc,mac]);return {encrypted,image:{url:'https://mmg.whatsapp.net/v/t62.7118-24/photo.enc?fixture=1',mediaKey,fileSha256:hash(bytes),fileEncSha256:hash(encrypted),fileLength:bytes.length,mimetype:'image/png',caption:'Photo fixture'}};}
const response=bytes=>new Response(bytes,{status:200,headers:{'content-length':String(bytes.length)}});
const request=row=>({chat:row.chat,messageId:row.id,fromMe:row.fromMe,...(row.participant?{participant:row.participant}:{}),expectedVersion:row.version});
async function insert(f,socket,image,options={}){socket.ev.emit('messages.upsert',{messages:[fixtureMessage(options.id||'PHOTO','',{key:{remoteJid:options.chat||PEER,id:options.id||'PHOTO',fromMe:false,...(options.participant?{participant:options.participant}:{})},message:{imageMessage:image}})]});await drain();return (await f.engine.request('messages',{chat:options.chat||PEER})).messages.find(r=>r.id===(options.id||'PHOTO')&&(!options.participant||r.participant===options.participant));}
test('photos use a private descriptor, authenticated bytes, encrypted persistent cache and no renderer secrets',async t=>{
 const a=await attachment();let calls=0;const f=await fixture(t,{photoFetch:async(url,opts)=>{calls++;assert.equal(new URL(url).hostname,'mmg.whatsapp.net');assert.equal(opts.redirect,'error');assert.equal(opts.credentials,'omit');return response(a.encrypted);}}),s=await f.connect(),row=await insert(f,s,a.image);
 assert.deepEqual(row.photo,{available:true});assert.ok(!JSON.stringify(row).includes('mediaKey'));assert.ok(!JSON.stringify(row).includes('whatsapp.net/v/'));
 const photo=await f.engine.request('photo',request(row));assert.equal(photo.state,'ready');assert.equal(photo.dataUrl,'data:image/png;base64,'+png.toString('base64'));
 await f.reopen();assert.equal((await f.engine.request('photo',request(row))).state,'ready');assert.equal(calls,1);
 const bytes=await readFile(f.directory+'/whatsapp.sqlite');assert.equal(bytes.includes(Buffer.from(photo.dataUrl)),false);assert.equal(bytes.includes(Buffer.from(a.image.mediaKey.toString('hex'))),false);
});
test('group photos with identical message IDs remain participant-bound and protected/view-once payloads never download',async t=>{
 const a=await attachment();let calls=0;const f=await fixture(t,{photoFetch:async()=>{calls++;return response(a.encrypted);}}),s=await f.connect();
 const first=await insert(f,s,a.image,{chat:group,participant:PEER}),second=await insert(f,s,{...a.image,mediaKey:Buffer.alloc(32,8)},{chat:group,participant:other});
 assert.equal((await f.engine.request('photo',request(first))).state,'ready');assert.equal((await f.engine.request('photo',request(second))).state,'unavailable');
 await assert.rejects(f.engine.request('photo',{...request(first),participant:undefined}));
 s.ev.emit('messages.upsert',{messages:[fixtureMessage('ONCE','',{message:{viewOnceMessageV2:{message:{imageMessage:a.image}}}}),fixtureMessage('ONCE-FLAG','',{message:{imageMessage:{...a.image,viewOnce:true}}})]});await drain();assert.equal((await f.engine.request('messages',{chat:PEER})).messages.length,0);assert.equal(calls,2);
});
test('deletion, lock and expiry fence photo completion and delete its cached content',async t=>{
 const a=await attachment();let release;const f=await fixture(t,{photoFetch:()=>new Promise(r=>{release=r;})}),s=await f.connect(),row=await insert(f,s,a.image);
 const pending=f.engine.request('photo',request(row));await drain();s.ev.emit('messages.delete',{keys:[{remoteJid:PEER,id:row.id,fromMe:false}]});await drain();release(response(a.encrypted));assert.equal((await pending).state,'unavailable');
 const row2=await insert(f,s,a.image,{id:'LOCK'}),pending2=f.engine.request('photo',request(row2));await drain();const closing=f.engine.close();release(response(a.encrypted));await closing;assert.equal((await pending2).state,'unavailable');
 await f.reopen();const store=new Store(f.directory,f.config.key,{account:f.config.account,connection:f.config.connection});try{assert.equal(store.totals('photo').count,0);}finally{store.close();}
});
test('tampering, oversized ciphertext, hostile hosts and mismatched message versions cannot supply image bytes',async t=>{
 const a=await attachment(),bad=Buffer.from(a.encrypted);bad[0]^=1;
 assert.equal(photoURL('https://mmg.whatsapp.net.evil.test/a'),null);assert.equal(photoURL('http://127.0.0.1/a'),null);assert.equal(photoURL('https://user@mmg.whatsapp.net/a'),null);
 assert.equal(imageDescriptor({...a.image,fileLength:PHOTO_BYTES+1}),undefined);
 let calls=0;const f=await fixture(t,{photoFetch:async()=>{calls++;return response(bad);}}),s=await f.connect(),row=await insert(f,s,a.image);
 assert.equal((await f.engine.request('photo',{...request(row),expectedVersion:'0'.repeat(64)})).state,'unavailable');assert.equal(calls,0);
 assert.equal((await f.engine.request('photo',request(row))).state,'unavailable');assert.equal(calls,1);
});
test('older placeholders recover descriptors on history replay, and photo expiry follows the earlier message expiry',async t=>{
 const a=await attachment(),f=await fixture(t,{photoFetch:async()=>response(a.encrypted)}),s=await f.connect();
 let row=await insert(f,s,{caption:'Photo fixture'});assert.equal(row.photo.available,false);assert.equal((await f.engine.request('photo',request(row))).state,'refresh-required');
 let requested;s.requestPlaceholderResend=async key=>{requested=key;};assert.equal((await f.engine.request('photo',{...request(row),retry:true})).state,'waiting');assert.equal(requested.id,row.id);
 row=await insert(f,s,{...a.image,contextInfo:{expiration:2}});assert.equal(row.photo.available,true);assert.equal((await f.engine.request('photo',request(row))).state,'ready');
 const real=Date.now;Date.now=()=>real()+4000;try{assert.equal((await f.engine.request('photo',request(row))).state,'unavailable');assert.equal((await f.engine.request('messages',{chat:PEER})).messages.length,0);}finally{Date.now=real;}
});

test('oversized downloads stop before reading their body, and media renewal preserves authenticated identity',async t=>{
 const a=await attachment();let oversized=true;
 const f=await fixture(t,{photoFetch:async()=>oversized?new Response(a.encrypted,{headers:{'content-length':String(PHOTO_BYTES+100)}}):new Response(null,{status:404})}),s=await f.connect();
 const big=await insert(f,s,a.image,{id:'BIG'});assert.equal((await f.engine.request('photo',request(big))).state,'unavailable');oversized=false;
 const expired=await insert(f,s,a.image,{id:'RENEW'});let calls=0;s.updateMediaMessage=async message=>{calls++;assert.equal(message.key.id,'RENEW');return {message:{imageMessage:{...a.image,url:'https://mmg.whatsapp.net/v/t62.7118-24/renewed.enc'}}};};
 assert.equal((await f.engine.request('photo',{...request(expired),retry:true})).state,'waiting');assert.equal(calls,1);
});
test('photo cache eviction preserves authentication and message records within the combined content quota',async t=>{
 const { Inbox }=await import('../src/inbox.mjs'),f=await fixture(t);const store=new Store(f.directory,f.config.key,{account:f.config.account,connection:f.config.connection});
 try{store.set('wa-auth','fixture',{secret:'auth must survive'});const inbox=new Inbox(store,()=>{},{maxBytes:2048});store.set('photo','cache',{dataUrl:'x'.repeat(3000)},{expires:Date.now()+60000,created:1});inbox.trim();assert.equal(store.totals('photo').count,0);assert.equal(store.get('wa-auth','fixture').secret,'auth must survive');}finally{store.close();}
});
