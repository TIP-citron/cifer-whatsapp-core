import test from 'node:test';
import assert from 'node:assert/strict';
import { fixture, drain, PEER, turn } from './helpers.mjs';
import { avatarDataURL, avatarURL, Avatars, AVATAR_TTL } from '../src/avatars.mjs';
import { Store } from '../src/store.mjs';
import { crc32, deflateSync } from 'node:zlib';
const chunk=(type,data)=>{const body=Buffer.concat([Buffer.from(type),data]),size=Buffer.alloc(4),crc=Buffer.alloc(4);size.writeUInt32BE(data.length);crc.writeUInt32BE(crc32(body));return Buffer.concat([size,body,crc]);};
const PNG=Buffer.concat([Buffer.from([137,80,78,71,13,10,26,10]),chunk('IHDR',Buffer.from([0,0,0,1,0,0,0,1,8,6,0,0,0])),chunk('IDAT',deflateSync(Buffer.from([0,255,0,0,255]))),chunk('IEND',Buffer.alloc(0))]);
const URL='https://pps.whatsapp.net/v/preview.jpg?token=synthetic';
const response=()=>new Response(PNG,{headers:{'content-type':'image/png'}});
const until=async predicate=>{for(let i=0;i<60;i++){if(await predicate())return;await turn();}assert.fail('Synthetic profile task did not settle.');};
const picture=async f=>(await f.engine.request('chats')).chats.find(row=>row.id===PEER)?.avatar;

test('profile URLs and raster containers reject untrusted destinations, formats and oversized canvases',()=>{
  assert.equal(avatarURL(URL),URL);
  for(const value of ['http://pps.whatsapp.net/x','https://whatsapp.net.evil.test/x','https://127.0.0.1/x','https://a:password@pps.whatsapp.net/x','https://pps.whatsapp.net:444/x','file:///tmp/photo','https://pps.whatsapp.net/x#fragment'])assert.equal(avatarURL(value),null);
  assert.equal(avatarDataURL(PNG,'image/png'),`data:image/png;base64,${PNG.toString('base64')}`);
  for(const [bytes,mime]of [[PNG,'image/jpeg'],[Buffer.from('<svg/>'),'image/svg+xml'],[Buffer.alloc(32769),'image/png'],[PNG.subarray(0,30),'image/png']])assert.throws(()=>avatarDataURL(bytes,mime));
  const huge=Buffer.from(PNG);huge.writeUInt32BE(513,16);assert.throws(()=>avatarDataURL(huge,'image/png'));
  const webp=Buffer.alloc(30);webp.write('RIFF');webp.writeUInt32LE(22,4);webp.write('WEBPVP8 ',8);webp.writeUInt32LE(10,16);Buffer.from([157,1,42]).copy(webp,23);webp.writeUInt16LE(1,26);webp.writeUInt16LE(1,28);
  assert.ok(avatarDataURL(webp,'image/webp').startsWith('data:image/webp;base64,'));webp.writeUInt16LE(513,26);assert.throws(()=>avatarDataURL(webp,'image/webp'));
  const lossless=Buffer.alloc(26);lossless.write('RIFF');lossless.writeUInt32LE(18,4);lossless.write('WEBPVP8L',8);lossless.writeUInt32LE(5,16);lossless[20]=47;assert.ok(avatarDataURL(lossless,'image/webp'));
  const jpeg=Buffer.from([255,216,255,192,0,11,8,0,1,0,1,1,1,17,0,255,218,0,2,0,255,217]);
  assert.ok(avatarDataURL(jpeg,'image/jpeg').startsWith('data:image/jpeg;base64,'));jpeg.writeUInt16BE(513,9);assert.throws(()=>avatarDataURL(jpeg,'image/jpeg'));
});

test('known contact previews are fetched locally once, encrypted, and emit metadata-only updates',async t=>{
  let queries=0,downloads=0;
  const f=await fixture(t,{avatarFetch:async(url,options)=>{downloads++;assert.equal(url,URL);assert.equal(options.credentials,'omit');assert.equal(options.redirect,'error');assert.equal(options.referrerPolicy,'no-referrer');assert.equal(options.method,'GET');return response();}});
  await assert.rejects(f.engine.request('profiles',{peers:[PEER]}),/Reconnect/);
  const socket=await f.connect();socket.profilePictureUrl=async(peer,type,timeout)=>{queries++;assert.equal(peer,PEER);assert.equal(type,'preview');assert.equal(timeout,4000);return URL;};
  socket.ev.emit('contacts.upsert',[{id:PEER,name:'Synthetic contact'}]);await drain();f.events.length=0;
  await f.engine.request('profiles',{peers:[PEER,PEER,'15550000999@s.whatsapp.net']});await f.engine.request('profiles',{peers:[PEER]});
  await until(async()=>!!await picture(f));assert.equal(queries,1);assert.equal(downloads,1);assert.equal(f.events.filter(e=>e.type==='changed').length,0);assert.ok(f.events.some(e=>e.type==='metadata'&&e.chat===PEER));
  const original=await picture(f);await f.reopen();f.sockets.at(-1).ev.emit('connection.update',{connection:'open'});await f.engine.request('profiles',{peers:[PEER]});assert.equal(await picture(f),original);assert.equal(downloads,1);
  const {readFile}=await import('node:fs/promises');const raw=await readFile(`${f.directory}/whatsapp.sqlite`);assert.equal(raw.includes(Buffer.from(original)),false);assert.equal(raw.includes(PNG),false);
  await f.engine.disconnect();assert.equal((await f.engine.request('chats')).chats.length,0);
  const store=new Store(f.directory,f.config.key,{account:f.config.account,connection:f.config.connection});try{assert.equal(store.totals('avatar').count,0);}finally{store.close();}
});

test('photo removal fences an older download and negative cache suppresses retries',async t=>{
  let downloads=0,release;
  const f=await fixture(t,{avatarFetch:async()=>{downloads++;return downloads===2?new Promise(resolve=>{release=resolve;}):response();}}),socket=await f.connect();
  socket.profilePictureUrl=async()=>URL;socket.ev.emit('contacts.upsert',[{id:PEER}]);await drain();await f.engine.request('profiles',{peers:[PEER]});await until(async()=>!!await picture(f));
  socket.ev.emit('contacts.update',[{id:PEER,imgUrl:'changed'}]);await until(()=>downloads===2);assert.equal(await picture(f),null);
  socket.ev.emit('contacts.update',[{id:PEER,imgUrl:'removed'}]);await drain();release(response());await drain();
  assert.equal(await picture(f),null);await f.engine.request('profiles',{peers:[PEER]});assert.equal(downloads,2);
  const real=Date.now,now=real();Date.now=()=>now+600001;
  try{await f.engine.request('profiles',{peers:[PEER]});await until(async()=>!!await picture(f));assert.equal(downloads,3);}finally{Date.now=real;}
});

test('closing aborts profile downloads and late completion cannot recreate account data',async t=>{
  let release,signal;
  const f=await fixture(t,{avatarFetch:async(_url,options)=>{signal=options.signal;return new Promise(resolve=>{release=resolve;});}}),socket=await f.connect();
  socket.profilePictureUrl=async()=>URL;socket.ev.emit('contacts.upsert',[{id:PEER}]);await drain();await f.engine.request('profiles',{peers:[PEER]});await until(()=>!!release);
  await f.engine.close();assert.equal(signal.aborted,true);release(response());await drain();await f.reopen();assert.equal(await picture(f),null);
});

test('verified PN/LID merges preserve cached photo without keeping an alias copy',async t=>{
  const lid='1234567890@lid';let calls=0;
  const f=await fixture(t,{avatarFetch:async()=>{calls++;return response();}}),socket=await f.connect();socket.profilePictureUrl=async()=>URL;
  socket.ev.emit('contacts.upsert',[{id:lid}]);await drain();await f.engine.request('profiles',{peers:[lid]});await until(async()=>(await f.engine.request('chats')).chats.some(c=>c.id===lid&&c.avatar));
  socket.ev.emit('contacts.update',[{id:lid,phoneNumber:PEER}]);await drain();await f.engine.request('profiles',{peers:[lid,PEER]});
  const chats=(await f.engine.request('chats')).chats;assert.ok(chats.some(c=>c.id===PEER&&c.avatar));assert.ok(!chats.some(c=>c.id===lid));assert.equal(calls,1);
});

test('photo TTL is enforced on reads and bounded encrypted cache evicts oldest entries',async t=>{
  const f=await fixture(t);await f.engine.close();const store=new Store(f.directory,f.config.key,{account:f.config.account,connection:f.config.connection});t.after(()=>store.close());
  const events=[],avatars=new Avatars({store,socket:()=>null,live:()=>false,changed:chat=>events.push(chat),maxEntries:2,maxBytes:1000});
  const data=avatarDataURL(PNG,'image/png');for(let i=0;i<3;i++)avatars.save(`1555000000${i}@s.whatsapp.net`,data,AVATAR_TTL);
  assert.equal(store.totals('avatar').count,2);assert.equal(store.get('avatar','15550000000@s.whatsapp.net'),null);assert.ok(events.includes('15550000000@s.whatsapp.net'));
  const real=Date.now,now=real();Date.now=()=>now+AVATAR_TTL+1;
  try{assert.equal(store.get('avatar','15550000002@s.whatsapp.net'),null);store.prune();assert.equal(store.totals('avatar').count,0);}finally{Date.now=real;}
  const byteBound=new Avatars({store,socket:()=>null,live:()=>false,changed:()=>{},maxBytes:350});
  for(let i=0;i<4;i++)byteBound.save(`1555000001${i}@s.whatsapp.net`,data,AVATAR_TTL);
  assert.ok(store.totals('avatar').bytes<=350);assert.equal(store.totals('avatar').count,1);
});

test('profile jobs cap concurrency and reject hostile download responses without exposing bytes',async t=>{
  let active=0,max=0,queries=0,downloads=0;const releases=[];
  const f=await fixture(t,{avatarFetch:async()=>{downloads++;return new Response('<svg>bad</svg>',{headers:{'content-type':'image/svg+xml'}});}}),socket=await f.connect();
  socket.profilePictureUrl=async()=>{const index=++queries;active++;max=Math.max(max,active);await new Promise(resolve=>releases.push(resolve));active--;return index===1?'https://evil.test/photo':URL;};
  const peers=Array.from({length:5},(_,i)=>`1555000001${i}@s.whatsapp.net`);socket.ev.emit('contacts.upsert',peers.map(id=>({id})));await drain();await f.engine.request('profiles',{peers});await until(()=>releases.length===2);assert.equal(max,2);
  for(let i=0;i<5;i++){await until(()=>!!releases[i]);releases[i]();await drain();}
  await drain();assert.equal(queries,5);assert.equal(downloads,4);assert.ok((await f.engine.request('chats')).chats.every(c=>c.avatar===null));await f.engine.request('profiles',{peers});assert.equal(queries,5);
});

test('untrusted response lengths and streamed overflow are cancelled before reaching IPC or cache',async t=>{
  let cancel=0,downloads=0;
  const f=await fixture(t,{avatarFetch:async()=>{
    downloads++;return new Response(new ReadableStream({start(controller){controller.enqueue(new Uint8Array(32769));},cancel(){cancel++;}}),{headers:{'content-type':'image/png',...(downloads===1?{'content-length':'32769'}:{})}});
  }}),socket=await f.connect();socket.profilePictureUrl=async()=>URL;
  const peers=[PEER,'15550000003@s.whatsapp.net'];socket.ev.emit('contacts.upsert',peers.map(id=>({id})));await drain();await f.engine.request('profiles',{peers});await until(()=>cancel===2);await drain();
  assert.ok((await f.engine.request('chats')).chats.every(c=>c.avatar===null));await f.engine.request('profiles',{peers});assert.equal(downloads,2);
});
