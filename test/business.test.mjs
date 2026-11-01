import test from 'node:test';
import assert from 'node:assert/strict';
import { proto } from 'baileys';
import { extract } from '../src/content.mjs';
import { fixture, fixtureMessage, drain, PEER } from './helpers.mjs';
const image={url:'https://mmg.whatsapp.net/fixture.enc',mediaKey:Buffer.alloc(32,1),fileSha256:Buffer.alloc(32,2),fileEncSha256:Buffer.alloc(32,3),fileLength:100,mimetype:'image/jpeg'};
const formats=[
 {templateMessage:{hydratedTemplate:{hydratedContentText:'Synthetic business offer',hydratedFooterText:'Reply STOP',imageMessage:image}}},
 {templateMessage:{hydratedFourRowTemplate:{hydratedContentText:'Synthetic business offer',hydratedFooterText:'Reply STOP',imageMessage:image}}},
 {buttonsMessage:{contentText:'Synthetic business offer',footerText:'Reply STOP',imageMessage:image}},
 {interactiveMessage:{header:{imageMessage:image},body:{text:'Synthetic business offer'},footer:{text:'Reply STOP'},nativeFlowMessage:{buttons:[{name:'cta_url',buttonParamsJson:'{"url":"https://invalid.test"}'}]}}}
];
for(const [i,body]of formats.entries())test(`business format ${i} renders text/footer/photo from protobuf upsert and history`,async t=>{
 const f=await fixture(t),s=await f.connect();
 const decoded=proto.WebMessageInfo.decode(proto.WebMessageInfo.encode(fixtureMessage('BIZ','',{message:body})).finish());
 s.ev.emit('messages.upsert',{messages:[decoded]});await drain();
 let row=(await f.engine.request('messages',{chat:PEER})).messages[0];assert.ok(row);assert.equal(row.text,'Synthetic business offer\n\nReply STOP');assert.deepEqual(row.photo,{available:true});assert.equal(row.editUntil,0);assert.ok(!JSON.stringify(row).includes('mediaKey'));assert.ok(!JSON.stringify(row).includes('invalid.test'));
 await f.reopen();row=(await f.engine.request('messages',{chat:PEER})).messages[0];assert.equal(row.text,'Synthetic business offer\n\nReply STOP');
});
test('interactive transport envelope is visible, actual view-once and hidden linked-device content remain excluded',()=>{
 for(const wrapper of ['viewOnceMessage','viewOnceMessageV2','viewOnceMessageV2Extension']){
  assert.equal(extract({[wrapper]:{message:formats[3]}})?.text,'Synthetic business offer\n\nReply STOP');
  assert.equal(extract({[wrapper]:{message:{imageMessage:image}}}),null);
  assert.equal(extract({[wrapper]:{message:{interactiveMessage:{...formats[3].interactiveMessage,header:{imageMessage:{...image,viewOnce:true}}}}}}),null);
 }
 assert.equal(extract({viewOnceMessageV2:{message:{...formats[3],imageMessage:image}}}),null);
 assert.equal(extract({templateMessage:{hydratedTemplate:{...formats[0].templateMessage.hydratedTemplate,maskLinkedDevices:true}}}),null);
});
test('business content stays bounded, expires and unknown structured cards get a placeholder',async t=>{
 assert.equal(extract({templateMessage:{fourRowTemplate:{content:{namespace:'unused'}}}})?.text,'Business message · open in WhatsApp');
 assert.equal(extract({interactiveMessage:{body:{text:'x'.repeat(16001)}}})?.text,'Long message · open in WhatsApp');
 const f=await fixture(t),s=await f.connect();s.ev.emit('messages.upsert',{messages:[fixtureMessage('EXPIRES','',{message:{interactiveMessage:{...formats[3].interactiveMessage,contextInfo:{expiration:2}}}})]});await drain();
 const real=Date.now;Date.now=()=>real()+3000;try{assert.equal((await f.engine.request('messages',{chat:PEER})).messages.length,0);}finally{Date.now=real;}
});
