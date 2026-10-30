import { EventEmitter } from 'node:events';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { createEngine } from '../src/index.mjs';
export const ACCOUNT='10000000-0000-4000-8000-000000000001',PEER='15550000001@s.whatsapp.net',SELF='15550000002@s.whatsapp.net';
export const turn=()=>new Promise(resolve=>setImmediate(resolve));
export async function drain(){await turn();await turn();}
export function wire({account=ACCOUNT,version=2,contact=null,createdAt=Date.now()}={}) {
  const context={version,id:randomUUID(),sender:account,...(version===2?{contact}:{target:PEER}),createdAt};
  return `CIFER:WA:${version}:${Buffer.from(JSON.stringify({context,sealed:{nonce:'01'.repeat(12),ciphertext:'02'.repeat(32)}})).toString('base64')}\nOpen in CIFER: https://hub.cifer.network/whatsapp/${version===2?'extension/':''}message/${context.id}`;
}
export function fixtureMessage(id,text='synthetic text',overrides={}){return {key:{id,remoteJid:PEER,fromMe:false},messageTimestamp:Math.floor(Date.now()/1000),message:{conversation:text},...overrides};}
export async function fixture(t,options={}){
  const directory=await mkdtemp(join(tmpdir(),'cifer-wa-core-')),events=[],sockets=[];
  const factory=config=>{
    const socket={config,ev:new EventEmitter(),user:{id:SELF},sent:[],history:[],ended:false,loggedOut:false,
      signalRepository:{lidMapping:{getPNForLID:async()=>null}},
      async requestPairingCode(){return 'ABCD1234';},
      async sendMessage(peer,content,opts){this.sent.push({peer,content,options:opts});return fixtureMessage(opts.messageId,content.text,{key:{id:opts.messageId,remoteJid:peer,fromMe:true},status:2});},
      async fetchMessageHistory(count,key,at){const id=randomUUID();this.history.push({id,count,key,at});return id;},
      async logout(){this.loggedOut=true;},end(){this.ended=true;}
    };sockets.push(socket);return socket;
  };
  const config={directory,key:Buffer.alloc(32,9),account:ACCOUNT,connection:'fixture-link',onEvent:e=>events.push(e),socketFactory:factory,...options};
  let engine=await createEngine(config);
  t.after(async()=>{await engine.close();await rm(directory,{recursive:true,force:true});});
  return {directory,events,sockets,config,get engine(){return engine;},async reopen(){await engine.close();engine=await createEngine(config);return engine;},async connect(){await engine.request('pair');const socket=sockets.at(-1);socket.ev.emit('connection.update',{connection:'open'});await drain();return socket;}};
}
