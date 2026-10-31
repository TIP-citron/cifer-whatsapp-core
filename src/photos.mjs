import { createDecipheriv, createHash, createHmac, timingSafeEqual } from 'node:crypto';
import { getMediaKeys } from 'baileys';
import { assertChat, exact, isGroup, messageId, messageVersion, peerPattern } from './content.mjs';
import { PHOTO_BYTES, PHOTO_PIXELS, photoURL, imageDescriptor } from './photo-content.mjs';
import { rasterDataURL } from './avatars.mjs';
const unavailable = () => ({state:'unavailable'});
const sha = bytes => createHash('sha256').update(bytes).digest();
export class Photos {
  constructor(engine,fetcher=globalThis.fetch){this.engine=engine;this.store=engine.store;this.fetcher=fetcher;this.pending=new Map();this.cooldown=new Map();this.queue=[];this.running=0;}
  pause(){for(const job of this.pending.values()){job.abort.abort();job.finish(unavailable());}this.pending.clear();this.queue=[];this.cooldown.clear();}
  forget(key){const job=this.pending.get(messageId(key));if(job){job.abort.abort();job.finish(unavailable());this.pending.delete(job.id);}}
  row(job){if(!this.engine.current(job.epoch)||job.abort.signal.aborted)return null;const row=this.store.get('message',job.id);return row&&row.kind!=='protected'&&row.label==='Photo · open in WhatsApp'&&row.expiresAt>Date.now()&&messageVersion(row)===job.version?row:null;}
  async request(args){
    exact(args,['chat','messageId','fromMe','expectedVersion'],['participant','retry']);
    if(typeof args.fromMe!=='boolean'||!/^[-\w]{1,128}$/.test(args.messageId||'')||!(/^[a-f0-9]{64}$/).test(args.expectedVersion||'')||args.retry!==undefined&&typeof args.retry!=='boolean')throw Error('Invalid photo request.');
    const epoch=this.engine.epoch,chat=await this.engine.canonical(assertChat(args.chat));
    if(!this.engine.current(epoch))return unavailable();
    if((isGroup(chat)||args.participant!==undefined)&&!peerPattern.test(args.participant||''))throw Error('A group photo requires its sender.');
    const participant=args.participant?await this.engine.canonical(args.participant):undefined;
    if(!this.engine.current(epoch))return unavailable();
    const id=messageId({chat,id:args.messageId,fromMe:args.fromMe,participant}),version=args.expectedVersion;
    const probe={id,version,epoch,abort:new AbortController()},row=this.row(probe);if(!row)return unavailable();
    const cached=this.store.get('photo',id);if(cached?.version===version)return {state:'ready',dataUrl:cached.dataUrl,expiresAt:Math.min(cached.expiresAt,row.expiresAt)};
    if(this.engine.state!=='connected'||!this.engine.socket)return {state:'offline'};
    if(this.pending.has(id)){const old=this.pending.get(id);if(old.version===version)return old.promise;old.abort.abort();old.finish(unavailable());this.pending.delete(id);}
    if(this.pending.size>=16)return {state:'busy'};
    if((this.cooldown.get(JSON.stringify([id,version]))||0)>Date.now())return unavailable();
    if(this.cooldown.size>=256)this.cooldown.delete(this.cooldown.keys().next().value);
    let finish;const promise=new Promise(resolve=>{finish=resolve;});
    const job={...probe,row,finish,promise,retry:args.retry===true,socket:this.engine.socket};
    this.pending.set(id,job);this.queue.push(job);this.drain();return promise;
  }
  drain(){while(this.running<2&&this.queue.length){const job=this.queue.shift();if(this.pending.get(job.id)!==job)continue;this.running++;
    const task=this.load(job).catch(()=>unavailable()).then(value=>{if(value.state==='unavailable'&&this.engine.current(job.epoch))this.cooldown.set(JSON.stringify([job.id,job.version]),Date.now()+15000);job.finish(value);}).finally(()=>{if(this.pending.get(job.id)===job)this.pending.delete(job.id);this.running--;this.engine.inflight.delete(task);this.drain();});this.engine.inflight.add(task);
  }}
  async load(job){
    let row=this.row(job);if(!row)return unavailable();
    if(!row.image){
      // Older app versions intentionally discarded attachment keys. Ask the phone
      // only on explicit refresh; never invent a URL or revive an expired message.
      if(job.retry&&typeof job.socket.requestPlaceholderResend==='function'){
        let timer;try{await Promise.race([job.socket.requestPlaceholderResend(this.engine.protocolKey(row)),new Promise(resolve=>{timer=setTimeout(resolve,5000);})]);}finally{clearTimeout(timer);}
        return this.row(job)?{state:'waiting'}:unavailable();
      }
      return {state:'refresh-required'};
    }
    const descriptor=row.image,url=photoURL(descriptor.url);if(!url||descriptor.length>PHOTO_BYTES)return unavailable();
    const timer=setTimeout(()=>job.abort.abort(),20000),chunks=[];let encrypted,plain;
    try{
      const response=await this.fetcher(url,{method:'GET',credentials:'omit',redirect:'error',cache:'no-store',referrerPolicy:'no-referrer',signal:job.abort.signal,headers:{Accept:'application/octet-stream'}});
      const limit=(Math.floor(descriptor.length/16)+1)*16+10,declared=response.headers.get('content-length');
      if(this.row(job)&&[404,410].includes(response.status)&&job.retry&&typeof job.socket.updateMediaMessage==='function'){
        await response.body?.cancel();
        // Explicit retry may ask WhatsApp to renew its expired CDN locator. The
        // sender's authenticated content hash/key must remain exactly the same.
        let retryTimer;
        try{
          const image={url:descriptor.url,mimetype:descriptor.mime,fileLength:descriptor.length,...Object.fromEntries(['mediaKey','fileSha256','fileEncSha256'].map(k=>[k,Buffer.from(descriptor[k],'hex')]))};
          const renewed=await Promise.race([job.socket.updateMediaMessage({key:this.engine.protocolKey(row),message:{imageMessage:image}}),new Promise(resolve=>{retryTimer=setTimeout(()=>resolve(null),5000);})]);
          const next=imageDescriptor(renewed?.message?.imageMessage),current=this.row(job);
          if(current&&next&&['mime','length','mediaKey','fileSha256','fileEncSha256'].every(k=>next[k]===descriptor[k])){this.engine.inbox.save({...current,image:next});return {state:'waiting'};}
        }finally{clearTimeout(retryTimer);}
        return unavailable();
      }
      if(!this.row(job)||response.status!==200||!response.body||declared!==null&&(!/^\d+$/.test(declared)||Number(declared)!==limit)){await response.body?.cancel();return unavailable();}
      const reader=response.body.getReader();let length=0;
      try{for(;;){const {done,value}=await reader.read();if(done)break;length+=value.byteLength;if(length>limit||!this.row(job))throw Error('Photo no longer available.');chunks.push(Buffer.from(value));}}
      finally{await reader.cancel().catch(()=>{});}
      if(length!==limit||!this.row(job))return unavailable();
      encrypted=Buffer.concat(chunks);
      if(!timingSafeEqual(sha(encrypted),Buffer.from(descriptor.fileEncSha256,'hex')))return unavailable();
      const keys=await getMediaKeys(Buffer.from(descriptor.mediaKey,'hex'),'image'),ciphertext=encrypted.subarray(0,-10);
      try{
        const mac=createHmac('sha256',keys.macKey).update(keys.iv).update(ciphertext).digest().subarray(0,10);
        if(!timingSafeEqual(mac,encrypted.subarray(-10)))return unavailable();
        const cipher=createDecipheriv('aes-256-cbc',keys.cipherKey,keys.iv);plain=Buffer.concat([cipher.update(ciphertext),cipher.final()]);
      }finally{for(const key of Object.values(keys))key.fill(0);}
      if(plain.length!==descriptor.length||!timingSafeEqual(sha(plain),Buffer.from(descriptor.fileSha256,'hex')))return unavailable();
      const dataUrl=rasterDataURL(plain,descriptor.mime,{maxBytes:PHOTO_BYTES,maxDimension:8192,maxPixels:PHOTO_PIXELS});
      row=this.row(job);if(!row)return unavailable();
      const expiresAt=row.expiresAt;
      this.store.transaction(()=>{this.store.set('photo',job.id,{version:job.version,dataUrl,expiresAt},{expires:expiresAt,created:Date.now(),tag:row.chat});this.engine.inbox.trim();});
      return this.row(job)?{state:'ready',dataUrl,expiresAt}:unavailable();
    }finally{clearTimeout(timer);job.abort.abort();for(const chunk of chunks)chunk.fill(0);encrypted?.fill(0);plain?.fill(0);}
  }
}
