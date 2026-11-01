import { createDecipheriv, createHash, createHmac, randomUUID, timingSafeEqual } from 'node:crypto';
import { getMediaKeys } from 'baileys';
import { assertChat, exact, isGroup, messageId, messageVersion, peerPattern } from './content.mjs';
import { MEDIA_CHUNK, MEDIA_BYTES, validMediaHeader, mediaDescriptor } from './media-content.mjs';
import { photoURL } from './photo-content.mjs';
import { mediaBudget } from './storage-policy.mjs';

const unavailable = () => ({state:'unavailable'});
/** Only authenticated chunks of a completely verified attachment can be read. */
export class Media {
  constructor(engine, fetcher = globalThis.fetch) {
    this.engine=engine;this.store=engine.store;this.fetcher=fetcher;this.jobs=new Map();this.leases=new Map();this.expired=new Set();
    for(const {id,value}of this.store.list('media-cache',{now:0}))if(!value.ready||value.expiresAt<=Date.now())this.remove(id);
    this.trim();
  }
  remove(id) {
    this.store.transaction(()=>{for(const {id:chunk}of this.store.list('media-chunk',{tag:id,now:0,limit:2048}))this.store.remove('media-chunk',chunk);this.store.remove('media-cache',id);});
    for(const [token,lease]of this.leases)if(lease.id===id)this.leases.delete(token);
  }
  trim(reserve=0) {
    let bytes=this.store.totals('media-chunk').bytes+this.store.totals('photo').bytes;
    for(const {id}of this.store.list('photo',{oldest:true})){if(bytes+reserve<=mediaBudget(this.store))break;this.store.remove('photo',id);bytes=this.store.totals('media-chunk').bytes+this.store.totals('photo').bytes;}
    for(const {id,value}of this.store.list('media-cache',{oldest:true,now:0})) {
      if(value.expiresAt<=Date.now()||bytes+reserve>mediaBudget(this.store)&&!this.jobs.has(value.message)&&![...this.leases.values()].some(l=>l.id===id&&l.until>Date.now())){
        this.remove(id);bytes=this.store.totals('media-chunk').bytes+this.store.totals('photo').bytes;
      }
    }
    return bytes+reserve<=mediaBudget(this.store);
  }
  pause() { for(const job of this.jobs.values())job.abort.abort();this.leases.clear();this.expired.clear(); }
  clear() { this.pause();this.store.transaction(()=>{this.store.clear('media-cache');this.store.clear('media-chunk');this.store.clear('photo');}); }
  forget(key) {
    const message=messageId(key);this.jobs.get(message)?.abort.abort();
    for(const {id}of this.store.list('media-cache',{tag:message,now:0}))this.remove(id);
  }
  row(job) {
    if(!this.engine.current(job.epoch)||job.abort?.signal.aborted)return null;
    const row=this.store.get('message',job.message);
    return row&&row.kind!=='protected'&&row.expiresAt>Date.now()&&messageVersion(row)===job.version?row:null;
  }
  async target(args) {
    exact(args,['chat','messageId','fromMe','expectedVersion'],['participant','retry']);
    if(typeof args.fromMe!=='boolean'||!/^[-\w]{1,128}$/.test(args.messageId||'')||!(/^[a-f0-9]{64}$/).test(args.expectedVersion||'')||args.retry!==undefined&&typeof args.retry!=='boolean')throw Error('Invalid media request.');
    const epoch=this.engine.epoch,chat=await this.engine.canonical(assertChat(args.chat));
    if(!this.engine.current(epoch))return null;
    if((isGroup(chat)||args.participant!==undefined)&&!peerPattern.test(args.participant||''))throw Error('A group attachment requires its sender.');
    const participant=args.participant?await this.engine.canonical(args.participant):undefined;
    const job={epoch,message:messageId({chat,id:args.messageId,fromMe:args.fromMe,participant}),version:args.expectedVersion,abort:new AbortController()};
    return this.row(job)?job:null;
  }
  lease(job, cache) {
    const row=this.row(job);if(!row||!cache?.ready||cache.version!==job.version)return unavailable();
    for(const [token,lease]of this.leases)if(lease.until<=Date.now())this.leases.delete(token);
    if(this.leases.size>=16)return {state:'busy'};
    const token=randomUUID(),until=Math.min(row.expiresAt,Date.now()+3600000);
    this.leases.set(token,{...job,id:cache.id,until});
    this.store.set('media-cache',cache.id,cache,{tag:job.message,created:Date.now(),expires:cache.expiresAt});
    return {state:'ready',token,mime:cache.mime,size:cache.size,expiresAt:until};
  }
  async open(args) {
    const job=await this.target(args);if(!job)return unavailable();
    let row=this.row(job);const cached=this.store.list('media-cache',{tag:job.message}).find(r=>r.value.ready&&r.value.version===job.version)?.value;
    if(cached)return this.lease(job,cached);
    if(this.engine.state!=='connected'||!this.engine.socket)return {state:'offline'};
    if(!row.media) {
      if(args.retry&&this.engine.socket.requestPlaceholderResend){
        let timer;try{await Promise.race([this.engine.socket.requestPlaceholderResend(this.engine.protocolKey(row)),new Promise(resolve=>{timer=setTimeout(resolve,5000);})]);}finally{clearTimeout(timer);}
        return this.row(job)?{state:'waiting'}:unavailable();
      }
      return {state:'refresh-required'};
    }
    if(args.retry&&this.expired.has(job.message)&&typeof this.engine.socket.updateMediaMessage==='function'){
      const descriptor=row.media,type=descriptor.type==='audio'?'audioMessage':descriptor.type==='video'?'videoMessage':'stickerMessage';let timer;
      try{
        const payload={url:descriptor.url,mimetype:descriptor.mime,fileLength:descriptor.length,...Object.fromEntries(['mediaKey','fileSha256','fileEncSha256'].map(k=>[k,Buffer.from(descriptor[k],'hex')]))};
        const renewed=await Promise.race([this.engine.socket.updateMediaMessage({key:this.engine.protocolKey(row),message:{[type]:payload}}),new Promise(resolve=>{timer=setTimeout(()=>resolve(null),5000);})]);
        const next=mediaDescriptor(renewed?.message||{}),current=this.row(job);
        if(!current||!next||!['mime','length','mediaKey','fileSha256','fileEncSha256'].every(k=>next[k]===descriptor[k]))return unavailable();
        row={...current,media:{...descriptor,url:next.url}};this.engine.inbox.save(row,true);this.expired.delete(job.message);
      }catch{return unavailable();}finally{clearTimeout(timer);}
    }
    let pending=this.jobs.get(job.message);
    if(pending&&pending.version!==job.version){pending.abort.abort();return {state:'busy'};}
    if(!pending){
      if(this.jobs.size>=2)return {state:'busy'};
      // Reserve encrypted/base64 record overhead for every concurrent download.
      const reserve=Math.ceil(row.media.length*1.34)+Math.ceil(row.media.length/MEDIA_CHUNK)*1024;
      if(!this.trim(reserve+[...this.jobs.values()].reduce((n,j)=>n+j.reserve,0)))return {state:'storage-full'};
      pending={...job,reserve};this.jobs.set(job.message,pending);
      pending.promise=this.download(pending,row.media).catch(()=>null).finally(()=>{if(this.jobs.get(job.message)===pending)this.jobs.delete(job.message);this.engine.inflight.delete(pending.promise);});
      this.engine.inflight.add(pending.promise);
    }
    const cache=await pending.promise;return cache?this.lease(job,cache):unavailable();
  }
  async download(job, descriptor) {
    const url=photoURL(descriptor.url);if(!url||descriptor.length>MEDIA_BYTES)return null;
    const id=randomUUID(),row=this.row(job);if(!row)return null;
    const cache={id,message:job.message,version:job.version,mime:descriptor.mime,size:descriptor.length,expiresAt:row.expiresAt,ready:false};
    this.store.set('media-cache',id,cache,{tag:job.message,created:Date.now(),expires:row.expiresAt});
    const timer=setTimeout(()=>job.abort.abort(),120000);let keys,reader,carry=Buffer.alloc(0),pending=Buffer.alloc(0),head=Buffer.alloc(0),index=0,length=0,plainLength=0,success=false;
    const expected=(Math.floor(descriptor.length/16)+1)*16+10;
    const encHash=createHash('sha256'),plainHash=createHash('sha256');
    const persist=chunk=>{
      if(!this.row(job))throw Error('Media retired.');
      this.store.set('media-chunk',`${id}:${index++}`,{bytes:chunk.toString('base64')},{tag:id,expires:cache.expiresAt});
    };
    const accept=bytes=>{
      plainLength+=bytes.length;if(plainLength>descriptor.length)throw Error('Media size changed.');
      plainHash.update(bytes);if(head.length<64){const next=Buffer.concat([head,bytes.subarray(0,64-head.length)]);head.fill(0);head=next;}
      const joined=Buffer.concat([pending,bytes]);pending.fill(0);bytes.fill(0);let offset=0;
      while(joined.length-offset>=MEDIA_CHUNK){persist(joined.subarray(offset,offset+MEDIA_CHUNK));offset+=MEDIA_CHUNK;}
      pending=Buffer.from(joined.subarray(offset));joined.fill(0);
    };
    try {
      keys=await getMediaKeys(Buffer.from(descriptor.mediaKey,'hex'),descriptor.type==='sticker'?'image':descriptor.type);
      if(!this.row(job))return null;
      const response=await this.fetcher(url,{credentials:'omit',redirect:'error',cache:'no-store',referrerPolicy:'no-referrer',signal:job.abort.signal});
      const declared=response.headers.get('content-length');
      if([404,410].includes(response.status)){if(this.expired.size>=256)this.expired.delete(this.expired.values().next().value);this.expired.add(job.message);}
      if(response.status!==200||!response.body||declared!==null&&(!/^\d+$/.test(declared)||Number(declared)!==expected)){await response.body?.cancel();return null;}
      const mac=createHmac('sha256',keys.macKey).update(keys.iv),cipher=createDecipheriv('aes-256-cbc',keys.cipherKey,keys.iv);
      reader=response.body.getReader();
      for(;;){
        const {done,value}=await reader.read();if(done)break;
        if(!this.row(job)||length+value.length>expected)throw Error('Media retired or oversized.');
        length+=value.length;encHash.update(value);
        const joined=Buffer.concat([carry,value]);carry.fill(0);
        const end=Math.max(0,joined.length-10),part=joined.subarray(0,end);
        if(part.length){mac.update(part);accept(cipher.update(part));}
        carry=Buffer.from(joined.subarray(end));joined.fill(0);
      }
      if(length!==expected||carry.length!==10||!timingSafeEqual(encHash.digest(),Buffer.from(descriptor.fileEncSha256,'hex'))||!timingSafeEqual(mac.digest().subarray(0,10),carry))return null;
      accept(cipher.final());
      if(plainLength!==descriptor.length||!timingSafeEqual(plainHash.digest(),Buffer.from(descriptor.fileSha256,'hex'))||!validMediaHeader(head,descriptor.mime)||!this.row(job))return null;
      if(pending.length)persist(pending);
      cache.ready=true;cache.chunks=index;
      this.store.set('media-cache',id,cache,{tag:job.message,created:Date.now(),expires:cache.expiresAt});this.store.checkpoint();success=true;return cache;
    } finally {
      clearTimeout(timer);if(!success)job.abort.abort();await reader?.cancel().catch(()=>{});
      carry.fill(0);pending.fill(0);head.fill(0);for(const key of Object.values(keys||{}))key.fill(0);
      if(!success)this.remove(id);
    }
  }
  read(args) {
    exact(args,['token','offset','length']);
    if(typeof args.token!=='string'||!Number.isSafeInteger(args.offset)||args.offset<0||!Number.isSafeInteger(args.length)||args.length<1||args.length>MEDIA_CHUNK)throw Error('Invalid media range.');
    const lease=this.leases.get(args.token),row=lease&&this.row(lease),cache=lease&&this.store.get('media-cache',lease.id);
    if(!lease||lease.until<=Date.now()||!row||!cache?.ready||args.offset>=cache.size)throw Error('Media unavailable.');
    const index=Math.floor(args.offset/MEDIA_CHUNK),offset=args.offset%MEDIA_CHUNK;
    const chunk=this.store.get('media-chunk',`${lease.id}:${index}`);if(!chunk)throw Error('Media unavailable.');
    const bytes=Buffer.from(chunk.bytes,'base64');try{return {bytes:bytes.subarray(offset,Math.min(bytes.length,offset+args.length)).toString('base64')};}finally{bytes.fill(0);}
  }
  async played(args) {
    exact(args,['token']);const lease=this.leases.get(args.token),row=lease&&this.row(lease),socket=this.engine.socket;
    if(!row||lease.until<=Date.now()||row.fromMe||!row.media?.voice||lease.played||typeof socket?.fetchPrivacySettings!=='function'||typeof socket?.sendReceipts!=='function')return {ok:false};
    lease.played=true;
    const privacy=await socket.fetchPrivacySettings();
    if(!this.row(lease)||this.engine.socket!==socket||privacy?.readreceipts!=='all')return {ok:false};
    await socket.sendReceipts([this.engine.protocolKey(row)],'played');return {ok:true};
  }
  close(args) { exact(args,['token']);if(typeof args.token!=='string')throw Error('Invalid media token.');this.leases.delete(args.token);return {ok:true}; }
}
