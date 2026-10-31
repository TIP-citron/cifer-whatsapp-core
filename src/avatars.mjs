export const AVATAR_BYTES = 32768;
export const AVATAR_DIMENSION = 512;
export const AVATAR_TTL = 3600000;
const NEGATIVE_TTL = 600000, ERROR_TTL = 60000;
const MIME = new Set(['image/jpeg', 'image/png', 'image/webp']);

/** Only WhatsApp-issued preview hosts, never renderer-provided URLs or redirects. */
export function avatarURL(value) {
  if (typeof value !== 'string' || value.length > 2048) return null;
  try {
    const url = new URL(value);
    return url.protocol === 'https:' && !url.username && !url.password && !url.port && !url.hash
      && (url.hostname.endsWith('.whatsapp.net') || url.hostname.endsWith('.whatsapp.com')) ? url.href : null;
  } catch { return null; }
}

/** Small raster containers only. No SVG, animation, HTML, external resources or oversized canvases. */
export function avatarDataURL(input, mime) { return rasterDataURL(input,mime,{maxBytes:AVATAR_BYTES,maxDimension:AVATAR_DIMENSION,maxPixels:AVATAR_DIMENSION**2}); }
export function rasterDataURL(input, mime, {maxBytes,maxDimension,maxPixels}) {
  const dimensions = (width,height) => Number.isSafeInteger(width)&&Number.isSafeInteger(height)&&width>0&&height>0&&width<=maxDimension&&height<=maxDimension&&width*height<=maxPixels;
  const bytes = Buffer.from(input);
  if (!MIME.has(mime) || !bytes.length || bytes.length > maxBytes) throw Error('Invalid profile image.');
  let width = 0, height = 0;
  if (mime === 'image/png') {
    if (bytes.length < 45 || !bytes.subarray(0, 8).equals(Buffer.from([137,80,78,71,13,10,26,10]))) throw Error('Invalid profile image.');
    let offset = 8, ended = false, image = false;
    while (offset + 12 <= bytes.length) {
      const size = bytes.readUInt32BE(offset), type = bytes.toString('ascii', offset + 4, offset + 8);
      if (size > bytes.length - offset - 12 || ['acTL','fcTL','fdAT'].includes(type) || type==='IHDR'&&offset!==8) throw Error('Invalid profile image.');
      if (offset === 8) {
        if (type !== 'IHDR' || size !== 13) throw Error('Invalid profile image.');
        width = bytes.readUInt32BE(offset + 8); height = bytes.readUInt32BE(offset + 12);
      }
      if (type === 'IDAT') image = true;
      offset += 12 + size;
      if (type === 'IEND') { ended = size === 0 && offset === bytes.length; break; }
    }
    if (!ended || !image) throw Error('Invalid profile image.');
  } else if (mime === 'image/jpeg') {
    if (bytes.length < 16 || bytes.readUInt16BE(0) !== 0xffd8 || bytes.readUInt16BE(bytes.length - 2) !== 0xffd9) throw Error('Invalid profile image.');
    let offset = 2, scan = false;
    while (offset + 4 <= bytes.length) {
      if (bytes[offset++] !== 255) throw Error('Invalid profile image.');
      while (bytes[offset] === 255) offset++;
      const marker = bytes[offset++];
      if (marker === 217) break;
      if (marker === 1 || marker >= 208 && marker <= 215) continue;
      if (offset + 2 > bytes.length) throw Error('Invalid profile image.');
      const size = bytes.readUInt16BE(offset);
      if (size < 2 || size > bytes.length - offset) throw Error('Invalid profile image.');
      if ([192,193,194].includes(marker)) {
        if (size < 8 || width || height) throw Error('Invalid profile image.');
        height = bytes.readUInt16BE(offset + 3); width = bytes.readUInt16BE(offset + 5);
        if(!dimensions(width,height))throw Error('Invalid profile image.');
      }
      if (marker === 218) { scan = true; break; }
      offset += size;
    }
    if (!scan) throw Error('Invalid profile image.');
  } else {
    if (bytes.length < 26 || bytes.toString('ascii',0,4) !== 'RIFF' || bytes.readUInt32LE(4) !== bytes.length - 8 || bytes.toString('ascii',8,12) !== 'WEBP') throw Error('Invalid profile image.');
    let offset = 12, frame = false;
    while (offset + 8 <= bytes.length) {
      const type = bytes.toString('ascii',offset,offset+4), size = bytes.readUInt32LE(offset+4), start = offset+8;
      if (size > bytes.length - start || type === 'ANIM' || type === 'ANMF') throw Error('Invalid profile image.');
      let w = 0, h = 0;
      if (type === 'VP8X') {
        if (size !== 10 || bytes[start] & 2) throw Error('Invalid profile image.');
        width = bytes.readUIntLE(start+4,3)+1; height = bytes.readUIntLE(start+7,3)+1;
      } else if (type === 'VP8 ') {
        if (size < 10 || !bytes.subarray(start+3,start+6).equals(Buffer.from([157,1,42]))) throw Error('Invalid profile image.');
        w = bytes.readUInt16LE(start+6)&16383; h = bytes.readUInt16LE(start+8)&16383; frame = true;
      } else if (type === 'VP8L') {
        if (size < 5 || bytes[start] !== 47) throw Error('Invalid profile image.');
        const bits = bytes.readUInt32LE(start+1); w = (bits&16383)+1; h = ((bits>>>14)&16383)+1; frame = true;
      }
      if (w) { if (width && (width !== w || height !== h)) throw Error('Invalid profile image.'); width=w; height=h; }
      offset = start+size+(size&1);
    }
    if (!frame || offset !== bytes.length) throw Error('Invalid profile image.');
  }
  if (!dimensions(width,height)) throw Error('Invalid profile image.');
  return `data:${mime};base64,${bytes.toString('base64')}`;
}

export class Avatars {
  constructor({store, socket, live, changed, fetcher=globalThis.fetch, maxEntries=100, maxBytes=4*1024*1024}) {
    Object.assign(this,{store,socket,live,changed,fetcher});this.maxEntries=Math.min(100,maxEntries);this.maxBytes=Math.min(4*1024*1024,maxBytes);
    this.pending=new Map();this.queue=[];this.running=0;
  }
  current(job) { return this.live() && this.pending.get(job.chat) === job && !job.abort.signal.aborted; }
  request(chats) {
    if (!this.live()) throw Error('Reconnect WhatsApp before loading profile photos.');
    for (const chat of chats) {
      if (!this.store.get('chat',chat) || this.store.get('avatar',chat) || this.pending.has(chat) || this.pending.size>=40) continue;
      const job={chat,abort:new AbortController(),socket:this.socket()};this.pending.set(chat,job);this.queue.push(job);
    }
    this.drain();return {ok:true};
  }
  drain() {
    while (this.running<2 && this.queue.length && this.live()) {
      const job=this.queue.shift();if (!this.current(job)) continue;
      this.running++;
      void this.load(job).catch(()=>{}).finally(()=>{if(this.pending.get(job.chat)===job)this.pending.delete(job.chat);this.running--;this.drain();});
    }
  }
  async load(job) {
    let avatar=null,ttl=NEGATIVE_TTL;
    const timer=job.timer=setTimeout(()=>job.abort.abort(),8000);
    try {
      const url=await job.socket.profilePictureUrl(job.chat,'preview',4000);
      if (!this.current(job)) return;
      if (url) {
        const safe=avatarURL(url);if(!safe)throw Error('Invalid profile host.');
        const response=await this.fetcher(safe,{method:'GET',credentials:'omit',redirect:'error',cache:'no-store',referrerPolicy:'no-referrer',signal:job.abort.signal,headers:{Accept:'image/jpeg,image/png,image/webp'}});
        if (!this.current(job)) { await response.body?.cancel(); return; }
        if (!response.ok) { await response.body?.cancel();if(![401,403,404].includes(response.status))throw Error('Profile unavailable.'); }
        else {
          const mime=(response.headers.get('content-type')||'').split(';')[0].trim().toLowerCase(),size=response.headers.get('content-length');
          if (!MIME.has(mime) || size!==null && (!/^\d+$/.test(size) || Number(size)>AVATAR_BYTES) || !response.body) { await response.body?.cancel();throw Error('Invalid profile response.'); }
          const reader=response.body.getReader(),chunks=[];let length=0;
          try {
            for (;;) {
              const {done,value}=await reader.read();if(done)break;
              length+=value.byteLength;if(length>AVATAR_BYTES || !this.current(job))throw Error('Profile response stopped.');
              chunks.push(Buffer.from(value));
            }
            avatar=avatarDataURL(Buffer.concat(chunks),mime);ttl=AVATAR_TTL;
          } finally { await reader.cancel().catch(()=>{});for(const chunk of chunks)chunk.fill(0); }
        }
      }
    } catch { ttl=ERROR_TTL; }
    finally { clearTimeout(timer); }
    if (!this.live() || this.pending.get(job.chat)!==job || !this.store.get('chat',job.chat)) return;
    this.save(job.chat,avatar,ttl);if(avatar)this.changed(job.chat);
  }
  save(chat,avatar,ttl) {
    for(const {id}of this.store.list('avatar',{expired:true,limit:1000}))this.store.remove('avatar',id);
    const now=Date.now();this.store.set('avatar',chat,{avatar,checkedAt:now,expiresAt:now+ttl},{created:now,expires:now+ttl});
    let {count,bytes}=this.store.totals('avatar');
    if(count>this.maxEntries||bytes>this.maxBytes)for(const {id,value}of this.store.list('avatar',{oldest:true,limit:1000})){
      if(count<=this.maxEntries&&bytes<=this.maxBytes)break;
      bytes-=Buffer.byteLength(JSON.stringify({id,value}));count--;this.store.remove('avatar',id);if(value.avatar)this.changed(id);
    }
  }
  invalidate(chat,removed=false) {
    const job=this.pending.get(chat);if(job){clearTimeout(job.timer);job.abort.abort();this.pending.delete(chat);}
    const old=this.store.get('avatar',chat);this.store.remove('avatar',chat);
    if(removed)this.save(chat,null,NEGATIVE_TTL);
    if(old?.avatar)this.changed(chat);
    if(!removed&&this.live())this.request([chat]);
  }
  pause() { for(const job of this.pending.values()){clearTimeout(job.timer);job.abort.abort();}this.pending.clear();this.queue=[]; }
}
