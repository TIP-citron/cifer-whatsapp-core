import { photoURL } from './photo-content.mjs';
import { avatarDataURL } from './avatars.mjs';

export const MEDIA_BYTES = 256 * 1024 * 1024;
export const MEDIA_CHUNK = 256 * 1024;
const formats = {
  audio: ['audio/ogg', 'audio/mpeg', 'audio/mp4', 'audio/aac', 'audio/wav', 'audio/webm'],
  video: ['video/mp4', 'video/webm'], sticker: ['image/webp'],
};
export function mediaDescriptor(body) {
  const type = body.audioMessage ? 'audio' : body.videoMessage || body.ptvMessage ? 'video' : body.stickerMessage ? 'sticker' : null;
  if (!type) return undefined;
  const m = body.audioMessage || body.videoMessage || body.ptvMessage || body.stickerMessage;
  const mime = typeof m.mimetype === 'string' ? m.mimetype.split(';')[0].trim().toLowerCase() : '';
  const length = Number(m.fileLength);
  if (m.viewOnce || !Number.isSafeInteger(length) || length < 1 || length > (type==='sticker'?4*1024**2:MEDIA_BYTES) || !formats[type].includes(mime)) return undefined;
  const keys = {};
  for (const k of ['mediaKey', 'fileSha256', 'fileEncSha256']) {
    if (!(m[k] instanceof Uint8Array) || m[k].length !== 32) return undefined;
    keys[k] = Buffer.from(m[k]).toString('hex');
  }
  const path = typeof m.directPath === 'string' && m.directPath.startsWith('/') && !m.directPath.startsWith('//') ? photoURL('https://mmg.whatsapp.net' + m.directPath) : null;
  const url = path || photoURL(m.url); if (!url) return undefined;
  let preview;
  if(type==='video'&&m.jpegThumbnail instanceof Uint8Array){try{preview=avatarDataURL(m.jpegThumbnail,'image/jpeg');}catch{/* A malformed preview never blocks the verified attachment. */}}
  return { type, mime, length, url, ...keys, ...(preview?{preview}:{}), voice: type === 'audio' && m.ptt === true, animated: type === 'sticker' && m.isAnimated === true,
    loop: type === 'video' && m.gifPlayback === true, round: !!body.ptvMessage,
    seconds: Number.isFinite(Number(m.seconds)) ? Math.max(0, Math.min(86400, Number(m.seconds))) : 0 };
}
export function mediaUnavailable(body) {
  const type=body.audioMessage?'audio':body.videoMessage||body.ptvMessage?'video':body.stickerMessage?'sticker':null;
  if(!type)return undefined;const m=body.audioMessage||body.videoMessage||body.ptvMessage||body.stickerMessage;
  if(Number(m.fileLength)>(type==='sticker'?4*1024**2:MEDIA_BYTES))return 'too-large';
  if(typeof m.mimetype==='string'&&!formats[type].includes(m.mimetype.split(';')[0].trim().toLowerCase()))return 'unsupported-format';
  return undefined;
}
export function mediaSummary(descriptor, label, reason) {
  const type = descriptor?.type || (label?.startsWith('Audio') ? 'audio' : label?.startsWith('Video') ? 'video' : label?.startsWith('Sticker') ? 'sticker' : null);
  return type ? { type, available: !!descriptor, ...(reason?{reason}:{}), ...(descriptor ? {mime:descriptor.mime, size:descriptor.length, voice:descriptor.voice, animated:descriptor.animated, loop:descriptor.loop, round:descriptor.round, seconds:descriptor.seconds,...(descriptor.preview?{preview:descriptor.preview}:{})} : {}) } : undefined;
}
/** Container sniffing before handing authenticated bytes to sandboxed decoders. */
export function validMediaHeader(bytes, mime) {
  const ascii = (start, end) => bytes.subarray(start,end).toString('ascii');
  if (mime === 'image/webp') {
    if (ascii(0,4)!=='RIFF'||ascii(8,12)!=='WEBP') return false;
    // Animated/static extended WebP carries dimensions before any frame data.
    if (ascii(12,16)==='VP8X') return bytes.length>=30 && bytes.readUIntLE(24,3)<1024 && bytes.readUIntLE(27,3)<1024;
    if(ascii(12,16)==='VP8L'&&bytes.length>=25&&bytes[20]===0x2f){const bits=bytes.readUInt32LE(21);return (bits&0x3fff)<1024&&((bits>>>14)&0x3fff)<1024;}
    if(ascii(12,16)==='VP8 '&&bytes.length>=30&&bytes.subarray(23,26).equals(Buffer.from([0x9d,0x01,0x2a])))return (bytes.readUInt16LE(26)&0x3fff)<=1024&&(bytes.readUInt16LE(28)&0x3fff)<=1024;
    return false;
  }
  if (mime.endsWith('/mp4')) return ascii(4,8)==='ftyp';
  if (mime.endsWith('/webm')) return bytes.subarray(0,4).equals(Buffer.from([0x1a,0x45,0xdf,0xa3]));
  if (mime==='audio/ogg') return ascii(0,4)==='OggS';
  if (mime==='audio/wav') return ascii(0,4)==='RIFF'&&ascii(8,12)==='WAVE';
  if (mime==='audio/mpeg') return ascii(0,3)==='ID3'||bytes[0]===255&&(bytes[1]&0xe0)===0xe0;
  if (mime==='audio/aac') return bytes[0]===255&&(bytes[1]&0xf6)===0xf0;
  return false;
}
