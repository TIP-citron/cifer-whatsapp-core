// Private descriptor only. Never project URLs, hashes or media keys into renderer DTOs.
export const PHOTO_BYTES = 12 * 1024 * 1024;
export const PHOTO_PIXELS = 32 * 1024 * 1024;
export function photoURL(value) {
  if(typeof value!=='string'||value.length>4096)return null;
  try{const u=new URL(value);return u.protocol==='https:'&&!u.username&&!u.password&&!u.port&&!u.hash&&(u.hostname==='mmg.whatsapp.net'||u.hostname==='mmg-fna.whatsapp.net'||u.hostname.endsWith('.mmg.whatsapp.net'))&&u.pathname.startsWith('/')?u.href:null;}catch{return null;}
}
export function imageDescriptor(image) {
  if(!image||image.viewOnce)return undefined;
  const length=Number(image.fileLength),mime=image.mimetype;
  if(!Number.isSafeInteger(length)||length<1||length>PHOTO_BYTES||!['image/jpeg','image/png','image/webp'].includes(mime))return undefined;
  const keys={};for(const name of ['mediaKey','fileSha256','fileEncSha256']){const value=image[name];if(!(value instanceof Uint8Array)||value.length!==32)return undefined;keys[name]=Buffer.from(value).toString('hex');}
  let url=photoURL(image.url);
  if(typeof image.directPath==='string'&&image.directPath.startsWith('/')&&!image.directPath.startsWith('//'))url=photoURL('https://mmg.whatsapp.net'+image.directPath)||url;
  if(!url)return undefined;
  return {url,mime,length,...keys};
}
