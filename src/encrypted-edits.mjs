import { createDecipheriv, hkdfSync } from 'node:crypto';
import { proto } from 'baileys';
import { WEEK, extract, keyOf, normalizePeer, peerPattern, isGroup } from './content.mjs';

export const editAuthors = values => [...new Set(values.map(normalizePeer).filter(v=>peerPattern.test(v)))].slice(0,4);
export function encryptedEdit(message, now=Date.now()) {
  const key=keyOf(message.key),at=Number(message.messageTimestamp)*1000;
  if(!key||!Number.isSafeInteger(at)||at<=now-WEEK||at>now+60000)return null;
  let body=message.message;
  for(let n=0;n<8&&body;n++){
    if(body.viewOnceMessage||body.viewOnceMessageV2||body.viewOnceMessageV2Extension)return null;
    const wrapped=body.ephemeralMessage||body.editedMessage||body.documentWithCaptionMessage;
    if(!wrapped)break;body=wrapped.message;
  }
  const seal=body?.secretEncryptedMessage;
  if(seal?.secretEncType!==2||!Object.hasOwn(seal,'secretEncType')||!/^[-\w]{1,128}$/.test(seal.targetMessageKey?.id||'')||!(seal.encPayload instanceof Uint8Array)||seal.encPayload.length<17||seal.encPayload.length>65536||!(seal.encIv instanceof Uint8Array)||seal.encIv.length!==12)return null;
  // Only the authenticated transport envelope chooses the conversation/direction.
  return {...key,authors:editAuthors(isGroup(key.chat)?[key.participant,key.participantAlt]:[key.chat,key.alternate]),id:seal.targetMessageKey.id,at,payload:Buffer.from(seal.encPayload).toString('base64'),iv:Buffer.from(seal.encIv).toString('hex')};
}
export function decryptEdit(seal, secretHex, authors, now=Date.now()) {
  if(!/^[a-f0-9]{64}$/.test(secretHex||''))return null;
  const secret=Buffer.from(secretHex,'hex'),ciphertext=Buffer.from(seal.payload,'base64');
  try{
    // Candidates are aliases of the authenticated author, never the embedded target.
    for(const originalSender of editAuthors(authors))for(const editor of editAuthors(authors)){
      let key,plain;
      try{
        key=Buffer.from(hkdfSync('sha256',secret,Buffer.alloc(32),Buffer.from(seal.id+originalSender+editor+'Message Edit'),32));
        const cipher=createDecipheriv('aes-256-gcm',key,Buffer.from(seal.iv,'hex'));
        cipher.setAAD(Buffer.alloc(0));cipher.setAuthTag(ciphertext.subarray(-16));
        plain=Buffer.concat([cipher.update(ciphertext.subarray(0,-16)),cipher.final()]);
        const protocol=proto.Message.decode(plain).protocolMessage;
        if(!protocol||!Object.hasOwn(protocol,'type')||protocol.type!==14||!protocol.editedMessage||(protocol.key?.id&&protocol.key.id!==seal.id))return null;
        const at=protocol.timestampMs?Number(protocol.timestampMs):seal.at;
        if(!Number.isSafeInteger(at)||at<=now-WEEK||at>now+60000)return null;
        return {content:extract(protocol.editedMessage),at};
      }catch{/* Authentication failure is not a message, and never enters logs. */}
      finally{key?.fill(0);plain?.fill(0);}
    }
    return null;
  }finally{secret.fill(0);}
}
