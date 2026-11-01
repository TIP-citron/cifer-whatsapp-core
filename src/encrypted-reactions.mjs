import { createDecipheriv, hkdfSync } from 'node:crypto';
import { proto } from 'baileys';
import { WEEK, keyOf, normalizePeer, peerPattern } from './content.mjs';

export const reactionAuthors = values => [...new Set(values.map(normalizePeer).filter(v=>peerPattern.test(v)))].slice(0,4);
export function encryptedReaction(message, now=Date.now(),retention=WEEK) {
  if(!keyOf(message.key))return null;
  const at=Number(message.messageTimestamp)*1000;
  if(!Number.isSafeInteger(at)||at<=now-retention||at>now+60000)return null;
  let body=message.message;
  for(let n=0;n<8&&body?.ephemeralMessage;n++)body=body.ephemeralMessage.message;
  const seal=body?.encReactionMessage,target=seal?.targetMessageKey;
  if(!/^[-\w]{1,128}$/.test(target?.id||'')||typeof target.fromMe!=='boolean'||!(seal.encPayload instanceof Uint8Array)||seal.encPayload.length<17||seal.encPayload.length>1024||!(seal.encIv instanceof Uint8Array)||seal.encIv.length!==12)return null;
  const pick=key=>({remoteJid:key.remoteJid,remoteJidAlt:key.remoteJidAlt,id:key.id,fromMe:key.fromMe,participant:key.participant,participantAlt:key.participantAlt});
  return {encrypted:true,envelope:pick(message.key),key:pick(target),at,payload:Buffer.from(seal.encPayload).toString('base64'),iv:Buffer.from(seal.encIv).toString('hex')};
}
export function decryptReaction(seal, secretHex, creators, actors, now=Date.now(),retention=WEEK) {
  if(!/^[a-f0-9]{64}$/.test(secretHex||''))return null;
  const secret=Buffer.from(secretHex,'hex'),ciphertext=Buffer.from(seal.payload,'base64');
  try{
    for(const creator of reactionAuthors(creators))for(const actor of reactionAuthors(actors)){
      let key,plain;
      try{
        key=Buffer.from(hkdfSync('sha256',secret,Buffer.alloc(32),Buffer.from(seal.id+creator+actor+'Enc Reaction'),32));
        const cipher=createDecipheriv('aes-256-gcm',key,Buffer.from(seal.iv,'hex'));
        cipher.setAAD(Buffer.alloc(0));cipher.setAuthTag(ciphertext.subarray(-16));
        plain=Buffer.concat([cipher.update(ciphertext.subarray(0,-16)),cipher.final()]);
        const reaction=proto.Message.ReactionMessage.decode(plain),at=Number(reaction.senderTimestampMs);
        if(reaction.key||!Number.isSafeInteger(at)||at<=now-retention||at>now+60000||typeof reaction.text!=='string')return null;
        return {text:reaction.text,at};
      }catch{/* Only authenticated reaction text is projected; no payloads enter logs. */}
      finally{key?.fill(0);plain?.fill(0);}
    }
    return null;
  }finally{secret.fill(0);}
}
