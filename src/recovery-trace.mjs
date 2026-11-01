import { normalizePeer, messageId } from './content.mjs';
const COUNT=['historyReplies','historyMessages','historyAnchorMatches','historyAnchorReactions','upsertAnchorMatches','upsertAnchorReactions','pdoReplies','pdoResults','pdoDecodeErrors','pdoAnchorMatches','pdoAnchorReactions','pdoAnchorEncrypted','pdoMissingChat','pdoMissingTime','pdoMissingDirection','pdoMissingSecret','appliedReactions'];
const MAX=65535;
export class RecoveryTrace {
 constructor(emit,decode){this.emit=typeof emit==='function'?emit:null;this.decode=decode;this.rows=[];this.next=0;}
 begin(anchor,visible){if(!this.emit)return null;const row={sequence:++this.next,anchor,anchorVisible:!!visible,anchorFromMe:anchor.fromMe,anchorKind:visible?.kind||'metadata',historySent:false,resendSent:false,resendResolved:false,resendRejected:false,...Object.fromEntries(COUNT.map(k=>[k,0]))};this.rows.push(row);if(this.rows.length>8)this.rows.shift();this.flush();return row;}
 set(row,values){if(row&&this.rows.includes(row)){Object.assign(row,values);this.flush();}}
 add(row,key,n=1){row[key]=Math.min(MAX,row[key]+n);}
 matches(row,key){return key?.id===row.anchor.id&&(!Object.hasOwn(key,'fromMe')||key.fromMe===row.anchor.fromMe)&&(!key.remoteJid||[row.anchor.chat,row.anchor.protocolKey?.remoteJid].includes(normalizePeer(key.remoteJid)));}
 history(messages,request){if(typeof request!=='string')return;for(const row of this.rows)if(row.historyId===request){this.add(row,'historyReplies');this.add(row,'historyMessages',messages.length);for(const m of messages)if(this.matches(row,m.key)){this.add(row,'historyAnchorMatches');this.add(row,'historyAnchorReactions',m.reactions?.length||0);}}this.flush();}
 upserts(messages){
  for(const m of messages){
   for(const row of this.rows)if(this.matches(row,m.key)){this.add(row,'upsertAnchorMatches');this.add(row,'upsertAnchorReactions',m.reactions?.length||0);}
   if(m.key?.fromMe!==true)continue;let body=m.message;for(let n=0;n<8&&body?.ephemeralMessage;n++)body=body.ephemeralMessage.message;
   const p=body?.protocolMessage;if(p?.type!==17)continue;const response=p.peerDataOperationRequestResponseMessage;if(!response)continue;
   for(const row of this.rows)if(row.resendId===response.stanzaId){this.add(row,'pdoReplies');for(const result of (response.peerDataOperationResult||[]).slice(0,128)){
    this.add(row,'pdoResults');const bytes=result.placeholderMessageResendResponse?.webMessageInfoBytes;if(!(bytes instanceof Uint8Array)||bytes.length>4*1024*1024)continue;
    let recovered;try{recovered=this.decode(bytes);}catch{this.add(row,'pdoDecodeErrors');continue;}
    if(!this.matches(row,recovered.key))continue;this.add(row,'pdoAnchorMatches');this.add(row,'pdoAnchorReactions',recovered.reactions?.length||0);
    if(recovered.message?.encReactionMessage)this.add(row,'pdoAnchorEncrypted');
    if(!recovered.key?.remoteJid)this.add(row,'pdoMissingChat');if(!Object.hasOwn(recovered.key||{},'fromMe'))this.add(row,'pdoMissingDirection');if(!Number(recovered.messageTimestamp))this.add(row,'pdoMissingTime');if(!recovered.messageSecret?.length&&!recovered.message?.messageContextInfo?.messageSecret?.length)this.add(row,'pdoMissingSecret');
   }}
  }this.flush();
 }
 applied(store,reactions){for(const row of this.rows){const target=store.get('message',messageId(row.anchor));row.appliedReactions=target?reactions.view(target).reduce((n,r)=>n+r.count,0):0;}this.flush();}
 snapshot(){return {schema:1,traces:this.rows.map(row=>Object.fromEntries(['sequence','anchorVisible','anchorFromMe','anchorKind','historySent','resendSent','resendResolved','resendRejected',...COUNT].map(k=>[k,row[k]])))};}
 flush(){if(this.emit)try{this.emit(this.snapshot());}catch{}}
 clear(){this.rows=[];this.next=0;this.flush();}
}
