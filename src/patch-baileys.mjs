import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';

// Reviewed corrections to the pinned decoder and group event buffer. No downloading or runtime patching.
export function patchBaileys() {
  const require=createRequire(import.meta.url),file=require.resolve('baileys/lib/Utils/decode-wa-message.js');
  const before="                        msg = msg.deviceSentMessage?.message || msg;";
  const after=`                        if (msg.deviceSentMessage?.message) {
                            const inner = msg.deviceSentMessage.message;
                            // Keep the original secret needed to authenticate later edits.
                            if (msg.messageContextInfo) inner.messageContextInfo = {
                                ...msg.messageContextInfo, ...inner.messageContextInfo
                            };
                            msg = inner;
                        }`;
  const source=readFileSync(file,'utf8');
  const original=source.includes(after)?source.replace(after,before):source;
  if(createHash('sha256').update(original).digest('hex')!=='ddd2f90e538b509e83a80a3e5198feced0337240885f0abf75c7352a34afe7cc'||original.split(before).length!==2)throw Error('Unreviewed Baileys decoder; do not apply the edit-secret patch.');
  if(source===original)writeFileSync(file,original.replace(before,after));
  const bufferFile=require.resolve('baileys/lib/Utils/event-buffer.js');
  const bufferBefore="const stringifyMessageKey = (key) => `${key.remoteJid},${key.id},${key.fromMe ? '1' : '0'}`;";
  const bufferAfter="const stringifyMessageKey = (key) => `${key.remoteJid},${key.id},${key.fromMe ? '1' : '0'}${key.remoteJid?.endsWith('@g.us') ? ',' + (key.participant || '').replace(/:\\d+@/, '@') : ''}`;";
  const buffer=readFileSync(bufferFile,'utf8'),originalBuffer=buffer.includes(bufferAfter)?buffer.replace(bufferAfter,bufferBefore):buffer;
  if(createHash('sha256').update(originalBuffer).digest('hex')!=='09da9a892668084c3016108a85517d627022afd2ff57ab9cf97f6051b4dbd509'||originalBuffer.split(bufferBefore).length!==2)throw Error('Unreviewed Baileys event buffer; do not apply the participant patch.');
  if(buffer===originalBuffer)writeFileSync(bufferFile,originalBuffer.replace(bufferBefore,bufferAfter));
}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href)patchBaileys();
