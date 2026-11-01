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
  const refreshBefore="                const key = stringifyMessageKey(message.key);\n                const existingMsg = data.historySets.messages[key];";
  const refreshAfter="                const refresh = eventData.syncType === 6 && typeof eventData.peerDataRequestSessionId === 'string';\n                const key = stringifyMessageKey(message.key) + (refresh ? ',history:' + eventData.peerDataRequestSessionId : '');\n                const existingMsg = data.historySets.messages[key];";
  const buffer=readFileSync(bufferFile,'utf8'),originalBuffer=buffer.replace(bufferAfter,bufferBefore).replace(refreshAfter,refreshBefore);
  if(createHash('sha256').update(originalBuffer).digest('hex')!=='09da9a892668084c3016108a85517d627022afd2ff57ab9cf97f6051b4dbd509'||originalBuffer.split(bufferBefore).length!==2)throw Error('Unreviewed Baileys event buffer; do not apply the participant patch.');
  if(originalBuffer.split(refreshBefore).length!==2)throw Error('Unreviewed history buffer; do not apply refresh patch.');
  const patched=originalBuffer.replace(bufferBefore,bufferAfter).replace(refreshBefore,refreshAfter);
  if(buffer!==patched)writeFileSync(bufferFile,patched);
  const recoveryFile=require.resolve('baileys/lib/Utils/process-message.js');
  const recoveryBefore="                                // Apply decoded message content onto cached metadata (preserves LID etc.)\n                                cachedData.message = webMessageInfo.message;\n                                //eslint-disable-next-line max-depth\n                                if (webMessageInfo.messageTimestamp) {\n                                    cachedData.messageTimestamp = webMessageInfo.messageTimestamp;\n                                }\n                                finalMsg = cachedData;";
  const recoveryAfter="                                // The phone can omit key fields and timestamps. Preserve native-known\n                                // metadata while retaining returned reactions and original message secrets.\n                                const known = cachedData.key, received = webMessageInfo.key;\n                                const normalize = value => typeof value === 'string' ? value.replace(/:\\d+@/, '@') : '';\n                                if (known && received && (\n                                    received.id && received.id !== known.id ||\n                                    Object.hasOwn(received, 'fromMe') && received.fromMe !== known.fromMe ||\n                                    ['remoteJid', 'participant'].some(field => received[field] && known[field] &&\n                                        ![known[field], known[field + 'Alt']].some(value => normalize(value) === normalize(received[field])))\n                                )) continue;\n                                finalMsg = { ...cachedData, ...webMessageInfo, key: { ...known, ...received } };";
  const recovery=readFileSync(recoveryFile,'utf8'),originalRecovery=recovery.replace(recoveryAfter,recoveryBefore);
  if(createHash('sha256').update(originalRecovery).digest('hex')!=='ba0f73da5bbffd4623ff4411aa6ee59f89070145b32e90095b4b830edfeb4793'||originalRecovery.split(recoveryBefore).length!==2)throw Error('Unreviewed phone recovery decoder; do not apply metadata patch.');
  const patchedRecovery=originalRecovery.replace(recoveryBefore,recoveryAfter);
  if(recovery!==patchedRecovery)writeFileSync(recoveryFile,patchedRecovery);
}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href)patchBaileys();
