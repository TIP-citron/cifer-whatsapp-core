import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';

// One reviewed correction to the pinned decoder. No downloading or runtime patching.
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
}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href)patchBaileys();
