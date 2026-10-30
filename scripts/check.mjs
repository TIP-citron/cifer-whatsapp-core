import { readdirSync, readFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
const walk=path=>readdirSync(path,{withFileTypes:true}).flatMap(e=>e.isDirectory()?walk(`${path}/${e.name}`):[`${path}/${e.name}`]);
for(const file of [...walk('src'),...walk('test'),...walk('scripts')].filter(f=>f.endsWith('.mjs'))){const result=spawnSync(process.execPath,['--check',file],{stdio:'inherit'});if(result.status)process.exit(result.status);}
for(const file of walk('src'))if(/WHATSAPP_SERVICE_TOKEN|decryptPayload|createServer\(|fetch\(/.test(readFileSync(file,'utf8')))throw Error(`Unexpected cloud/listener dependency in ${file}`);
process.stdout.write('Syntax and standalone boundaries passed.\n');
