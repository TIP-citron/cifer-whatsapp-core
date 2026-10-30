import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp,readFile,rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { Store } from '../src/store.mjs';
import { authStore } from '../src/auth-store.mjs';

test('encrypted durable store binds account, hides routing/content, authenticates values and rolls back',async t=>{
 const dir=await mkdtemp(join(tmpdir(),'cifer-wa-store-'));t.after(()=>rm(dir,{recursive:true,force:true}));
 const key=Buffer.alloc(32,7),binding={account:'A',connection:'one'};let store=new Store(dir,key,binding);
 store.set('message','SECRET_PHONE',{text:'SENTINEL_PRIVATE_TEXT'},{tag:'SECRET_PHONE'});
 assert.throws(()=>store.transaction(()=>{store.set('message','x',{text:'rollback'});throw Error();}));assert.equal(store.get('message','x'),null);
 store.close();const bytes=await readFile(join(dir,'whatsapp.sqlite'));assert.equal(bytes.includes(Buffer.from('SENTINEL_PRIVATE_TEXT')),false);assert.equal(bytes.includes(Buffer.from('SECRET_PHONE')),false);
 assert.throws(()=>new Store(dir,Buffer.alloc(32,8),binding));assert.throws(()=>new Store(dir,key,{account:'B',connection:'one'}));
 store=new Store(dir,key,binding);assert.equal(store.get('message','SECRET_PHONE').text,'SENTINEL_PRIVATE_TEXT');store.close();assert.equal(key[0],7);
});
test('real Baileys Signal key serialization restores app-state keys and fences stale callbacks',async t=>{
 const dir=await mkdtemp(join(tmpdir(),'cifer-wa-auth-'));let active=true;const store=new Store(dir,Buffer.alloc(32,3),{account:'A',connection:'one'});t.after(async()=>{store.close();await rm(dir,{recursive:true,force:true});});
 const auth=authStore(store,()=>active);auth.state.creds.nextPreKeyId=73;
 await auth.state.keys.set({'app-state-sync-key':{test:{keyData:Buffer.from('test-key')}},session:{peer:Buffer.from('signal-fixture')}});
 const restored=authStore(store,()=>active);assert.equal(restored.state.creds.nextPreKeyId,73);assert.equal((await restored.state.keys.get('app-state-sync-key',['test'])).test.keyData.toString(),'test-key');
 active=false;store.clear('wa-auth');store.clear('wa-key');await assert.rejects(auth.state.keys.set({session:{peer:Buffer.from('late')}}));assert.throws(()=>auth.save());assert.equal(store.get('wa-auth','creds'),null);
});
