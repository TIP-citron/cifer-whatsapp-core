import { BufferJSON, initAuthCreds, proto } from 'baileys';

/** A Signal key batch and its updated credentials always commit together. */
export function authStore(store, live) {
  const saved=store.get('wa-auth','creds'),creds=saved?JSON.parse(saved,BufferJSON.reviver):initAuthCreds();
  const check=()=>{if(!live())throw Error('WhatsApp session ended.');};
  const save=()=>{check();store.transaction(()=>store.set('wa-auth','creds',JSON.stringify(creds,BufferJSON.replacer)));};
  const state={creds,keys:{
    async get(type,ids){check();const values={};for(const id of ids){const raw=store.get('wa-key',JSON.stringify([type,id]));if(raw){let value=JSON.parse(raw,BufferJSON.reviver);if(type==='app-state-sync-key')value=proto.Message.AppStateSyncKeyData.fromObject(value);values[id]=value;}}return values;},
    async set(data){check();store.transaction(()=>{for(const [type,values]of Object.entries(data))for(const [id,value]of Object.entries(values||{})){const key=JSON.stringify([type,id]);if(value==null)store.remove('wa-key',key);else store.set('wa-key',key,JSON.stringify(value,BufferJSON.replacer));}save();});}
  }};
  save();return {state,save};
}
