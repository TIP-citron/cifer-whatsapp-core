import { exact } from './content.mjs';
export const DAY = 86400000, RETENTION_HORIZON = 90 * DAY;
export const historyDays = store => store.get('system','storage-policy')?.historyDays || 7;
export const mediaBudget = store => store.get('system','storage-policy')?.mediaBytes || 1024 ** 3;
export function storageSettings(engine, args) {
  exact(args, [], ['historyDays','mediaBytes','clearMedia']);
  if (args.historyDays !== undefined && ![7,30,90].includes(args.historyDays) || args.mediaBytes !== undefined && ![256*1024**2,1024**3,5*1024**3].includes(args.mediaBytes) || args.clearMedia !== undefined && typeof args.clearMedia !== 'boolean') throw Error('Invalid storage settings.');
  const store=engine.store, old=historyDays(store), next=args.historyDays || old;
  store.transaction(()=>{
    store.set('system','storage-policy',{historyDays:next,mediaBytes:args.mediaBytes || mediaBudget(store)});
    // Increasing retention affects future imports. Existing expiry is never extended.
    if(next<old)for(const {value:row}of store.list('message',{limit:10000})) {
      const expiresAt=Math.min(row.expiresAt,row.createdAt+next*DAY);
      if(expiresAt<=Date.now())engine.inbox.tombstone(row);
      else if(expiresAt!==row.expiresAt)engine.inbox.save({...row,expiresAt});
    }
  });
  if(args.clearMedia){engine.media.clear();engine.inbox.revision(null,'reset');}
  engine.media.trim();store.checkpoint();
  return {historyDays:next,mediaBytes:mediaBudget(store),usedMediaBytes:store.totals('media-chunk').bytes+store.totals('photo').bytes,maxMessages:10000,maxMessageBytes:64*1024**2};
}
