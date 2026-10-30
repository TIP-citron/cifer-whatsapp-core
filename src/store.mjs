import { DatabaseSync } from 'node:sqlite';
import { createCipheriv, createDecipheriv, createHmac, randomBytes } from 'node:crypto';
import { chmodSync, existsSync, lstatSync, mkdirSync } from 'node:fs';
import { join } from 'node:path';

/** One encrypted database per trusted account+connection. Routing IDs are keyed hashes. */
export class Store {
  constructor(directory, key, binding) {
    if (!(key instanceof Uint8Array) || key.byteLength !== 32) throw Error('Invalid storage key.');
    this.key = Buffer.from(key); this.closed = false; this.depth = 0;
    mkdirSync(directory, { recursive: true, mode: 0o700 });
    if (!lstatSync(directory).isDirectory() || lstatSync(directory).isSymbolicLink()) throw Error('Invalid storage directory.');
    chmodSync(directory, 0o700);
    const path = join(directory, 'whatsapp.sqlite');
    if (existsSync(path) && (!lstatSync(path).isFile() || lstatSync(path).isSymbolicLink())) throw Error('Invalid storage file.');
    this.db = new DatabaseSync(path); chmodSync(path, 0o600);
    this.db.exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA secure_delete=ON; PRAGMA busy_timeout=5000; PRAGMA temp_store=MEMORY;
      CREATE TABLE IF NOT EXISTS records(namespace TEXT NOT NULL,id TEXT NOT NULL,tag TEXT NOT NULL,created INTEGER NOT NULL,expires INTEGER NOT NULL,size INTEGER NOT NULL,unread INTEGER NOT NULL DEFAULT 0,blob BLOB NOT NULL,PRIMARY KEY(namespace,id));
      CREATE INDEX IF NOT EXISTS records_page ON records(namespace,tag,created DESC,id);
      CREATE INDEX IF NOT EXISTS records_expiry ON records(expires);
      CREATE TABLE IF NOT EXISTS counters(name TEXT PRIMARY KEY,value INTEGER NOT NULL);`);
    for (const suffix of ['-wal', '-shm']) if (existsSync(path + suffix)) chmodSync(path + suffix, 0o600);
    try {
      const old = this.get('system', 'binding');
      if (old && (old.account !== binding.account || old.connection !== binding.connection)) throw Error('The local WhatsApp account changed.');
      if (!old) this.set('system', 'binding', binding);
    } catch (error) { this.close(); throw error; }
  }
  assertOpen() { if (this.closed) throw Error('Storage is closed.'); }
  hash(value) { this.assertOpen(); return createHmac('sha256', this.key).update(value).digest('hex'); }
  rowId(namespace, id) { return namespace === 'system' && id === 'binding' ? 'binding' : this.hash(JSON.stringify([namespace, id])); }
  decode(namespace, row) {
    const blob = Buffer.from(row.blob), decipher = createDecipheriv('aes-256-gcm', this.key, blob.subarray(0, 12));
    decipher.setAAD(Buffer.from(JSON.stringify([namespace, row.id]))); decipher.setAuthTag(blob.subarray(-16));
    const plain = Buffer.concat([decipher.update(blob.subarray(12, -16)), decipher.final()]);
    try { return JSON.parse(plain.toString('utf8')); } finally { plain.fill(0); }
  }
  get(namespace, id, now = Date.now()) {
    this.assertOpen();
    const row = this.db.prepare('SELECT id,blob FROM records WHERE namespace=? AND id=? AND (expires=0 OR expires>?)').get(namespace, this.rowId(namespace, id), now);
    return row ? this.decode(namespace, row).value : null;
  }
  set(namespace, id, value, { expires = 0, created = 0, tag = '', unread = false } = {}) {
    this.assertOpen();
    const rowId = this.rowId(namespace, id), nonce = randomBytes(12), plain = Buffer.from(JSON.stringify({ id, value }));
    try {
      const cipher = createCipheriv('aes-256-gcm', this.key, nonce); cipher.setAAD(Buffer.from(JSON.stringify([namespace, rowId])));
      const blob = Buffer.concat([nonce, cipher.update(plain), cipher.final(), cipher.getAuthTag()]);
      this.db.prepare(`INSERT INTO records VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(namespace,id) DO UPDATE SET tag=excluded.tag,created=excluded.created,expires=excluded.expires,size=excluded.size,unread=excluded.unread,blob=excluded.blob`)
        .run(namespace, rowId, tag ? this.hash(tag) : '', created, expires, plain.length, unread ? 1 : 0, blob);
    } finally { plain.fill(0); }
  }
  remove(namespace, id) { this.assertOpen(); this.db.prepare('DELETE FROM records WHERE namespace=? AND id=?').run(namespace, this.rowId(namespace, id)); }
  clear(namespace) { this.assertOpen(); this.db.prepare('DELETE FROM records WHERE namespace=?').run(namespace); }
  list(namespace, { tag, now = Date.now(), limit = 30000, oldest = false, expired = false } = {}) {
    this.assertOpen();
    const values = [namespace, now]; let sql = `SELECT id,blob FROM records WHERE namespace=? AND ${expired ? 'expires>0 AND expires<=?' : '(expires=0 OR expires>?)'}`;
    if (tag !== undefined) { sql += ' AND tag=?'; values.push(tag ? this.hash(tag) : ''); }
    sql += ` ORDER BY created ${oldest ? 'ASC' : 'DESC'},rowid ${oldest ? 'ASC' : 'DESC'} LIMIT ?`; values.push(limit);
    return this.db.prepare(sql).all(...values).map(row => this.decode(namespace, row));
  }
  totals(namespace) { this.assertOpen(); return this.db.prepare('SELECT COUNT(*) AS count,COALESCE(SUM(size),0) AS bytes FROM records WHERE namespace=?').get(namespace); }
  messageStats(now = Date.now()) {
    this.assertOpen(); return new Map(this.db.prepare("SELECT tag,SUM(unread) AS unread,MAX(created) AS createdAt FROM records WHERE namespace='message' AND expires>? GROUP BY tag").all(now).map(r=>[r.tag,r]));
  }
  transaction(fn) {
    this.assertOpen(); if (this.depth) return fn();
    this.db.exec('BEGIN IMMEDIATE'); this.depth++;
    try { const out = fn(); this.db.exec('COMMIT'); return out; }
    catch (error) { this.db.exec('ROLLBACK'); throw error; }
    finally { this.depth--; }
  }
  next() { this.assertOpen(); return Number(this.db.prepare("INSERT INTO counters VALUES('revision',1) ON CONFLICT(name) DO UPDATE SET value=value+1 RETURNING value").get().value); }
  revision() { this.assertOpen(); return Number(this.db.prepare("SELECT value FROM counters WHERE name='revision'").get()?.value || 0); }
  prune(now = Date.now()) { this.assertOpen(); this.db.prepare('DELETE FROM records WHERE expires>0 AND expires<=?').run(now); }
  checkpoint() { this.assertOpen(); this.db.exec('PRAGMA wal_checkpoint(TRUNCATE)'); }
  close() { if (this.closed) return; try { this.checkpoint(); } finally { this.closed = true; this.db.close(); this.key.fill(0); } }
}
