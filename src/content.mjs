import { createHash } from 'node:crypto';

export const WEEK = 604800000, MAX_MESSAGES = 10000, MAX_BYTES = 64 * 1024 * 1024;
export const peerPattern = /^\d{5,24}@(s\.whatsapp\.net|lid)$/;
export const uuidPattern = /^[a-f0-9]{8}-[a-f0-9]{4}-[1-8][a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/i;
export const normalizePeer = value => typeof value === 'string' ? value.replace(/:\d+@/, '@') : '';
export const digest = value => createHash('sha256').update(value).digest('hex');
export function canonicalJSON(value) { return JSON.stringify(value, (_key, item) => item && typeof item === 'object' && !Array.isArray(item) ? Object.fromEntries(Object.keys(item).sort().map(key=>[key,item[key]])) : item); }
export function assertPeer(value) { const peer = normalizePeer(value); if (!peerPattern.test(peer)) throw Error('Choose an individual WhatsApp chat.'); return peer; }
export function exact(value, keys, optional = []) {
  if (!value || typeof value !== 'object' || Array.isArray(value) || keys.some(k => !Object.hasOwn(value, k)) || Object.keys(value).some(k => !keys.includes(k) && !optional.includes(k))) throw Error('Invalid WhatsApp request.');
}
export function keyOf(key) {
  const chat = normalizePeer(key?.remoteJid), id = key?.id;
  if (!peerPattern.test(chat) || typeof id !== 'string' || !/^[\w-]{1,128}$/.test(id) || typeof key.fromMe !== 'boolean') return null;
  return { chat, id, fromMe: key.fromMe, alternate: normalizePeer(key.remoteJidAlt) };
}
export const messageId = key => JSON.stringify([key.chat, key.id, key.fromMe]);
export function protectedWire(value) {
  if (typeof value !== 'string' || Buffer.byteLength(value) > 16384 || !/^CIFER:WA:[12]:/.test(value)) throw Error('Invalid protected message.');
  const version = Number(value[9]), encoded = value.slice(11).split('\n')[0], decoded = Buffer.from(encoded, 'base64');
  if (!/^[A-Za-z0-9+/]+={0,2}$/.test(encoded) || decoded.toString('base64') !== encoded) throw Error('Invalid protected message.');
  let data; try { data = JSON.parse(decoded.toString('utf8')); } catch { throw Error('Invalid protected message.'); }
  exact(data, ['context', 'sealed']); exact(data.context, version === 1 ? ['version', 'id', 'sender', 'target', 'createdAt'] : ['version', 'id', 'sender', 'contact', 'createdAt']); exact(data.sealed, ['nonce', 'ciphertext']);
  const c = data.context, s = data.sealed;
  if (c.version !== version || !uuidPattern.test(c.id || '') || !uuidPattern.test(c.sender || '') || !Number.isSafeInteger(c.createdAt) || c.createdAt < 1
    || version === 1 && !peerPattern.test(c.target || '') || version === 2 && c.contact !== null && !uuidPattern.test(c.contact || '')
    || !/^[a-f0-9]{24}$/.test(s.nonce || '') || !/^(?:[a-f0-9]{2}){16,2016}$/.test(s.ciphertext || '')) throw Error('Invalid protected message.');
  const context = version === 1 ? { version, id:c.id, sender:c.sender, target:c.target, createdAt:c.createdAt } : { version, id:c.id, sender:c.sender, contact:c.contact, createdAt:c.createdAt };
  const canonical = `CIFER:WA:${version}:${Buffer.from(JSON.stringify({ context, sealed:{nonce:s.nonce,ciphertext:s.ciphertext} })).toString('base64')}\nOpen in CIFER: https://hub.cifer.network/whatsapp/${version === 2 ? 'extension/' : ''}message/${c.id}`;
  if (canonical !== value) throw Error('Invalid protected message.');
  return context;
}
export function extract(body) {
  for (let n = 0; n < 8 && body; n++) {
    if (body.viewOnceMessage || body.viewOnceMessageV2 || body.viewOnceMessageV2Extension) return null;
    const wrapped = body.ephemeralMessage || body.editedMessage || body.documentWithCaptionMessage;
    if (!wrapped) break; body = wrapped.message;
  }
  if (!body) return null;
  const text = body.conversation ?? body.extendedTextMessage?.text ?? body.imageMessage?.caption ?? body.videoMessage?.caption ?? body.documentMessage?.caption;
  const media = body.imageMessage ? 'Photo' : body.videoMessage ? 'Video' : body.audioMessage ? 'Audio' : body.documentMessage ? 'Document' : body.stickerMessage ? 'Sticker' : null;
  const context = body.extendedTextMessage?.contextInfo ?? body.imageMessage?.contextInfo ?? body.videoMessage?.contextInfo ?? body.documentMessage?.contextInfo ?? body.audioMessage?.contextInfo ?? body.stickerMessage?.contextInfo;
  const expiration = Number(context?.expiration || 0);
  if (!Number.isSafeInteger(expiration) || expiration < 0) return null;
  if (typeof text === 'string' && text.startsWith('CIFER:WA:')) {
    try { const c = protectedWire(text); return { wire: text, kind: 'protected', expiration, protectedCreatedAt: c.createdAt }; }
    catch { return { text: 'Unsupported or damaged protected message.', kind: 'unsupported', expiration }; }
  }
  if (typeof text !== 'string' && !media) return null;
  const fits = typeof text === 'string' && Buffer.byteLength(text) <= 16000;
  return { text: fits ? text : media ? `${media} · open in WhatsApp` : 'Long message · open in WhatsApp', ...(media ? { label: `${media} · open in WhatsApp` } : {}), kind: fits ? 'ordinary' : 'unsupported', expiration };
}
export function sanitizeMessage(message, now = Date.now()) {
  const key = keyOf(message.key), createdAt = Number(message.messageTimestamp) * 1000;
  if (!key || !Number.isSafeInteger(createdAt) || createdAt <= now - WEEK || createdAt > now + 60000) return null;
  const body = extract(message.message); if (!body) return null;
  const expiresAt = Math.min(createdAt + WEEK, body.expiration ? createdAt + body.expiration * 1000 : Infinity, body.protectedCreatedAt ? body.protectedCreatedAt + WEEK : Infinity);
  if (expiresAt <= now) return null;
  return { ...key, ...body, createdAt, expiresAt, status: message.status, name: typeof message.pushName === 'string' ? message.pushName.slice(0, 120) : undefined };
}
export function receipt(status) { return status === 4 || status === 5 ? 'read' : status === 3 ? 'delivered' : status === 2 ? 'sent' : null; }
export function strongest(a = 'sent', b = 'sent') { const ranks = { queued: 0, uncertain: 0, sent: 1, delivered: 2, read: 3 }; return (ranks[a] ?? 0) >= (ranks[b] ?? 0) ? a : b; }
export function publicKey(value) {
  exact(value, ['principal', 'secretId', 'publicKey']);
  if (!uuidPattern.test(value.principal) || !/^\d{1,80}$/.test(value.secretId) || typeof value.publicKey !== 'string' || value.publicKey.length !== 1580 || Buffer.from(value.publicKey, 'base64').length !== 1184 || Buffer.from(value.publicKey, 'base64').toString('base64') !== value.publicKey) throw Error('Invalid contact key.');
}
export function validateBinding(value, account, chat) {
  exact(value, ['version', 'principal', 'chat', 'contactId', 'revision', 'owner', 'peer', 'confirmedAt']);
  if (value.version !== 1 || value.principal !== account || value.chat !== chat || !uuidPattern.test(value.contactId) || !uuidPattern.test(value.revision) || !Number.isSafeInteger(value.confirmedAt) || value.confirmedAt > Date.now() + 60000) throw Error('Invalid contact binding.');
  publicKey(value.owner); publicKey(value.peer); if (value.owner.principal !== account || value.peer.principal === account) throw Error('Invalid contact binding.');
  return value;
}
const wrapped = value => { exact(value, ['encrypted', 'cifer']); if (typeof value.encrypted !== 'string' || !/^(?:[a-f0-9]{2}){1,4096}$/.test(value.encrypted) || typeof value.cifer !== 'string' || !/^[a-f0-9]{2208}$/.test(value.cifer)) throw Error('Invalid protected descriptor.'); };
export function validatePrepared(value, account, chat, wire) {
  exact(value, ['version', 'principal', 'chat', 'owner', 'binding', 'wire', 'recovery', 'recipientGrant']);
  if (Buffer.byteLength(JSON.stringify(value)) > 49152 || value.version !== 1 || value.principal !== account || value.chat !== chat || value.wire !== wire) throw Error('Invalid prepared message.');
  const c = protectedWire(wire); publicKey(value.owner); wrapped(value.recovery);
  if (c.version !== 2 || c.sender !== account || value.owner.principal !== account) throw Error('Invalid prepared sender.');
  if (value.binding) { validateBinding(value.binding, account, chat); if (c.contact !== value.binding.contactId || canonicalJSON(value.owner)!==canonicalJSON(value.binding.owner)) throw Error('Protected contact changed.'); wrapped(value.recipientGrant); }
  else if (c.contact !== null || value.recipientGrant !== null) throw Error('Invalid protected recipient.');
  return value;
}
