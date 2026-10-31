import test from 'node:test';
import assert from 'node:assert/strict';
import { proto } from 'baileys';
import { processSyncAction } from 'baileys/lib/Utils/chat-utils.js';
import { fixture, fixtureMessage, drain, PEER, SELF, ACCOUNT } from './helpers.mjs';
import { messageId, WEEK } from '../src/content.mjs';
import { Store } from '../src/store.mjs';

const other = '15550000003@s.whatsapp.net';
const key = (id, chat = PEER, fromMe = false) => ({ id, remoteJid: chat, fromMe });
const status = (socket, id, value, chat = PEER, fromMe = false) => socket.ev.emit('messages.update', [{ key: key(id, chat, fromMe), update: { status: value } }]);
const incoming = (socket, messages, type = 'notify') => socket.ev.emit('messages.upsert', { messages, type });
const unread = async f => (await f.engine.request('status')).unreadTotal;

test('phone read and played receipts clear only matching incoming notifications and persist after restart', async t => {
  const f = await fixture(t), socket = await f.connect();
  incoming(socket, [fixtureMessage('ONE'), fixtureMessage('TWO'), fixtureMessage('ONE', 'other chat', { key: key('ONE', other) }), fixtureMessage('ONE', 'outgoing', { key: key('ONE', PEER, true) })]);
  await drain(); assert.equal(await unread(f), 3);
  status(socket, 'ONE', proto.WebMessageInfo.Status.READ, PEER, true); await drain(); assert.equal(await unread(f), 3);
  f.events.length = 0;
  status(socket, 'ONE', proto.WebMessageInfo.Status.READ); status(socket, 'TWO', proto.WebMessageInfo.Status.PLAYED); await drain();
  assert.equal(await unread(f), 1);
  assert.deepEqual((await f.engine.request('status')).unreadChats.map(x => x.chat), [other]);
  assert.ok(f.events.some(e => e.type === 'metadata')); assert.ok(!f.events.some(e => e.type === 'changed'));
  await f.reopen(); assert.equal(await unread(f), 1);
  incoming(f.sockets.at(-1), [fixtureMessage('ONE'), fixtureMessage('TWO')]); await drain(); assert.equal(await unread(f), 1);
});

test('read receipt arriving before its message survives restart and opaque identity reconciliation', async t => {
  const f = await fixture(t), socket = await f.connect(), lid = '1234567890@lid';
  status(socket, 'LATE', proto.WebMessageInfo.Status.READ, lid); await drain(); await f.reopen();
  const next = f.sockets.at(-1); next.ev.emit('contacts.upsert', [{ id: lid, phoneNumber: PEER }]); await drain();
  incoming(next, [fixtureMessage('LATE')]); await drain();
  const page = await f.engine.request('messages', { chat: PEER });
  assert.equal(page.messages.length, 1); assert.equal(page.messages[0].delivery, 'read'); assert.equal(await unread(f), 0);
});

test('already-read upserts and append replays stay read while new notify messages remain unread', async t => {
  const f = await fixture(t), socket = await f.connect();
  incoming(socket, [fixtureMessage('READ', 'already read', { status: proto.WebMessageInfo.Status.READ })]);
  incoming(socket, [fixtureMessage('APPEND')], 'append'); incoming(socket, [fixtureMessage('NEW')]); await drain();
  assert.equal(await unread(f), 1);
  incoming(socket, [fixtureMessage('NEW', 'seen later', { status: proto.WebMessageInfo.Status.READ })]); await drain(); assert.equal(await unread(f), 0);
});

test('real WhatsApp mark-chat-read clears the selected chat without clearing subsequent arrivals or other chats', async t => {
  const f = await fixture(t), socket = await f.connect(), lid = '1234567890@lid';
  incoming(socket, [fixtureMessage('OLD'), fixtureMessage('OTHER', 'other', { key: key('OTHER', other) })]);
  socket.ev.emit('contacts.upsert', [{ id: lid, phoneNumber: PEER }]); await drain();
  processSyncAction({ index: ['markChatAsRead', lid], syncAction: { value: { markChatAsReadAction: { read: true } } } }, socket.ev, { id: SELF });
  await drain(); assert.equal(await unread(f), 1);
  incoming(socket, [fixtureMessage('AFTER')]); await drain(); assert.equal(await unread(f), 2);
  for (const unreadCount of [null, undefined, -1, 2]) socket.ev.emit('chats.update', [{ id: PEER, unreadCount }]);
  await drain(); assert.equal(await unread(f), 2);
  socket.ev.emit('messaging-history.set', { contacts: [], messages: [], chats: [{ id: PEER, unreadCount: 0 }] }); await drain(); assert.equal(await unread(f), 1);
});

test('upgrade repairs persisted incoming read receipts without clearing messages that have not been read', async t => {
  const f = await fixture(t), socket = await f.connect(); incoming(socket, [fixtureMessage('READ-BEFORE-UPGRADE'), fixtureMessage('UNREAD')]); await drain(); await f.engine.close();
  const store = new Store(f.directory, f.config.key, { account: f.config.account, connection: f.config.connection });
  try {
    const id = messageId({ chat: PEER, id: 'READ-BEFORE-UPGRADE', fromMe: false }), row = store.get('message', id);
    store.set('message', id, { ...row, delivery: 'read', readAt: null }, { expires: row.expiresAt, created: row.createdAt, tag: row.chat, unread: true });
    store.remove('system', 'incoming-read-state-v1');
  } finally { store.close(); }
  await f.reopen(); assert.equal(await unread(f), 1);
});

test('read updates stay within their account and old socket callbacks are fenced after restart', async t => {
  const a = await fixture(t), b = await fixture(t, { account: ACCOUNT.replace(/1$/, '2') }), sa = await a.connect(), sb = await b.connect();
  incoming(sa, [fixtureMessage('SAME-ID')]); incoming(sb, [fixtureMessage('SAME-ID')]); await drain();
  status(sa, 'SAME-ID', proto.WebMessageInfo.Status.READ); await drain(); assert.equal(await unread(a), 0); assert.equal(await unread(b), 1);
  await b.reopen(); status(sb, 'SAME-ID', proto.WebMessageInfo.Status.READ); sb.ev.emit('chats.update', [{ id: PEER, unreadCount: 0 }]); await drain(); assert.equal(await unread(b), 1);
});

test('pending read metadata is removed by deletion, expiry and disconnect', async t => {
  const f = await fixture(t); await f.connect();
  const inspect = async fn => {
    await f.engine.close(); const store = new Store(f.directory, f.config.key, { account: f.config.account, connection: f.config.connection });
    try { fn(store); } finally { store.close(); await f.reopen(); }
  };
  const id = messageId({ chat: PEER, id: 'PENDING', fromMe: false });
  status(f.sockets.at(-1), 'PENDING', proto.WebMessageInfo.Status.READ); await drain();
  await inspect(store => assert.ok(store.get('read-receipt', id)));
  f.sockets.at(-1).ev.emit('messages.delete', { keys: [key('PENDING')] }); await drain();
  await inspect(store => assert.equal(store.get('read-receipt', id), null));
  status(f.sockets.at(-1), 'EXPIRE', proto.WebMessageInfo.Status.READ); await drain();
  const real = Date.now, now = real(); Date.now = () => now + WEEK + 1;
  try { await f.reopen(); await inspect(store => assert.equal(store.totals('read-receipt').count, 0)); } finally { Date.now = real; }
  status(f.sockets.at(-1), 'DISCONNECT', proto.WebMessageInfo.Status.READ); await drain(); await f.engine.disconnect();
  await inspect(store => assert.equal(store.totals('read-receipt').count, 0));
});

test('canonical read receipt applies when its opaque message later merges into the chat', async t => {
  const f = await fixture(t), socket = await f.connect(), lid = '1234567890@lid';
  status(socket, 'MAPPED', proto.WebMessageInfo.Status.READ);
  incoming(socket, [fixtureMessage('MAPPED', 'opaque', { key: key('MAPPED', lid) })]); await drain(); assert.equal(await unread(f), 1);
  socket.ev.emit('contacts.upsert', [{ id: lid, phoneNumber: PEER }]); await drain();
  assert.equal(await unread(f), 0); assert.equal((await f.engine.request('messages', { chat: PEER })).messages[0].delivery, 'read');
});
