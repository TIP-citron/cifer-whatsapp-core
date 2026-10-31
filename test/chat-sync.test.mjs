import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { randomUUID } from 'node:crypto';
import { Store } from '../src/store.mjs';
import { Inbox } from '../src/inbox.mjs';
import { ChatSync, CHAT_SYNC_NAMESPACES } from '../src/chat-sync.mjs';
import { keyOf, isGroup, messageId, sanitizeMessage, WEEK } from '../src/content.mjs';

const CHAT = '15551112222@s.whatsapp.net', GROUP = '120363123456789@g.us', A = '15551113333@s.whatsapp.net', B = '15551114444@s.whatsapp.net';
const turn = () => new Promise(resolve => setImmediate(resolve));
function raw(id, { chat = CHAT, fromMe = false, participant, timestamp = Math.floor(Date.now() / 1000), message = { conversation: 'fixture body' }, status } = {}) {
  return { key: { id, remoteJid: chat, fromMe, ...(participant ? { participant } : {}) }, messageTimestamp: timestamp, message, ...(status !== undefined ? { status } : {}) };
}
async function setup(t) {
  const directory = await mkdtemp(join(tmpdir(), 'cifer-chat-sync-')), key = Buffer.alloc(32, 7), binding = { account: 'synthetic', connection: 'synthetic' };
  const e = { store: new Store(directory, key, binding), closed: false, stopping: false, closing: false, epoch: 1, state: 'connected', appStateReady: true, inflight: new Set(), notices: [], aliases: new Map() };
  e.metadata = chat => e.notices.push(chat);
  e.canonical = async chat => e.aliases.get(chat) || chat;
  e.cleanKey = async key => {
    const clean = keyOf(key); if (!clean) return null;
    clean.chat = await e.canonical(clean.chat);
    if (isGroup(clean.chat) && clean.participant) clean.participant = await e.canonical(clean.participant);
    return clean;
  };
  e.socket = { reads: [], patches: [], histories: [], async readMessages(keys) { this.reads.push(keys); }, async chatModify(value, chat) { this.patches.push({ value, chat }); }, async fetchMessageHistory(count, key, at) { this.histories.push({ count, key, at }); return 'HISTORY'; } };
  const init = () => { e.inbox = new Inbox(e.store, chat => e.notices.push(chat), {}, key => e.sync?.forget(key)); e.sync = new ChatSync(e); };
  init();
  e.ingest = async (messages, options) => { await e.sync.capture(messages, options); for (const message of messages) { const clean = sanitizeMessage(message); if (clean) e.inbox.ingest(clean, options); } };
  e.reopen = () => { e.sync.pause(); e.store.close(); e.store = new Store(directory, key, binding); e.epoch++; init(); };
  t.after(async () => { e.closed = true; e.sync.pause(); e.store.close(); await rm(directory, { recursive: true, force: true }); });
  return e;
}

test('raw metadata captures filtered and expired incoming messages without content and survives content pruning', async t => {
  const e = await setup(t), timestamp = Math.floor((Date.now() - WEEK - 10000) / 1000);
  const secret = 'NEVER-RETAIN-THIS-CONTENT';
  const message = raw('OLD-IN', { chat: GROUP, participant: A, timestamp, message: { viewOnceMessage: { message: { imageMessage: { caption: secret, mediaKey: secret } } } } });
  const snapshot = e.sync.snapshot([message]); assert.ok(!JSON.stringify(snapshot).includes(secret));
  await e.sync.capture(snapshot); e.inbox.prune();
  assert.equal(e.store.totals('message').count, 0);
  const rows = e.sync.rows(GROUP); assert.equal(rows.length, 1); assert.equal(rows[0].protocolKey.participant, A);
  assert.ok(!JSON.stringify(e.store.list('chat-sync-range')).includes(secret));
  e.reopen(); assert.equal(e.sync.rows(GROUP).length, 1); assert.equal(e.sync.range(GROUP), null);
});

test('per-chat overflow caps metadata and makes archive ranges incomplete', async t => {
  const e = await setup(t), now = Math.floor(Date.now() / 1000);
  await e.sync.capture([raw('ANCHOR', { timestamp: now - 130 }), ...Array.from({ length: 128 }, (_, n) => raw('K' + n, { timestamp: now - n, fromMe: true }))]);
  assert.equal(e.sync.rows(CHAT).length, 128); assert.equal(e.sync.state(CHAT).incomplete, true); assert.equal(e.sync.range(CHAT), null);
  e.socket.fetchMessageHistory = async () => { throw Error('unavailable'); };
  await assert.rejects(e.sync.setArchived({ chat: CHAT, archived: true, operationId: randomUUID() }), /metadata is unavailable/);
  assert.equal(e.socket.patches.length, 0);
});

test('normal incoming traffic compacts to its newest received anchor rather than overflowing', async t => {
  const e = await setup(t), now = Math.floor(Date.now() / 1000);
  await e.sync.capture(Array.from({ length: 200 }, (_, n) => raw('INCOMING' + n, { timestamp: now - n })));
  assert.equal(e.sync.rows(CHAT).length, 1); assert.equal(e.sync.state(CHAT).incomplete, false);
  assert.equal(e.sync.range(CHAT).messages[0].key.id, 'INCOMING0');
});

test('old history cannot establish a current tail without a matching chat timestamp', async t => {
  const e = await setup(t), timestamp = Math.floor(Date.now() / 1000) - 500;
  await e.sync.capture([raw('OLD', { timestamp })], { history: true });
  assert.equal(e.sync.range(CHAT), null);
  e.sync.chatUpdate({ conversationTimestamp: timestamp + 100 }, CHAT); assert.equal(e.sync.range(CHAT), null);
  await e.sync.capture([raw('TAIL', { timestamp: timestamp + 100 })], { history: true });
  assert.ok(e.sync.range(CHAT));
});

test('global capacity evicts metadata and marks affected chats incomplete', async t => {
  const e = await setup(t);
  e.store.transaction(() => { for (let n = 0; n < 10001; n++) {
    const chat = `${15550000000 + Math.floor(n / 100)}@s.whatsapp.net`, row = { chat, id: 'G' + n, fromMe: false, protocolKey: { remoteJid: chat, id: 'G' + n, fromMe: false }, messageTimestamp: 100000 + n, knownUnread: false };
    e.store.set('chat-sync-range', JSON.stringify([chat, row.id, false, null]), row, { tag: chat, created: row.messageTimestamp });
  } });
  e.sync.limitRanges(); assert.equal(e.store.totals('chat-sync-range').count, 10000);
  assert.equal(e.sync.state('15550000000@s.whatsapp.net').incomplete, true);
});

test('archive requires ready app state, uses raw incoming group key and honors authoritative keep-archived changes', async t => {
  const e = await setup(t), timestamp = Math.floor(Date.now() / 1000);
  e.inbox.contact(GROUP, { name: 'Team', customField: 'preserved' });
  await e.sync.capture([raw('IN', { chat: GROUP, participant: A, timestamp: timestamp - 10 }), raw('OUT', { chat: GROUP, fromMe: true, timestamp })]);
  e.appStateReady = false;
  await assert.rejects(e.sync.setArchived({ chat: GROUP, archived: true, operationId: randomUUID() }), /synchronization/);
  assert.equal(e.socket.patches.length, 0); e.appStateReady = true;
  const operationId = randomUUID(), result = await e.sync.setArchived({ chat: GROUP, archived: true, operationId });
  assert.deepEqual(result, { chat: GROUP, archived: true, operationId, state: 'sent' });
  const patch = e.socket.patches[0].value;
  assert.equal(patch.archive, true); assert.equal(patch.lastMessages.lastMessageTimestamp, timestamp);
  assert.equal(patch.lastMessages.messages.find(row => row.key.id === 'IN').key.participant, A);
  assert.equal(e.store.get('chat', GROUP).customField, 'preserved');
  await e.sync.capture([raw('NEW', { chat: GROUP, participant: B })]);
  e.sync.chatUpdate({ name: 'same' }, GROUP); assert.equal(e.store.get('chat', GROUP).archived, true);
  e.sync.chatUpdate({ archived: false }, GROUP); assert.equal(e.store.get('chat', GROUP).archived, false);
  assert.equal(e.store.get('chat', GROUP).name, 'Team');
});

test('uncertain archive operations persist and never replay, while authoritative state reconciles them', async t => {
  const e = await setup(t); await e.sync.capture([raw('IN')]);
  e.socket.chatModify = async function (value, chat) { this.patches.push({ value, chat }); throw Error('lost acknowledgement'); };
  const args = { chat: CHAT, archived: true, operationId: randomUUID() };
  assert.equal((await e.sync.setArchived(args)).state, 'uncertain');
  assert.notEqual(e.store.get('chat', CHAT).archived, true); e.reopen();
  assert.equal((await e.sync.setArchived(args)).state, 'uncertain'); assert.equal(e.socket.patches.length, 1);
  await assert.rejects(e.sync.setArchived({ ...args, archived: false }), /already refers/);
  e.sync.chatUpdate({ archived: true }, CHAT);
  assert.equal((await e.sync.setArchived(args)).state, 'sent'); assert.equal(e.socket.patches.length, 1);
});

test('concurrent archive operation retries cannot dispatch twice', async t => {
  const e = await setup(t); await e.sync.capture([raw('IN')]);
  let finish; e.socket.chatModify = async function (value, chat) { this.patches.push({ value, chat }); await new Promise(resolve => { finish = resolve; }); };
  const args = { chat: CHAT, archived: true, operationId: randomUUID() }, first = e.sync.setArchived(args);
  await turn(); assert.equal((await e.sync.setArchived(args)).state, 'uncertain'); finish();
  assert.equal((await first).state, 'sent'); assert.equal(e.socket.patches.length, 1);
});

test('metadata refresh is bounded, correlated, requires completion and cannot invent an incoming key', async t => {
  const e = await setup(t); await e.sync.capture([raw('IN')]); e.sync.invalidate(CHAT);
  const action = e.sync.setArchived({ chat: CHAT, archived: true, operationId: randomUUID() }); await turn();
  assert.equal(e.socket.histories[0].count, 128); assert.ok(e.socket.histories[0].at < 100000000000);
  await e.sync.capture([raw('IN')], { history: true, requestId: 'HISTORY', progress: 100 });
  assert.equal((await action).state, 'sent');
  const other = '15551115555@s.whatsapp.net'; await e.sync.capture([raw('OUT', { chat: other, fromMe: true })]);
  e.socket.fetchMessageHistory = async () => { throw Error('unavailable'); };
  await assert.rejects(e.sync.setArchived({ chat: other, archived: true, operationId: randomUUID() }), /metadata is unavailable/);
});

test('malformed raw keys invalidate a chat even through the snapshot boundary', async t => {
  const e = await setup(t); await e.sync.capture([raw('GOOD', { chat: GROUP, participant: A })]);
  await e.sync.capture(e.sync.snapshot([raw('MISSING-PARTICIPANT', { chat: GROUP })]));
  assert.equal(e.sync.state(GROUP).incomplete, true); assert.equal(e.sync.range(GROUP), null);
});

test('participant-aware visible reads do not mark same-ID group messages from other participants', async t => {
  const e = await setup(t); await e.ingest([raw('SAME', { chat: GROUP, participant: A }), raw('SAME', { chat: GROUP, participant: B })]);
  await assert.rejects(e.sync.markRead({ chat: GROUP, ids: ['SAME'] }), /participant-aware/);
  await e.sync.markRead({ chat: GROUP, messages: [{ id: 'SAME', fromMe: false, participant: A }] });
  assert.deepEqual(e.socket.reads[0], [{ remoteJid: GROUP, id: 'SAME', fromMe: false, participant: A }]);
  assert.equal(e.store.get('message', messageId({ chat: GROUP, id: 'SAME', fromMe: false, participant: B })).readAt, null);
  assert.equal(e.socket.patches.length, 0);
  await e.sync.markRead({ chat: GROUP, messages: [{ id: 'SAME', fromMe: false, participant: B }] });
  assert.equal(e.socket.patches.length, 1); assert.equal(e.socket.patches[0].value.markRead, true);
});

test('filtered incoming messages prevent a whole-chat read patch', async t => {
  const e = await setup(t); await e.ingest([raw('VISIBLE')]);
  await e.sync.capture([raw('HIDDEN', { message: { viewOnceMessage: { message: { conversation: 'private' } } } })]);
  await e.sync.markRead({ chat: CHAT, ids: ['VISIBLE'] });
  assert.equal(e.socket.reads.length, 1); assert.equal(e.socket.patches.length, 0);
});

test('offline read intents survive restart and retry idempotently without repeated no-op events', async t => {
  const e = await setup(t); await e.ingest([raw('VISIBLE')]); e.state = 'disconnected';
  assert.equal((await e.sync.markRead({ chat: CHAT, ids: ['VISIBLE'] })).state, 'pending');
  assert.equal(e.store.totals('chat-sync-read').count, 1); const count = e.notices.length;
  await e.sync.markRead({ chat: CHAT, ids: ['VISIBLE'] }); assert.equal(e.notices.length, count);
  e.reopen(); e.state = 'connected';
  let attempts = 0; e.socket.readMessages = async function (keys) { this.reads.push(keys); if (++attempts === 1) throw Error('temporary failure'); };
  await e.sync.retryReads(); assert.equal(e.store.totals('chat-sync-read').count, 1);
  await e.sync.retryReads(); assert.equal(e.store.totals('chat-sync-read').count, 0); assert.equal(attempts, 2);
  assert.equal(e.socket.patches.length, 0, 'restart discards old whole-chat cutoff until the current tail is proven');
});

test('new unread arrivals during receipt delivery block the stale whole-chat cutoff', async t => {
  const e = await setup(t); await e.ingest([raw('VISIBLE')]);
  let finish; e.socket.readMessages = async function (keys) { this.reads.push(keys); await new Promise(resolve => { finish = resolve; }); };
  const reading = e.sync.markRead({ chat: CHAT, ids: ['VISIBLE'] }); await turn();
  await e.ingest([raw('AFTER')]); finish(); await reading;
  assert.equal(e.socket.patches.length, 0);
  assert.equal(e.store.get('message', messageId({ chat: CHAT, id: 'AFTER', fromMe: false })).readAt, null);
});

test('stale-generation receipt completion cannot consume durable intents for the next connection', async t => {
  const e = await setup(t); await e.ingest([raw('VISIBLE')]);
  let finish; e.socket.readMessages = async () => new Promise(resolve => { finish = resolve; });
  const reading = e.sync.markRead({ chat: CHAT, ids: ['VISIBLE'] }); await turn();
  e.sync.pause(); e.epoch++; const old = e.socket; e.socket = { ...old, reads: [], async readMessages(keys) { this.reads.push(keys); } };
  finish(); await reading; assert.equal(e.store.totals('chat-sync-read').count, 1);
  await e.sync.retryReads(); assert.equal(e.socket.reads.length, 1); assert.equal(e.store.totals('chat-sync-read').count, 0);
});

test('phone read updates are local, archived guards stay quiet and reset removes sync metadata', async t => {
  const e = await setup(t); await e.ingest([raw('VISIBLE')]); e.inbox.contact(CHAT, { archived: true });
  assert.deepEqual(await e.sync.markRead({ chat: CHAT, ids: ['VISIBLE'], onlyIfUnarchived: true }), { ok: true, skipped: true });
  assert.equal(e.socket.reads.length, 0);
  e.sync.chatUpdate({ unreadCount: 0 }, CHAT); assert.equal(e.sync.knownUnread(CHAT), false); assert.equal(e.socket.reads.length, 0);
  e.sync.reset(); for (const ns of CHAT_SYNC_NAMESPACES) assert.equal(e.store.totals(ns).count, 0);
});

test('participant alias migration preserves wire identity for future read receipts', async t => {
  const e = await setup(t), lid = '123456789012@lid';
  await e.ingest([raw('OPAQUE', { chat: GROUP, participant: lid })]);
  e.aliases.set(lid, A); e.inbox.rebindParticipant(lid, A); e.sync.rebindParticipant(lid, A);
  await e.sync.markRead({ chat: GROUP, messages: [{ id: 'OPAQUE', fromMe: false, participant: A }] });
  assert.equal(e.socket.reads[0][0].participant, lid); assert.equal(e.sync.rows(GROUP)[0].participant, A);
});

test('archive drains queued offline messages before taking its final protocol range', async t => {
  const e = await setup(t), now = Math.floor(Date.now() / 1000);
  await e.sync.capture([raw('BEFORE', { timestamp: now - 100 })]);
  let drained = false;
  e.flushEvents = async epoch => {
    assert.equal(epoch, e.epoch);
    await e.sync.capture([raw('OFFLINE-NEW', { timestamp: now })]); drained = true;
  };
  const result = await e.sync.setArchived({ chat: CHAT, archived: true, operationId: randomUUID() });
  assert.equal(result.state, 'sent'); assert.equal(drained, true);
  assert.deepEqual(e.socket.patches[0].value.lastMessages.messages.map(row => row.key.id), ['OFFLINE-NEW']);
});

test('whole-chat read waits for queued incoming captures and reevaluates its cutoff', async t => {
  const e = await setup(t); await e.ingest([raw('VISIBLE')]);
  e.flushEvents = async () => { await e.ingest([raw('QUEUED-INCOMING')]); };
  await e.sync.markRead({ chat: CHAT, ids: ['VISIBLE'] });
  assert.equal(e.socket.reads.length, 1); assert.equal(e.socket.patches.length, 0);
  assert.equal(e.store.get('message', messageId({ chat: CHAT, id: 'QUEUED-INCOMING', fromMe: false })).readAt, null);
});

test('queue-drain generation changes fail archive without dispatch', async t => {
  const e = await setup(t); await e.sync.capture([raw('IN')]);
  e.flushEvents = async () => { e.sync.pause(); e.epoch++; throw Error('WhatsApp connection changed.'); };
  await assert.rejects(e.sync.setArchived({ chat: CHAT, archived: true, operationId: randomUUID() }), /connection changed/);
  assert.equal(e.socket.patches.length, 0); assert.equal(e.store.totals('chat-sync-archive').count, 0);
});

test('explicitly viewed imported history sends a receipt despite its local history-read marker', async t => {
  const e = await setup(t); await e.ingest([raw('HISTORY-UNREAD')], { history: true });
  const key = { chat: CHAT, id: 'HISTORY-UNREAD', fromMe: false };
  assert.ok(e.store.get('message', messageId(key)).readAt);
  assert.equal(e.store.get('message', messageId(key)).delivery, 'sent');
  const notices = e.notices.length;
  await e.sync.markRead({ chat: CHAT, ids: ['HISTORY-UNREAD'] });
  assert.deepEqual(e.socket.reads[0], [{ remoteJid: CHAT, id: 'HISTORY-UNREAD', fromMe: false }]);
  assert.equal(e.notices.length, notices); assert.equal(e.store.totals('chat-sync-read-confirmed').count, 1);
  e.reopen(); await e.sync.markRead({ chat: CHAT, ids: ['HISTORY-UNREAD'] });
  assert.equal(e.socket.reads.length, 1);
});

test('upgrade from local-only seen markers still queues an explicit visible receipt offline', async t => {
  const e = await setup(t); await e.ingest([raw('OLD-LOCAL-READ')]);
  e.inbox.read(CHAT, ['OLD-LOCAL-READ']); e.reopen(); e.state = 'disconnected';
  const notices = e.notices.length;
  const result = await e.sync.markRead({ chat: CHAT, ids: ['OLD-LOCAL-READ'] });
  assert.equal(result.state, 'pending'); assert.equal(e.store.totals('chat-sync-read').count, 1);
  await e.sync.markRead({ chat: CHAT, ids: ['OLD-LOCAL-READ'] });
  assert.equal(e.store.totals('chat-sync-read').count, 1); assert.equal(e.notices.length, notices);
  e.reopen(); e.state = 'connected'; await e.sync.retryReads();
  assert.equal(e.socket.reads.length, 1); assert.equal(e.store.totals('chat-sync-read-confirmed').count, 1);
});

test('implicit read-all only selects locally unread active messages', async t => {
  const e = await setup(t); await e.ingest([raw('HISTORY')], { history: true }); await e.ingest([raw('UNREAD')]);
  await e.sync.markRead({ chat: CHAT });
  assert.deepEqual(e.socket.reads.flat().map(row => row.id), ['UNREAD']);
  await e.ingest([raw('ARCHIVED-UNREAD')]); e.inbox.contact(CHAT, { archived: true });
  assert.equal((await e.sync.markRead({ chat: CHAT })).skipped, true);
  assert.deepEqual(e.socket.reads.flat().map(row => row.id), ['UNREAD']);
});

test('restart clears old tail provenance and cutoffs until new authoritative head evidence arrives', async t => {
  const e = await setup(t), timestamp = Math.floor(Date.now() / 1000);
  await e.ingest([raw('OLD-TAIL', { timestamp })]); e.state = 'disconnected';
  await e.sync.markRead({ chat: CHAT, ids: ['OLD-TAIL'] });
  assert.ok(e.store.get('chat-sync-cutoff', CHAT)); e.reopen(); e.state = 'connected';
  assert.equal(e.sync.state(CHAT).tailKnown, false); assert.equal(e.sync.state(CHAT).remoteTimestamp, 0);
  assert.equal(e.store.get('chat-sync-cutoff', CHAT), null); assert.equal(e.sync.rows(CHAT).length, 1);
  await e.sync.capture([raw('OLD-TAIL', { timestamp })], { history: true });
  e.socket.fetchMessageHistory = async () => { throw Error('no fresh history'); };
  const args = { chat: CHAT, archived: true, operationId: randomUUID() };
  await assert.rejects(e.sync.setArchived(args), /metadata is unavailable/); assert.equal(e.socket.patches.length, 0);
  e.sync.chatUpdate({ conversationTimestamp: timestamp }, CHAT);
  assert.equal((await e.sync.setArchived(args)).state, 'sent');
});

test('pending read intents expire with their source messages while content-free ranges remain', async t => {
  const e = await setup(t); await e.ingest([raw('EXPIRING')]);
  const key = { chat: CHAT, id: 'EXPIRING', fromMe: false }, message = e.store.get('message', messageId(key)), now = Date.now();
  e.inbox.save({ ...message, expiresAt: now + 1000 }); e.state = 'disconnected';
  await e.sync.markRead({ chat: CHAT, ids: ['EXPIRING'] });
  assert.equal(e.store.list('chat-sync-read')[0].value.expiresAt, now + 1000);
  const realNow = Date.now; Date.now = () => now + 2000;
  try {
    e.state = 'connected'; await e.sync.retryReads(); e.inbox.prune();
    assert.equal(e.socket.reads.length, 0); assert.equal(e.store.totals('chat-sync-read').count, 0);
    assert.equal(e.store.totals('chat-sync-read-confirmed').count, 0); assert.equal(e.sync.rows(CHAT).length, 1);
  } finally { Date.now = realNow; }
});

test('message deletion removes pending and confirmed receipts without discarding archive range metadata', async t => {
  const e = await setup(t); await e.ingest([raw('PENDING'), raw('CONFIRMED')]); e.state = 'disconnected';
  await e.sync.markRead({ chat: CHAT, ids: ['PENDING'] });
  e.inbox.tombstone({ chat: CHAT, id: 'PENDING', fromMe: false });
  assert.equal(e.store.totals('chat-sync-read').count, 0); assert.equal(e.sync.rows(CHAT).length, 2);
  e.state = 'connected'; await e.sync.markRead({ chat: CHAT, ids: ['CONFIRMED'] });
  assert.equal(e.store.totals('chat-sync-read-confirmed').count, 1);
  e.inbox.tombstone({ chat: CHAT, id: 'CONFIRMED', fromMe: false });
  assert.equal(e.store.totals('chat-sync-read-confirmed').count, 0); assert.equal(e.store.get('chat-sync-cutoff', CHAT), null);
  assert.equal(e.sync.rows(CHAT).length, 2);
});

test('deletion during a receipt request cannot recreate a confirmation after acknowledgement', async t => {
  const e = await setup(t); await e.ingest([raw('IN-FLIGHT')]);
  let finish; e.socket.readMessages = async () => new Promise(resolve => { finish = resolve; });
  const request = e.sync.markRead({ chat: CHAT, ids: ['IN-FLIGHT'] }); await turn();
  e.inbox.tombstone({ chat: CHAT, id: 'IN-FLIGHT', fromMe: false }); finish(); await request;
  assert.equal(e.store.totals('chat-sync-read').count, 0); assert.equal(e.store.totals('chat-sync-read-confirmed').count, 0);
  assert.equal(e.socket.patches.length, 0);
});
