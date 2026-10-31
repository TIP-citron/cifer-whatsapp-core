import { assertChat, isGroup, normalizePeer, peerPattern, messageId, exact, uuidPattern, WEEK } from './content.mjs';

export const CHAT_SYNC_NAMESPACES = ['chat-sync-range', 'chat-sync-state', 'chat-sync-archive', 'chat-sync-read', 'chat-sync-read-confirmed', 'chat-sync-cutoff'];
const RANGE = 'chat-sync-range', STATE = 'chat-sync-state', ARCHIVE = 'chat-sync-archive', READ = 'chat-sync-read', CONFIRMED = 'chat-sync-read-confirmed', CUTOFF = 'chat-sync-cutoff';
const PER_CHAT = 128, GLOBAL = 10000, WAIT = 5000;
const idPattern = /^[\w-]{1,128}$/;
const identity = row => JSON.stringify([row.chat, row.id, row.fromMe, isGroup(row.chat) && !row.fromMe ? row.participant : null]);
const stamp = raw => { const value = Number(raw); return Number.isSafeInteger(value) && value > 0 && value <= Math.floor(Date.now() / 1000) + 60 ? value : null; };
const latestFirst = (a, b) => b.messageTimestamp - a.messageTimestamp || b.id.localeCompare(a.id);
const copy = value => JSON.parse(JSON.stringify(value));
async function bounded(promise, ms = WAIT) {
  let timer;
  try { return await Promise.race([promise, new Promise((_, reject) => { timer = setTimeout(() => reject(Error('WhatsApp synchronization timed out.')), ms); })]); }
  finally { clearTimeout(timer); }
}

/** Content-free protocol metadata and idempotent read receipts; archive patches are never replayed. */
export class ChatSync {
  constructor(engine) {
    this.engine = engine; this.store = engine.store; this.inbox = engine.inbox;
    this.generation = 0; this.readyEpoch = null; this.readJob = null; this.readAgain = false; this.refreshes = new Map();
    for (const { value } of this.store.list(ARCHIVE, { limit: GLOBAL })) {
      if (value.state === 'sending') this.saveArchive({ ...value, state: 'uncertain' });
    }
    this.limitRanges();
    this.clearTailProof();
    for (const { id, value } of this.store.list(READ, { limit: GLOBAL })) {
      const target = this.readTarget(value);
      if (!target) this.forget(value);
      else if (!value.expiresAt || value.expiresAt > target.expiresAt) this.store.set(READ, id, { ...value, expiresAt: target.expiresAt }, { tag: value.chat, created: value.createdAt, expires: target.expiresAt });
    }
  }
  context() { return { epoch: this.engine.epoch, generation: this.generation, socket: this.engine.socket }; }
  valid(context) { const e = this.engine; return !e.closed && !e.stopping && !e.closing && context.epoch === e.epoch && context.generation === this.generation && context.socket === e.socket; }
  online() { const e = this.engine; return !e.closed && !e.stopping && !e.closing && e.state === 'connected' && !!e.socket; }
  ready() {
    if (!this.online() || this.engine.appStateReady === false) return false;
    return this.engine.appStateReady === true || this.readyEpoch === this.engine.epoch;
  }
  setReady(value, epoch = this.engine.epoch) {
    if (epoch !== this.engine.epoch) return;
    this.readyEpoch = value ? epoch : null;
    if (value) this.resume();
  }
  resume() { if (this.online()) void this.retryReads(); }
  pause() {
    this.readyEpoch = null; this.generation++; this.readJob = null; this.readAgain = false;
    for (const request of this.refreshes.values()) request.finish(false);
    this.refreshes.clear();
    this.clearTailProof();
  }
  clearTailProof() {
    if (this.store.closed) return;
    this.store.transaction(() => {
      for (const { id, value } of this.store.list(STATE, { limit: GLOBAL })) this.store.set(STATE, id, { ...value, tailKnown: false, remoteTimestamp: 0 });
      this.store.clear(CUTOFF);
    });
  }
  reset() { this.pause(); this.store.transaction(() => { for (const ns of CHAT_SYNC_NAMESPACES) this.store.clear(ns); }); }
  forget(key) {
    if (this.store.closed || !key?.chat) return;
    this.store.remove(READ, identity(key)); this.store.remove(CONFIRMED, identity(key)); this.store.remove(CUTOFF, key.chat);
  }
  readTarget(value) {
    if (value.expiresAt && value.expiresAt <= Date.now() || this.store.get('tombstone', messageId(value))) return null;
    const row = this.store.get('message', messageId(value));
    return row && !row.fromMe && row.expiresAt > Date.now() ? row : null;
  }
  state(chat) { return this.store.get(STATE, chat) || { chat, incomplete: false, tailKnown: false, version: 0, liveVersion: 0 }; }
  saveState(state) { this.store.set(STATE, state.chat, state); }
  touch(chat, values = {}) { const next = { ...this.state(chat), ...values }; this.saveState(next); return next; }
  notify(chat) { this.engine.metadata?.(chat); }
  chatFields(chat, fields) { this.inbox.contact(chat, fields); this.notify(chat); }
  invalidate(chat, timestamp = Math.floor(Date.now() / 1000)) {
    try { chat = assertChat(chat); } catch { return; }
    const old = this.state(chat); this.touch(chat, { incomplete: true, gapAt: Math.max(old.gapAt || 0, timestamp, this.rows(chat)[0]?.messageTimestamp || 0), version: old.version + 1 });
    this.store.remove(CUTOFF, chat);
  }
  /** Called in the event callback, before content sanitization or retention filters. */
  snapshot(messages) {
    if (!Array.isArray(messages)) return [];
    return messages.map(message => {
      if (message?.invalidChat) { try { return { invalidChat: assertChat(message.invalidChat) }; } catch { return null; } }
      const key = message?.key || {}, chat = normalizePeer(key.remoteJid);
      let validChat; try { validChat = assertChat(chat); } catch { return null; }
      const timestamp = stamp(message.messageTimestamp);
      const participant = normalizePeer(key.participant), alternate = normalizePeer(key.participantAlt);
      if (!idPattern.test(key.id || '') || typeof key.fromMe !== 'boolean' || !timestamp || isGroup(validChat) && !key.fromMe && !peerPattern.test(participant)) return { invalidChat: validChat };
      return { key: { remoteJid: validChat, id: key.id, fromMe: key.fromMe,
        ...(key.remoteJidAlt ? { remoteJidAlt: normalizePeer(key.remoteJidAlt) } : {}),
        ...(peerPattern.test(participant) ? { participant } : {}), ...(peerPattern.test(alternate) ? { participantAlt: alternate } : {}) },
        messageTimestamp: timestamp, read: message.read === true || message.status === 4 || message.status === 5 };
    }).filter(Boolean);
  }
  async capture(messages, { history = false, requestId, progress } = {}) {
    // Replace the input immediately: nothing below retains message bodies/media/secrets.
    messages = this.snapshot(messages);
    const context = this.context(), captured = new Map(), liveAnchors = new Map();
    for (const message of messages) {
      if (!this.valid(context)) return;
      if (message.invalidChat) {
        const chat = await this.engine.canonical(message.invalidChat);
        if (!this.valid(context)) return;
        this.invalidate(chat || message.invalidChat); continue;
      }
      const clean = await this.engine.cleanKey(message.key);
      if (!this.valid(context)) return;
      if (!clean) { this.invalidate(message.key.remoteJid); continue; }
      const chat = clean.chat, participant = clean.participant || normalizePeer(message.key.participant);
      if (isGroup(chat) && !clean.fromMe && !peerPattern.test(participant || '')) { this.invalidate(chat); continue; }
      const protocolKey = { remoteJid: message.key.remoteJid, id: clean.id, fromMe: clean.fromMe,
        ...(message.key.participant ? { participant: message.key.participant } : {}) };
      const row = { chat, id: clean.id, fromMe: clean.fromMe, ...(isGroup(chat) && participant ? { participant } : {}), protocolKey, messageTimestamp: message.messageTimestamp };
      const id = identity(row), old = this.store.get(RANGE, id);
      row.knownUnread = old ? old.knownUnread && !message.read : !row.fromMe && !history && !message.read;
      row.observedAt = old?.observedAt || Date.now();
      this.store.set(RANGE, id, row, { tag: chat, created: row.messageTimestamp });
      if (!old) {
        const state = this.state(chat);
        this.touch(chat, { version: state.version + 1, ...(!history ? { tailKnown: true, liveVersion: (state.liveVersion || 0) + 1 } : {}) });
        if (!history && !row.fromMe && (!liveAnchors.has(chat) || liveAnchors.get(chat) < row.messageTimestamp)) liveAnchors.set(chat, row.messageTimestamp);
      }
      if (!captured.has(chat)) captured.set(chat, []);
      captured.get(chat).push(row);
    }
    if (!this.valid(context)) return;
    for (const chat of captured.keys()) this.compact(chat, liveAnchors.get(chat));
    this.limitRanges();
    const request = requestId && this.refreshes.get(requestId);
    if (request && this.valid(request.context)) {
      const rows = captured.get(request.chat) || [];
      // A full page cannot prove that a truncated/incomplete range has been repaired.
      const state = this.state(request.chat);
      const valid = progress === 100 && rows.length > 0 && rows.length < PER_CHAT && state.tailKnown
        && state.liveVersion === request.liveVersion && !messages.some(row => row.invalidChat === request.chat)
        && rows.some(row => !row.fromMe) && this.rows(request.chat).length <= PER_CHAT;
      if (valid) this.touch(request.chat, { incomplete: false });
      request.finish(valid);
      this.refreshes.delete(requestId);
    }
  }
  compact(chat, liveAnchor) {
    const rows = this.store.list(RANGE, { tag: chat, limit: Math.max(PER_CHAT + 1, this.store.totals(RANGE).count) }).map(row => row.value).sort(latestFirst);
    const incoming = rows.find(row => !row.fromMe);
    if (!incoming) return;
    let unknownUnread = this.state(chat).unknownUnreadBeforeAnchor || false;
    for (const row of rows) if (row.messageTimestamp < incoming.messageTimestamp) {
      if (row.knownUnread && !this.store.get('message', messageId(row))) unknownUnread = true;
      this.store.remove(RANGE, identity(row));
    }
    const state = this.state(chat), latest = rows[0].messageTimestamp;
    const proof = latest >= (state.remoteTimestamp || 0) && (state.tailKnown || state.remoteTimestamp > 0);
    this.touch(chat, { tailKnown: !!proof, unknownUnreadBeforeAnchor: unknownUnread,
      ...(liveAnchor && liveAnchor >= latest && liveAnchor > (state.gapAt || 0) ? { incomplete: false } : {}) });
  }
  limitRanges() {
    const byChat = new Map();
    const rows = this.store.list(RANGE, { limit: Math.max(GLOBAL + 1, this.store.totals(RANGE).count) }).map(row => row.value).sort(latestFirst);
    let kept = 0;
    for (const row of rows) {
      const count = byChat.get(row.chat) || 0;
      if (count >= PER_CHAT || kept >= GLOBAL) {
        this.store.remove(RANGE, identity(row)); this.invalidate(row.chat);
      } else { byChat.set(row.chat, count + 1); kept++; }
    }
  }
  rows(chat) { return this.store.list(RANGE, { tag: chat, limit: PER_CHAT + 1 }).map(row => row.value).sort(latestFirst); }
  range(chat) {
    if (this.state(chat).incomplete || !this.state(chat).tailKnown) return null;
    const rows = this.rows(chat);
    if (!rows.length || rows.length > PER_CHAT || !rows.some(row => !row.fromMe)) return null;
    // Explicit range avoids Baileys' array helper using the final (possibly oldest) timestamp.
    return { lastMessageTimestamp: rows[0].messageTimestamp, messages: rows.map(row => ({ key: copy(row.protocolKey), messageTimestamp: row.messageTimestamp })) };
  }
  async refreshRange(chat, context) {
    const pending = [...this.refreshes.values()].find(row => row.chat === chat);
    if (pending) return pending.promise;
    const rows = this.rows(chat), newest = rows[0], socket = context.socket;
    if (!newest || typeof socket?.fetchMessageHistory !== 'function') return false;
    const version = this.state(chat).version, liveVersion = this.state(chat).liveVersion;
    let requestId;
    try { requestId = await bounded(socket.fetchMessageHistory(PER_CHAT, copy(newest.protocolKey), newest.messageTimestamp)); }
    catch { return false; }
    if (!this.valid(context) || typeof requestId !== 'string' || !requestId) return false;
    let finish;
    const promise = new Promise(resolve => { finish = resolve; });
    this.refreshes.set(requestId, { chat, context, version, liveVersion, promise, finish });
    try { return await bounded(promise); }
    catch { return false; }
    finally { this.refreshes.delete(requestId); }
  }
  chatUpdate(raw, chat) {
    if (!chat) return;
    const remoteTimestamp = stamp(raw.conversationTimestamp);
    if (remoteTimestamp) {
      const latestRemote = Math.max(this.state(chat).remoteTimestamp || 0, remoteTimestamp);
      this.touch(chat, { remoteTimestamp: latestRemote });
      const newest = this.rows(chat)[0];
      if (newest?.messageTimestamp >= latestRemote) this.touch(chat, { tailKnown: true });
      else this.touch(chat, { tailKnown: false });
    }
    if (typeof raw.archived === 'boolean') {
      this.touch(chat, { archiveVersion: (this.state(chat).archiveVersion || 0) + 1 });
      for (const { value: operation } of this.store.list(ARCHIVE, { tag: chat, limit: GLOBAL })) {
        if (operation.state === 'sending' || operation.state === 'uncertain') this.saveArchive({ ...operation, state: operation.archived === raw.archived ? 'sent' : 'uncertain', reconciled: true });
      }
      this.chatFields(chat, { archived: raw.archived, archiveSyncState: 'sent' });
    }
    if (raw.unreadCount === 0) {
      this.inbox.read(chat);
      this.touch(chat, { unknownUnreadBeforeAnchor: false });
      for (const row of this.rows(chat)) if (row.knownUnread) this.store.set(RANGE, identity(row), { ...row, knownUnread: false }, { tag: chat, created: row.messageTimestamp });
      this.store.remove(CUTOFF, chat);
    }
  }
  archiveView(row) { return { chat: row.chat, archived: row.archived, operationId: row.operationId, state: row.state === 'sent' ? 'sent' : 'uncertain' }; }
  saveArchive(row) { this.store.set(ARCHIVE, row.operationId, row, { tag: row.chat, created: row.createdAt }); }
  async setArchived(args) {
    exact(args, ['chat', 'archived', 'operationId']);
    if (typeof args.archived !== 'boolean' || !uuidPattern.test(args.operationId || '')) throw Error('Invalid archive request.');
    const context = this.context(), chat = await this.engine.canonical(assertChat(args.chat));
    if (!this.valid(context)) throw Error('WhatsApp connection changed.');
    const old = this.store.get(ARCHIVE, args.operationId);
    if (old) {
      if (old.chat !== chat || old.archived !== args.archived) throw Error('This operation already refers to another archive action.');
      return this.archiveView(old);
    }
    if (!this.ready() || typeof context.socket.chatModify !== 'function') throw Error('Wait for WhatsApp synchronization before changing archived chats.');
    if (this.store.totals(ARCHIVE).count >= GLOBAL) throw Error('Archive synchronization storage is full.');
    if (this.store.list(ARCHIVE, { tag: chat, limit: GLOBAL }).some(({ value }) => value.state !== 'sent' && !value.reconciled)) throw Error('The earlier archive change is unconfirmed. Check this chat in WhatsApp.');
    let lastMessages = this.range(chat);
    if (!lastMessages && await this.refreshRange(chat, context)) lastMessages = this.range(chat);
    // A transport-ready signal can precede the helper's queued offline-message work.
    // Drain that queue before taking the final immutable protocol range.
    if (this.engine.flushEvents) await this.engine.flushEvents(context.epoch);
    if (!this.valid(context) || !this.ready()) throw Error('WhatsApp connection changed.');
    lastMessages = this.range(chat);
    if (!lastMessages) throw Error('Recent WhatsApp message metadata is unavailable. Open this chat in WhatsApp and try again.');
    const concurrent = this.store.get(ARCHIVE, args.operationId);
    if (concurrent) {
      if (concurrent.chat !== chat || concurrent.archived !== args.archived) throw Error('This operation already refers to another archive action.');
      return this.archiveView(concurrent);
    }
    if (this.store.list(ARCHIVE, { tag: chat, limit: GLOBAL }).some(({ value }) => value.state !== 'sent' && !value.reconciled)) throw Error('The earlier archive change is unconfirmed. Check this chat in WhatsApp.');
    lastMessages = this.range(chat);
    if (!lastMessages) throw Error('Recent WhatsApp message metadata changed. Try again after synchronization.');
    // Reserve immediately before dispatch. Any crash or transport ambiguity means no replay.
    const operation = { chat, archived: args.archived, operationId: args.operationId, state: 'sending', createdAt: Date.now(), archiveVersion: this.state(chat).archiveVersion || 0 };
    this.saveArchive(operation); this.chatFields(chat, { archiveSyncState: 'uncertain', archiveOperationId: operation.operationId });
    const job = (async () => {
      try {
        await bounded(context.socket.chatModify({ archive: operation.archived, lastMessages }, chat));
        if (!this.valid(context)) return this.archiveView(operation);
        const latest = this.store.get(ARCHIVE, operation.operationId);
        if (!latest) return this.archiveView(operation);
        this.saveArchive({ ...latest, state: 'sent' });
        if ((this.state(chat).archiveVersion || 0) === operation.archiveVersion) this.chatFields(chat, { archived: operation.archived, archiveSyncState: 'sent' });
        return this.archiveView({ ...latest, state: 'sent' });
      } catch {
        if (!this.valid(context)) return this.archiveView(operation);
        const latest = this.store.get(ARCHIVE, operation.operationId);
        if (latest) { const next = { ...latest, state: latest.state === 'sent' ? 'sent' : 'uncertain' }; this.saveArchive(next); return this.archiveView(next); }
        return this.archiveView(operation);
      }
    })();
    this.engine.inflight?.add(job);
    try { return await job; } finally { this.engine.inflight?.delete(job); }
  }
  knownUnread(chat) {
    return this.state(chat).unknownUnreadBeforeAnchor || this.rows(chat).some(row => row.knownUnread) || this.store.list('message', { tag: chat, limit: GLOBAL }).some(({ value }) => !value.fromMe && !value.readAt);
  }
  async markRead(args) {
    exact(args, ['chat'], ['ids', 'messages', 'onlyIfUnarchived']);
    if (args.ids !== undefined && args.messages !== undefined || args.onlyIfUnarchived !== undefined && typeof args.onlyIfUnarchived !== 'boolean') throw Error('Invalid message selection.');
    const context = this.context(), chat = await this.engine.canonical(assertChat(args.chat));
    if (!this.valid(context)) throw Error('WhatsApp connection changed.');
    const explicit = args.ids !== undefined || args.messages !== undefined;
    if ((args.onlyIfUnarchived || !explicit) && this.store.get('chat', chat)?.archived) return { ok: true, skipped: true };
    if (isGroup(chat) && args.ids !== undefined) throw Error('Group reads require participant-aware message keys.');
    const selection = args.messages ?? args.ids;
    if (selection !== undefined && (!Array.isArray(selection) || selection.length > 100)) throw Error('Invalid message selection.');
    if (args.ids?.some(id => typeof id !== 'string' || !idPattern.test(id))) throw Error('Invalid message selection.');
    if (args.messages) for (const key of args.messages) {
      exact(key, ['id', 'fromMe'], ['participant']);
      if (!idPattern.test(key.id || '') || typeof key.fromMe !== 'boolean' || isGroup(chat) && !key.fromMe && !peerPattern.test(key.participant || '') || !isGroup(chat) && key.participant !== undefined) throw Error('Invalid message selection.');
    }
    const all = this.store.list('message', { tag: chat, limit: GLOBAL }).map(row => row.value);
    const selected = all.filter(row => !row.fromMe && (args.ids ? args.ids.includes(row.id) : args.messages ? args.messages.some(key => !key.fromMe && key.id === row.id && (!isGroup(chat) || key.participant === row.participant)) : !row.readAt)).slice(0, 100);
    const rangeRows = this.rows(chat), now = Date.now();
    const intents = selected.map(row => {
      const metadata = rangeRows.find(value => identity(value) === identity(row));
      const protocolKey = metadata?.protocolKey || row.protocolKey || (!isGroup(chat) ? { remoteJid: chat, id: row.id, fromMe: false } : null);
      if (!protocolKey || isGroup(chat) && !peerPattern.test(normalizePeer(protocolKey.participant))) throw Error('This group message is missing its WhatsApp participant identity.');
      return { chat, id: row.id, fromMe: false, ...(row.participant ? { participant: row.participant } : {}), protocolKey: copy(protocolKey), createdAt: now, expiresAt: row.expiresAt, onlyIfUnarchived: !!args.onlyIfUnarchived };
    });
    const needsReceipt = row => row.delivery !== 'read' && !this.store.get(CONFIRMED, identity(row)) && !this.store.get(READ, identity(row));
    if (this.store.totals(READ).count + selected.filter(needsReceipt).length > GLOBAL) throw Error('Read synchronization storage is full.');
    let changed = false, receiptAdded = false;
    this.store.transaction(() => {
      for (let i = 0; i < selected.length; i++) {
        const row = selected[i], intent = intents[i], id = identity(intent);
        if (needsReceipt(row)) {
          // readAt may have come from history import or an older local-only client.
          // Only an acknowledged wire receipt (or incoming READ status) suppresses this intent.
          this.store.set(READ, id, intent, { tag: chat, created: now, expires: intent.expiresAt }); receiptAdded = true;
        }
        if (!row.readAt) {
          changed = true;
          this.inbox.save({ ...row, readAt: now }, true);
        }
        const metadata = this.store.get(RANGE, id);
        if (metadata?.knownUnread) this.store.set(RANGE, id, { ...metadata, knownUnread: false }, { tag: chat, created: metadata.messageTimestamp });
      }
      const lastMessages = this.range(chat);
      if ((changed || receiptAdded) && lastMessages && !this.knownUnread(chat)) this.store.set(CUTOFF, chat, { chat, lastMessages, version: this.state(chat).version, createdAt: now, onlyIfUnarchived: !!args.onlyIfUnarchived }, { tag: chat, created: now });
    });
    if (changed) this.notify(chat);
    // Local read succeeds offline; receipts have durable, content-free, idempotent retries.
    if (this.online()) await this.retryReads();
    if (!this.valid(context)) return { ok: true, state: 'pending' };
    return { ok: true, state: this.store.list(READ, { tag: chat, limit: 1 }).length || this.store.get(CUTOFF, chat) ? 'pending' : 'sent' };
  }
  retryReads() {
    if (this.readJob) { this.readAgain = true; return this.readJob; }
    if (!this.online()) return Promise.resolve();
    const context = this.context();
    const job = this.flushReads(context).catch(() => {}).finally(() => {
      this.engine.inflight?.delete(job);
      if (this.readJob !== job) return;
      this.readJob = null;
      const again = this.readAgain; this.readAgain = false;
      if (again && this.online()) void this.retryReads();
    });
    this.readJob = job; this.engine.inflight?.add(job); return job;
  }
  async flushReads(context) {
    for (const ns of [READ, CONFIRMED]) for (const { id, value } of this.store.list(ns, { expired: true, limit: GLOBAL })) {
      this.store.remove(ns, id); this.store.remove(CUTOFF, value.chat);
    }
    if (typeof context.socket.readMessages === 'function') {
      const pending = this.store.list(READ, { limit: GLOBAL }).filter(({ value }) => !value.onlyIfUnarchived || !this.store.get('chat', value.chat)?.archived);
      for (let offset = 0; offset < pending.length; offset += 100) {
        if (!this.valid(context)) return;
        const batch = pending.slice(offset, offset + 100).filter(({ id, value }) => {
          if (!this.store.get(READ, id) || !this.readTarget(value)) { this.forget(value); return false; }
          return true;
        });
        if (!batch.length) continue;
        try { await bounded(context.socket.readMessages(batch.map(({ value }) => copy(value.protocolKey)))); }
        catch { return; }
        if (!this.valid(context)) return;
        this.store.transaction(() => {
          const now = Date.now();
          for (const { id, value } of batch) if (this.store.get(READ, id)?.createdAt === value.createdAt) {
            const target = this.readTarget(value);
            if (!target) { this.forget(value); continue; }
            const { chat, id: message, fromMe, participant } = value;
            const expiresAt = Math.min(target.expiresAt, value.expiresAt || Infinity, now + WEEK);
            this.store.set(CONFIRMED, id, { chat, id: message, fromMe, ...(participant ? { participant } : {}), confirmedAt: now, expiresAt }, { tag: chat, created: now, expires: expiresAt });
            this.store.remove(READ, id);
          }
          const excess = this.store.totals(CONFIRMED).count - GLOBAL;
          if (excess > 0) for (const { id } of this.store.list(CONFIRMED, { oldest: true, limit: excess })) this.store.remove(CONFIRMED, id);
        });
      }
    }
    if (!this.ready() || typeof context.socket.chatModify !== 'function') return;
    for (const { value: cutoff } of this.store.list(CUTOFF, { limit: GLOBAL })) {
      if (this.engine.flushEvents) {
        try { await this.engine.flushEvents(context.epoch); } catch { return; }
      }
      if (!this.valid(context) || !this.ready()) return;
      const chat = cutoff.chat;
      const currentCutoff = this.store.get(CUTOFF, chat);
      if (!currentCutoff || currentCutoff.createdAt !== cutoff.createdAt || currentCutoff.version !== cutoff.version) continue;
      if (cutoff.onlyIfUnarchived && this.store.get('chat', chat)?.archived || this.store.list(READ, { tag: chat, limit: 1 }).length) continue;
      if (this.state(chat).incomplete || this.knownUnread(chat) || this.state(chat).version !== cutoff.version) { this.store.remove(CUTOFF, chat); continue; }
      try { await bounded(context.socket.chatModify({ markRead: true, lastMessages: copy(cutoff.lastMessages) }, chat)); }
      catch { return; }
      if (!this.valid(context)) return;
      const latest = this.store.get(CUTOFF, chat);
      if (latest?.createdAt === cutoff.createdAt && latest.version === cutoff.version) this.store.remove(CUTOFF, chat);
    }
  }
  rebind(old, chat) {
    if (old === chat) return;
    this.store.transaction(() => {
      for (const ns of [RANGE, READ, CONFIRMED]) for (const { id, value } of this.store.list(ns, { tag: old, limit: GLOBAL })) {
        this.store.remove(ns, id); const moved = { ...value, chat }, nextId = identity(moved), existing = this.store.get(ns, nextId);
        this.store.set(ns, nextId, existing ? { ...moved, ...existing, ...(ns === RANGE ? { knownUnread: moved.knownUnread && existing.knownUnread } : {}) } : moved, { tag: chat, created: value.messageTimestamp || value.createdAt || value.confirmedAt, ...(value.expiresAt ? { expires: value.expiresAt } : {}) });
      }
      const previous = this.store.get(STATE, old), current = this.state(chat);
      if (previous) this.saveState({ ...previous, ...current, chat, incomplete: previous.incomplete || current.incomplete, version: Math.max(previous.version, current.version) + 1 });
      this.store.remove(STATE, old); this.store.remove(CUTOFF, old); this.store.remove(CUTOFF, chat);
      for (const { value } of this.store.list(ARCHIVE, { tag: old, limit: GLOBAL })) this.saveArchive({ ...value, chat, state: value.state === 'sending' ? 'uncertain' : value.state });
    });
    this.limitRanges();
  }
  rebindParticipant(old, participant) {
    if (old === participant || !peerPattern.test(old) || !peerPattern.test(participant)) return;
    this.store.transaction(() => {
      for (const ns of [RANGE, READ, CONFIRMED]) for (const { id, value } of this.store.list(ns, { limit: GLOBAL })) {
        if (!isGroup(value.chat) || value.participant !== old) continue;
        this.store.remove(ns, id);
        const moved = { ...value, participant }, target = identity(moved), existing = this.store.get(ns, target);
        this.store.set(ns, target, existing ? { ...moved, ...existing, ...(ns === RANGE ? { knownUnread: moved.knownUnread && existing.knownUnread } : {}) } : moved, { tag: moved.chat, created: moved.messageTimestamp || moved.createdAt || moved.confirmedAt, ...(moved.expiresAt ? { expires: moved.expiresAt } : {}) });
        this.store.remove(CUTOFF, moved.chat);
      }
    });
  }
}
