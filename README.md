# CIFER WhatsApp Core

MIT-licensed, single-account WhatsApp engine intended for an isolated native helper.
It connects directly to WhatsApp using pinned `baileys@7.0.0-rc14`. It has no HTTP
listener, no CIFER service credential, no shared Worker dependency, and no provider
or message-decryption API. This repository is local source; it has not been published
to a package registry.

## Runtime and installation

Use Node 22.20 or newer with `node:sqlite` available. Install the reviewed lockfile:

```sh
npm ci --ignore-scripts
npm test
npm run check
npm audit --omit=dev
```

The native host must silence dependency console output **before importing the
engine**. Baileys uses a silent logger here, but some libsignal versions write
session information through global console methods. Run the engine in an isolated
helper and suppress `log`, `info`, `warn`, `error`, `debug`, `trace`, `dir` and `table`
there. Do not serialize raw exceptions or transport objects to diagnostics. The
engine does not globally replace a library consumer's console.

## Native integration

```js
const { createEngine } = await import('@cifer/whatsapp-core');
const engine = await createEngine({
  directory: trustedAccountDirectory,
  key: keyFromKeychain, // 32-byte Uint8Array; never a renderer input
  account: verifiedCiferPrincipal,
  connection: stableLocalLinkId,
  onEvent: event => sendAccountFencedEvent(event),
});
await engine.request('pair', {});
await engine.request('messages', { chat: '15550000001@s.whatsapp.net', limit: 20 });
await engine.close(); // suspend/unlock cycle: preserves linked credentials
```

The host creates a new random storage key, wraps it with the platform Keychain,
and supplies it over private IPC. The engine copies the key and clears its copy
when closed. It never accepts filesystem paths, storage keys or account binding
changes through `request`. A database cannot be opened for another account or
connection. Every host request/response/event must additionally be fenced by the
native authenticated account and lifecycle epoch. A renderer cannot assert its
own identity. Close the helper before activating another account.

`createEngine` resolves to `{ request(method, args), close(), disconnect() }`.
On restart it reconnects only if that local store was previously explicitly paired.
`close()` stops the socket and closes durable state; it does not unlink WhatsApp.
`disconnect()` attempts WhatsApp logout, erases credentials and message state even
if the network is unavailable, and returns `{disconnected:true, remoteUnlinked}`.
When `remoteUnlinked` is false, the user can revoke the linked device in WhatsApp.

| Method | Arguments | Result |
| --- | --- | --- |
| `status` | `{}` | State, identity, QR/code, expiry, connection, revision and content-free unread counts |
| `pair` | `{phone?}` | Status; phone includes country code, digits only; request again after the QR-ready state if code linking is desired |
| `chats` | `{before?,limit?}` | `{chats,cursor}`; default 10, max 100 |
| `messages` | `{chat,before?,limit?}` | `{messages,cursor}`; default 20, max 100 |
| `history` | `{chat,before?}` | `{state:'loading'|'ready'|'unavailable',hasMore?}`; phone fetches at most 20 per request |
| `prepareSend` | `{operationId,chat,text,kind,prepared?}` | Durable prepared operation; does not publish or send |
| `send` | Same immutable arguments | Operation state and stable WhatsApp message ID |
| `operation` | `{id}` | Recoverable operation or null |
| `operations` | `{limit?}` | `{operations}`; default 50, max 100 |
| `markRead` / `read` | `{chat,ids?}` | `{ok:true}`; local unread only, no WhatsApp receipt |
| `profiles` | `{peers}` | `{ok:true}`; resolves known identities, does not download avatars |
| `binding` | `{chat}` | Local public CIFER-contact binding or null |
| `bindContact` | `{chat,binding:null|object}` | `{ok:true}` |
| `disconnect` | `{}` | `{disconnected:true,remoteUnlinked:boolean}` |

Events are `{type:'status',status}` or `{type:'changed',revision,chat?}`. The host
adds its own generation. Missing QR/code values are null; missing pairing expiry
is zero. Chat `lastMessage` is a string and protected content uses a placeholder.
Messages contain `id`, `chat`, `kind`, `text` or `wire`, `fromMe`, `createdAt`,
`expiresAt`, `delivery` and optional `operationId`. Page cursors are opaque strings
or null. Unknown methods and extra request fields fail closed.

## Storage and delivery rules

- SQLite WAL with full synchronous commits stores AES-256-GCM encrypted records.
  Record/peer IDs are keyed hashes. Namespace, timestamps, sizes and unread metadata
  remain visible on disk. Files/directories are restricted to the user.
- Signal key batches and credentials commit transactionally. Closed/disconnected
  callbacks cannot recreate credential rows.
- Message history is limited to seven days and 10,000 records. A shared 64 MiB
  serialized-content budget covers messages, recoverable operations and retry
  records. New preparation fails if unsent operations alone exhaust that budget;
  otherwise the oldest inbox records are evicted first. Completed sends erase their
  duplicate outbox body. Disappearing expiry shortens retention. Credentials are not treated as
  evictable history. Minimal operation hashes persist until unlink to prevent a
  stale operation ID from sending again after its content expires.
- Edits remove original recoverable outbox content. Delete events create durable
  tombstones; replayed history cannot recreate deleted messages. PN/LID merging
  preserves deletions, shorter expiry and existing read state. A prepared send bound
  to an old alias is cancelled, not silently retargeted.
- Sending journals intent before WhatsApp I/O. Repeated requests return the recorded
  state. A crash/network ambiguity becomes `uncertain` and is never blindly resent.
  Receipt and outgoing-echo evidence can advance it to sent/delivered/read.
- Known chat disappearance settings are passed to outgoing messages. Available
  WhatsApp history/settings depend on what the phone synchronizes.

The native UI should mark only visible message IDs read. An omitted IDs list is an
explicit local mark-chat-read operation. Expiry is enforced at read time before the
minute cleanup; the UI must also remove displayed plaintext at `expiresAt`.

## Protected CIFER messages

The engine transports canonical `CIFER:WA:1`/`CIFER:WA:2` ciphertext. It does not
encrypt/decrypt protected bodies, contact the publication service, or hold a CIFER
private signing key. V2 preparation can persist a strictly validated object with
public sender/contact keys, ciphertext, recovery descriptor and recipient grant.
No plaintext, content key or private key field is accepted there.

The host prepares protection, persists `prepareSend`, completes any separate
publication action, then calls `send` with the identical operation. After a crash,
the host can recover preparation with `operation`/`operations`; nothing is
automatically published or resent. Keeping CIFER protected-message publication
and key opening is a separate trust boundary from ordinary local WhatsApp traffic.

## Supported scope and limits

V1 supports individual chats, ordinary text, protected wire transport, captions,
contact names, phone/LID aliases, limited history, edits/deletes, disappearing
expiry and delivery receipts. Media messages produce local placeholders. Groups,
calls, reactions, status, attachment upload/download and profile-photo downloads
are not implemented. No full WhatsApp-client parity is claimed.

Baileys is an unofficial WhatsApp implementation. Automated fixtures validate the
engine and real dependency event/serialization shapes, but do not replace a paired
phone canary. Before release verify initial pairing, phone history availability,
restart/offline delivery, edits/revokes and disappearance settings on real test
accounts. Also verify native packaging, Keychain failure behavior, account-switch
fences, network destinations and worst-case memory use.

Local encryption protects files at rest. The running Mac/helper and its software
updates remain trusted; plaintext ordinary WhatsApp messages exist in memory while
being processed. There is no promise of secure deletion from OS backups or of
recalling content another device already downloaded.
