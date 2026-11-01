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
npm run prepare:baileys
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
| `pair` | `{phone?}` | Status; phone includes country code, digits only. First phone request waits up to 10 seconds for readiness, then up to 10 seconds for a code. Refreshing expired pairing restarts the unregistered socket. |
| `chats` | `{view?:"active"|"archived",before?,limit?}` | `{chats,cursor}`; default 10, max 100 |
| `chat` | `{chat}` | Single conversation summary, independent of active/archived pagination |
| `setArchived` | `{chat,archived,operationId}` | `{chat,archived,operationId,state:"sent"|"uncertain"}`; requires connected app-state readiness; uncertain operations are never replayed |
| `messages` | `{chat,before?,limit?}` | `{messages,cursor}`; default 20, max 100 |
| `history` | `{chat,before?,recent?}` | `{state:'loading'|'ready'|'unavailable',hasMore?}`; earlier pages fetch 20; `recent:true` fetches at most 50 using the latest visible message, requests that exact message separately, and may recover one newer metadata-only message. Cannot combine `recent` with `before`. Requests coalesce and reuse results for one minute. |
| `prepareSend` | `{operationId,chat,text,kind,prepared?}` | Durable prepared operation; does not publish or send |
| `send` | Same immutable arguments | Operation state and stable WhatsApp message ID |
| `operation` | `{id}` | Recoverable operation or null |
| `operations` | `{limit?}` | `{operations}`; default 50, max 100 |
| `markRead` / `read` | `{chat,messages?:[{id,fromMe,participant?}],onlyIfUnarchived?}` | `{ok:true}`; visible-message receipts and durable, bounded read sync; legacy `ids` accepted only for direct chats |
| `profiles` | `{peers}` | `{ok:true}`; schedules previews for at most 20 known chats while connected; work is bounded, deduplicated and asynchronous |
| `binding` | `{chat}` | Local public CIFER-contact binding or null |
| `bindContact` | `{chat,binding:null|object}` | `{ok:true}` |
| `disconnect` | `{}` | `{disconnected:true,remoteUnlinked:boolean}` |

Events are `{type:'status',status}`, `{type:'changed',revision,chat?}` or
`{type:'metadata',chat?}`. Reaction, photo/contact and local read-marker events refresh
metadata without invalidating message content or sender-confirmation controls. The host
adds its own generation. Missing QR/code values are null; missing pairing expiry
is zero. Chat `lastMessage` is a string and protected content uses a placeholder.
Messages contain `id`, `chat`, `kind`, `text` or `wire`, `fromMe`, `createdAt`,
`expiresAt`, `delivery` and optional `operationId`. Page cursors are opaque strings
or null. Chat `avatar` is null or a validated raster data URI, never a remote URL
or filesystem path. Unknown methods and extra request fields fail closed.

Profile previews use the linked WhatsApp socket to obtain a URL, then the helper
downloads it without cookies, referrer or redirects. Only HTTPS subdomains of
`whatsapp.net` and `whatsapp.com` are accepted. JPEG, PNG and WebP are checked for
their actual container and dimensions (maximum 512×512, 32 KiB; no SVG/animation).
The encrypted account/connection database caches at most 100 entries and 4 MiB,
separately from message history. Photos expire after one hour; absent/private
photos after ten minutes; transient failures after one minute. Expired cache is
inaccessible immediately and pruned within one minute; the UI replaces its memory
copy on refresh or clears it on lock/account change. Removed photos invalidate the
cache immediately. Verified PN/LID mappings can move cache entries; no phone number
is inferred from a LID. Two jobs run concurrently, with at most 40 queued/active.
Pause/close aborts work and fences late completions; unlink erases the cache.

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

The current engine supports individual chats and existing groups, ordinary text,
direct-chat protected wire transport, contact names, phone/LID aliases, bounded
history, edits/deletes, disappearing expiry and delivery receipts. Incoming photos
load locally in direct chats and groups; other media retain placeholders. Archive/unarchive and viewed-message read state
synchronize with WhatsApp. Group creation/administration, protected group sends,
calls, reactions, status, attachment uploads and non-photo downloads remain outside this scope.
No full WhatsApp-client parity is claimed.
Ordinary outgoing text is limited to 2,000 UTF-8 bytes; incoming text/captions and
protected wire records have their own bounded parsing limits.

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

### Message edits and deletion

The engine consumes edit/revoke protocol envelopes as well as Baileys buffered
upsert and history events. Replayed originals cannot replace a newer version or
resurrect a deletion. Edits retain the original creation time and shortest expiry.
An edit received before its original is held encrypted for at most two minutes,
using conservative disappearance/retention bounds; expiry then records a content-free
tombstone instead of allowing an older body to return.

`messages` returns `editedAt`, a content `version`, and `editUntil`/`deleteUntil`.
Only delivered-to-server own text is editable within 15 minutes. Delete for everyone
is offered conservatively for two days; WhatsApp still determines remote acceptance.
`delete-local` removes only this engine's copy, including incoming messages.

Call `prepareMutation` before publishing a replacement protected wire, then `mutate`
with the identical `{operationId, chat, messageId, fromMe, participant?, action, expectedVersion}`
and, for `edit`, `{text, kind, prepared?}`. Group references include the original
participant. Actions are `edit`, `delete-everyone`, or `delete-local`. Native ownership and current stored content are authoritative.
`mutations({chat})` exposes unfinished actions. Prepared actions require an explicit
resume; queued actions recovered after a crash become uncertain and are never resent.

Protected edits publish a fresh v2 wire for the same verified recipient. Neither
editing nor deleting a WhatsApp row revokes access to previously granted CIFER
publication links. The host must communicate that limitation.

### Native encrypted edits

Current WhatsApp clients also send `secretEncryptedMessage` edits. The engine
retains the original 32-byte message secret inside that message's encrypted SQLite
record. It never appears in public method results, events, retry callbacks, or logs.
It shares the message's retention/size budget and is removed with its message.
A tightly scoped, hash-checked build-time patch to the pinned Baileys decoder keeps
outer message context when unwrapping an own linked-device message. Run
`prepare:baileys` after installing with scripts disabled; CIFER's bundle builder
also applies and verifies this patch. No runtime patch or download occurs.

Decryption uses the standard HKDF-SHA256/AES-GCM add-on format, binding the original
message ID and authenticated author's PN/LID identities. Chat and direction come
from the authenticated transport envelope, never the embedded target. Unsupported
or unauthenticated ciphertext cannot replace a stored body. See the protocol
reference in [Baileys PR 2743](https://github.com/WhiskeySockets/Baileys/pull/2743);
the core keeps its reviewed Baileys release and authenticates edit authors in both
direct and group conversations.

An encrypted edit arriving before its original/secret is stored encrypted for at
most two minutes and within the shortest known disappearance/retention deadline.
If a cached older message lacks its secret, its stale body is replaced with an
unavailable-edit placeholder while history recovery is pending. Expired pending
edits create a content-free tombstone. Messages processed by older CIFER builds
may lack this secret; an upgrade cannot recover a previously discarded edit unless
WhatsApp supplies the relevant history again. Fresh native-client edits must be
verified on the signed candidate before release.

## Existing groups and archive

Group conversations preserve authenticated participant identity independently of the
conversation. Group message references include participant; direct-chat identifiers
remain compatible. Sender labels are display data, never authority. Discovery is
bounded and histories are loaded lazily. Membership/announcement permissions are
refreshed before transmission; group creation and administration are not exposed.
Group receipts count distinct recipients (Delivered to N / Read by N), never infer
that all members read a message. Edits require the original author. Administrator
revokes without historical authorization evidence remain deferred rather than
modifying another member's content using a lossy event.

Extra CIFER protection remains direct-chat-only. Protected group payloads are
retained as opaque protected messages for an explicit unsupported/locked display;
they do not authorize a group recipient binding or automatic key-service access.

Archived chats keep their own unread state but are absent from global notification
counts. The active/archived filter is applied before pagination. The account's
WhatsApp keep-archived preference remains authoritative. Archiving does not mark
read, delete content or alter mute settings. Offline or incomplete-range actions
fail safely; ambiguous patches are reconciled rather than automatically replayed.

Content-free encrypted protocol ranges (keys, timestamps, participants) may outlive
message bodies. They are capped at 128 per chat / 10,000 overall; incompleteness is
explicit and refresh never restores expired content. Group roster caches are
bounded to 20 groups / 10,000 participants / 24 hours. Disconnect clears these
namespaces. Message content still follows the existing seven-day/10,000/64-MiB
limits and earlier disappearing-message deadlines. Read receipts apply only to
selected visible messages; whole-chat read synchronization uses a verified cutoff
and never clears a newer arrival.

These source changes require packaged, real-account qualification before a public
Mac release. Synthetic fixtures alone are not proof of live WhatsApp behavior.

### Incoming photos

`photo` takes `{ chat, messageId, fromMe, participant?, expectedVersion, retry? }`.
The participant is mandatory for groups. Message summaries expose only
`photo: { available }`; media locators and keys stay in encrypted local records.
A ready result contains a validated raster data URI and its message expiry.
Other states are `offline`, `busy`, `unavailable`, `refresh-required` and `waiting`.

Only visible conversation photos should be requested. Downloads use WhatsApp
media hosts with redirects disabled and a 12 MiB limit. The engine verifies the
encrypted hash, MAC and plaintext hash before accepting JPEG, PNG or static WebP
(up to 32 megapixels and 8192 pixels on either side). At most two downloads run
at once. Cached bytes share the existing 64 MiB encrypted content budget and are
evicted before message bodies; they never displace authentication. Expiry, edits,
deletions, alias changes and account shutdown invalidate cached or pending photos.
View-once content is never downloaded or retained.

Older versions discarded attachment descriptors. Explicit refresh can ask the
linked phone to resend such a message, while history replay can also restore its
descriptor. A manual retry can renew an expired WhatsApp CDN locator; the original
content hashes and media key must still match. Unavailable photos remain retryable
and require the phone to retain the original. No CIFER cloud service handles photo
bytes, and the engine writes no plaintext media files.

## Emoji reactions

Direct chats and existing groups support receiving, adding, changing and removing
emoji reactions. Message pages expose only `reactions: [{emoji,count,mine}]`;
reactor identities stay in encrypted native storage. Use the durable
`prepareMutation` / `mutate` flow with `action: "react"`, `text` containing one
emoji (or an empty string to remove), the original message reference and its
`expectedVersion`. Group references require the original sender's participant ID.
No message body or CIFER grant is published for a reaction.

The engine reads standalone reaction events, buffered upserts and history,
reconciles PN/LID identities and keeps one latest reaction per person. Removals
retain ordering tombstones until the original expires, so delayed adds cannot
restore an old reaction. Reactions do not alter message content versions, unread
counts or conversation ordering. Members of ordinary announcement groups may
react even when only admins can send messages; membership is refreshed before I/O.

Reaction records share the encrypted 64-MiB content budget, with at most 2,048 per
message and 10,000 overall. Unknown-message reactions wait at most two minutes;
known-message reactions expire with the original, including earlier disappearance.
Deletion and disconnect erase them. Uncertain sends are never automatically
replayed; an authenticated own-device echo can reconcile the operation.

`prepare:baileys` also applies a hash-pinned group-buffer identity fix. It preserves
participants in buffered keys so two group authors using the same message ID
cannot merge their messages or reactions. The direct-chat key format is unchanged.

Business template, button and interactive cards display their text/footer and supported
photo through the same encrypted photo path. Native-flow JSON and action buttons are
not executed. Interactive transport wrappers are recognized without exposing ordinary
view-once media or templates marked hidden on linked devices. Recent refresh repairs
pre-upgrade reactions and skipped cards from the paired phone; expired/deleted
content remains excluded. Availability depends on the phone supplying that history. Exact-message recovery supplies only the known key and timestamp as fallback metadata. The pinned phone-response decoder retains returned reaction aggregates and secrets, rejects conflicting ownership, and never borrows a timestamp from another message. Compact aggregate reaction keys inherit their enclosing chat; group actor identities remain required.

Encrypted reaction envelopes use the existing per-message secret, the authenticated sender and original author, HKDF `Enc Reaction`, and AES-GCM with empty AAD. Pending envelopes are encrypted, bounded, expire within two minutes, and are erased with their target or account. Protocol references: [whatsmeow message secrets](https://github.com/tulir/whatsmeow/blob/main/msgsecret.go) and [wacore reaction format](https://docs.rs/wacore/latest/wacore/reaction/index.html). Explicit on-demand history uses a request-specific Baileys buffer key so refreshed reactions are not suppressed by initial-sync deduplication.

Recent recovery starts from the latest visible message when available. A newer reaction or protocol record in the archive/read synchronization range does not replace that target. One additional exact-message request may recover newer unsupported content; no message ID or timestamp is invented.
