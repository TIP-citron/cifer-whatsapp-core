# Security model

This engine is intended to run in an isolated, signed native helper under a trusted
Mac supervisor. It is a WhatsApp endpoint and necessarily handles ordinary
plaintext in memory. It does not claim protection from malware running as the user,
an administrator, a compromised native host, or a malicious software update.

## Boundaries

1. The native host verifies the signed-in CIFER account, owns the filesystem path
   and Keychain key, and passes a fixed account/connection binding over private IPC.
   The renderer never chooses those values.
2. A store is bound to exactly that account/connection. Encryption authenticates
   record identity and namespace. A fixed encrypted binding record detects a wrong
   key before any fresh account state is created.
3. The engine owns one Baileys socket. Its generation invalidates stale credential,
   message, history and send callbacks on pause/disconnect. The host separately
   rejects responses/events from a previous account generation.
4. Ordinary content never enters a CIFER API here. Protected messages are validated
   opaque ciphertext and their separate publication/key-opening boundary belongs
   to the host integration. `prepared` accepts only the documented public-key and
   ciphertext fields; its contact binding must still match local confirmation when
   preparing or sending.

No listener, cloud token, arbitrary URL operation, generic decryption operation,
filesystem request or shell command is exposed. Requests reject extra fields and
validate peer IDs and sizes. Direct and existing group conversations are supported; protected recipient bindings remain individual-only. Group authorship and receipt identity are distinct from conversation identity.

Profile photos are a separate bounded download path: only already-known chats
can schedule a WhatsApp preview query. Its returned URL must be HTTPS on a
WhatsApp subdomain, with no credentials, custom port or fragment; redirects are
rejected and no cookies/referrer are supplied. Content length and streamed bytes
are capped at 32 KiB. Only bounded, nonanimated JPEG/PNG/WebP raster containers
are returned over IPC as data URIs. Image decoding remains part of the trusted
renderer runtime; the native boundary and UI also validate the thumbnail.
This does not add an egress firewall around all Baileys history/app-state traffic.

## Persistence and lifecycle

Credentials and Signal key batches are transactional. The storage key is copied
from a 32-byte binary input, never an environment variable/argument, and the engine
clears its copy after closing. The platform host is responsible for Keychain
wrapping, locking and removing the wrapped key on explicit account erasure.

Serialized message/outbox/retry content shares a 64 MiB budget; inbox messages have
a 10,000-record and seven-day limit. Shorter disappearance settings shorten these
lifetimes. Completed outbox bodies are scrubbed, and minimal idempotency hashes
remain until unlink. Timing, sizes, namespace and unread counts remain observable
in the encrypted SQLite file. WAL checkpointing and secure_delete limit live file
remnants; historical OS backups or externally copied files cannot be recalled.

Edits scrub original outbox bodies, deletes create tombstones and expiry is checked
before reads. Alias merges apply tombstones first and keep stricter expiry. A
prepared operation cannot silently move to another confirmed contact. Ambiguous
sends are uncertain; the engine does not resend them automatically.

Encrypted profile cache is bound to the same account/connection, capped at 100
records and 4 MiB, and expires after one hour (negative answers ten minutes,
transient failures one minute). Expired bytes cannot be read and are pruned on
the next cleanup; the renderer clears its own copy on lock/account switch.
Photo removal invalidates cached data and in-flight work. Close/pause aborts
downloads and prevents stale completions from writing; unlink clears the cache.

Before importing the engine in the helper, silence dependency console methods.
Never log transport errors, auth objects, QR codes or message content. The bundled
native helper must disable crash payload collection that would expose these values.

## Validation and release limits

Automated tests use synthetic identities/messages, an injected socket and the
actual pinned Baileys protocol/Signal serialization implementations. They cover
store encryption and binding, credential transactions, edits/revokes, expiry,
alias/tombstone conflicts, bounded content, operation recovery, uncertain sends,
contact binding changes and close/disconnect races.

A release still needs real phone canaries, native IPC/account-isolation tests,
Keychain/universal-binary packaging validation and a network trace. Baileys/WhatsApp
protocol compatibility can change. A clean dependency advisory report is not a
proof that a dependency has no vulnerabilities.

For a suspected vulnerability, share a minimal synthetic reproduction with the
maintainer through an agreed private channel. Do not include real WhatsApp
credentials, message contents, pairing material or a storage database/key.

Group read/delivery evidence is per recipient. Participant PN/LID aliases cannot
merge group conversations or grant CIFER access. Admin-revoke role evidence is
bound to a unique helper session and lifecycle epoch, preventing stale persisted
roles from authorizing mutations after restart. Unknown historical authorization
is deferred. Group discovery, metadata fetches, history ingestion and read/archive
callbacks retain their original generation across every awaited stage.

Encrypted synchronization metadata contains no bodies/media/Signal secrets and
may survive body expiry to support older-chat archive operations. It has separate
hard bounds and is erased on unlink. Archived unread rows are retained but omitted
from all notification and badge summaries. No archive operation implicitly reads
a conversation; uncertain archive changes are never automatically replayed.

Reaction authors are derived from authenticated WhatsApp envelope keys, never from
renderer claims. Original group message identities include their participant.
Cross-conversation reaction targets are rejected. Only one bounded emoji or an
empty removal is accepted, and renderer results omit raw author/key objects.
Reaction metadata has the original content's expiry and participates in content
quota/erasure. A helper generation change fences incoming and outgoing reactions.


Received media uses encrypted, account-bound SQLite chunk records. Interrupted
downloads are never marked readable and are removed on restart. Native playback
capabilities expire and are scoped to the helper epoch and exact message version.
The host must keep range reads private, prohibit backend fallthrough, set no-store
responses and terminate playback on lock. File lengths, encrypted hashes, MACs,
plaintext hashes and container headers are verified before a capability is issued.
