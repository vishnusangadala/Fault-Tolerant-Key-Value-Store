# Wire protocol

Each TCP connection carries one UTF-8 JSON request terminated by newline and one JSON response terminated by newline. JSON escaping supports newline/NUL characters within values. Maximum input frame is approximately 8 MiB (checked between receive chunks), keys 4096 bytes, values 16384 bytes. The CLI uses command arguments and cannot represent NUL; programmatic JSON clients can.

| Request | Example | Successful result |
|---|---|---|
| Status | `{"type":"status"}` | ID, role, term, leader ID, commit/last indices |
| Put | `{"type":"put","key":"a","value":"b"}` | `{"ok":true,"index":2}` |
| Get | `{"type":"get","key":"a"}` | `{"ok":true,"found":true,"value":"b"}` |
| Delete | `{"type":"del","key":"a"}` | `{"ok":true,"index":4}` |
| Test isolation | `{"type":"isolate","enabled":true}` | Local status; false heals the node |

Missing keys return `found:false` and an empty value. An existing empty value returns `found:true`. Followers return `not_leader` and a leader ID (`-1` means unknown); map the ID to your configured endpoint. Leaders without a usable majority return `unavailable` with unknown operation outcome. Invalid client fields return `invalid_request`. Invalid framing closes the connection. CLI exit codes: 0 success, 2 application rejection, 1 transport/configuration exception.

Raft RPCs use `vote` (term, candidate ID, last index/term) and `append` (term, leader ID, previous index/term, entries, commit index). They are internal trusted-peer messages, not a validated public API. The isolation endpoint blocks inbound/outbound Raft traffic while leaving status and client requests reachable; it is for deterministic lab tests only.
