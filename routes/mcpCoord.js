// routes/mcpCoord.js - JSON-RPC 2.0 MCP shim at POST /api/mcp/coord
//
// 8 coord.* tools exposed via the standard MCP protocol so Claude Code can
// wire them in as a connector via .mcp.json. Auth = laptop-agent's existing
// Bearer (AGENT_TOKEN). Per-call identity threaded via:
//   - X-Tab-Id / X-Tab-Credential headers (preferred)
//   - or params.tab_id / params.tab_credential (fallback)
// Tab cred validation is best-effort in v1: requests for inbox operations
// resolve the topic from ctx.tab_id, mismatched-cred calls still go through
// but will only operate on the topic for the asserted tab_id. (No cross-tab
// inbox theft because each topic is unique to tab_id.)

const coord = require('../tools/coord')
const usage = require('../tools/usage')

const PROTOCOL_VERSION = '2025-03-26'
const SERVER_INFO = Object.freeze({ name: 'EcodiaOS Coord', version: '1.1.0' })

const RPC_ERR = Object.freeze({
  PARSE_ERROR: { code: -32700, message: 'Parse error' },
  INVALID_REQUEST: { code: -32600, message: 'Invalid Request' },
  METHOD_NOT_FOUND: { code: -32601, message: 'Method not found' },
  INVALID_PARAMS: { code: -32602, message: 'Invalid params' },
  INTERNAL_ERROR: { code: -32603, message: 'Internal error' },
  UNAUTHENTICATED: { code: -32001, message: 'Unauthenticated' },
})

const TOOLS = Object.freeze([
  {
    name: 'coord.send_message',
    description: 'Send a message to an inbox topic. to = "chat.<tab_id>.inbox" | "chat.conductor.inbox" | etc. body = arbitrary JSON object. Returns {message_id, created_at}. Delivery: a message whose body.type === "chat" is ALSO pushed as a live turn into the target chat\'s tab (position-addressed injection) and the result carries a `delivery` field; set body.deliver:"queue" to force inbox-only. All other body types (done/progress/bound/etc) are inbox-only, unchanged. For chat-to-chat coordination prefer coord.message_chat, which is the ergonomic wrapper.',
    inputSchema: {
      type: 'object',
      properties: {
        to: { type: 'string', description: 'Inbox topic, e.g. "chat.conductor.inbox"' },
        body: { type: 'object', description: 'Arbitrary JSON message body' },
        task_id: { type: 'string', description: 'Optional task correlation id' },
        in_reply_to: { type: 'string', description: 'Optional id of message this replies to' },
      },
      required: ['to', 'body'],
      additionalProperties: true,
    },
  },
  {
    name: 'coord.message_chat',
    description: 'Push a message straight into ANOTHER live Claude Code chat as a new turn - how chats coordinate without polling an inbox. `to` accepts EXACT identities ONLY (v5, 2026-08-28): a full "chat.*.inbox" address, the literal "conductor", "session:<id>", a worker tab_id, or a worker task_id. Anything else is an ERROR and nothing is sent or queued: {ok:false, error:"unresolved_target", reason:"not_a_stable_identity"|"label_address_retired"|"conductor_unregistered", hint}. Fuzzy targeting is GONE - a chat name, a context phrase or a partial tab label no longer resolve. It was deleted because it could not be made correct: it matched on the Claude Code auto-title, which is generated from the user first words, truncated to ~24 chars, retitled without notice and duplicated across tabs, so a confident match was still a guess (measured 2026-08-28: a send to "conductor" landed on a tab titled "Resi sent you a few emai..."). Get a real target from coord.list_channels: use a row `address`, and treat address:null / addressable:false as "not reachable yet" rather than something to guess at. Returns on success {ok:true, to_address, resolved_by, delivered, delivery, message_id}; delivered:false means it persisted to the inbox but could not inject live (target mid-turn / rate-limited / away). Injection steals focus (kill-switch env COORD_CHAT_INJECT=0).',
    inputSchema: {
      type: 'object',
      properties: {
        to: { type: 'string', description: 'Target chat, as an EXACT identity: a full "chat.*.inbox" address or "chat.session:<id>.inbox" (both from coord.list_channels), the literal "conductor", "session:<id>", a worker tab_id, or a worker task_id. A chat name, context phrase or partial tab label is REFUSED, not guessed at.' },
        text: { type: 'string', description: 'The message to land as a turn in the target chat. Optional when resolve_only is true.' },
        resolve_only: { type: 'boolean', description: 'Dry-run: return the resolution decision ({would_send_to, resolved_by, parent_rewrite?}) WITHOUT sending, or the same error a real send would return. Use to check "who would this route to?" before committing a send.' },
        from_label: { type: 'string', description: 'How you want to be named to the recipient (e.g. "Chambers chat"). Defaults to your tab identity.' },
        from_address: { type: 'string', description: 'The address the recipient should reply to. Defaults to your own inbox (worker) or the conductor. Set this so a peer chat can reply directly to you.' },
        task_id: { type: 'string', description: 'Optional task correlation id.' },
        in_reply_to: { type: 'string', description: 'Optional id of the coord message this replies to.' },
      },
      required: ['to', 'text'],
      additionalProperties: true,
    },
  },
  {
    name: 'coord.name_chat',
    description: 'Label THIS chat in the coordination directory so a human or a peer can tell which chat is which. Sets a short kebab name (e.g. "studio-ux"), an optional one-line context ("Studio editor UX pass"), and optional aliases; writes them into this chat session anchor and they PERSIST across turns, where the Claude Code auto-title does not. NOTE (changed 2026-08-28): a name is a LABEL, not an address. coord.message_chat({to:"studio-ux"}) no longer works and returns an error - name matching was part of the fuzzy resolver and was deleted with it. The name appears in coord.list_channels beside that row stable `address`, which is what you actually send to. Targets the most-recently-active chat (the caller) by default; pass session_id to target a specific one. Returns {ok, session_id, name, address, note}.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Short stable handle for this chat, e.g. "studio-ux" or "friend-motion". Kebab/lowercase reads best but any string works (matched case- and punctuation-insensitively).' },
        context: { type: 'string', description: 'One line on what this chat is doing, e.g. "Studio editor breadcrumb + pin-release". Used as a secondary match signal and shown in list_channels.' },
        aliases: { type: 'array', items: { type: 'string' }, description: 'Optional extra handles this chat also answers to.' },
        session_id: { type: 'string', description: 'Optional: target a specific chat by its Claude Code session_id instead of the most-recently-active one.' },
      },
      additionalProperties: true,
    },
  },
  {
    name: 'coord.list_channels',
    description: 'List every live Claude Code chat (the coordination directory), most-reliably-targetable first. Each entry: {address (the target, or NULL), name, context, session_address (stable across retitles), label, kind: conductor|worker|chat, active, addressable, task_id?, worker_status?}. Send to `address`. A row with address:null / addressable:false has NO stable identity and is NOT reachable - there is no label fallback to guess one (that guess is what misrouted, so label_address was removed 2026-08-28). Such a chat becomes addressable by taking a user turn, which anchors its session_id. `name` is a human label for picking the right row, never a target. Returns {ok, count, channels[], your_address, inject_enabled}.',
    inputSchema: { type: 'object', properties: {}, additionalProperties: true },
  },
  {
    name: 'coord.read_inbox',
    description: 'Read unread messages from your inbox (resolves from your tab_id). Returns {topic, count, messages[]}. Marks returned messages as seen.',
    inputSchema: {
      type: 'object',
      properties: {
        topic: { type: 'string', description: 'Override topic; defaults to your tab_id inbox' },
        since: { type: 'string', format: 'date-time' },
        limit: { type: 'integer', minimum: 1, maximum: 200, default: 50 },
      },
      additionalProperties: true,
    },
  },
  {
    name: 'coord.peek_inbox',
    description: 'Same as read_inbox but does NOT mark messages seen. Use for non-consuming probes (wait-loops, observer flows) where a downstream read_inbox should still see the message.',
    inputSchema: {
      type: 'object',
      properties: {
        topic: { type: 'string' },
        since: { type: 'string', format: 'date-time' },
        limit: { type: 'integer', minimum: 1, maximum: 200, default: 50 },
      },
      additionalProperties: true,
    },
  },
  {
    name: 'coord.wait_for_inbox',
    description: 'Long-poll for the next inbox message. Returns {trigger_message, also_unread[<=20], more_unread, hold_duration_ms, timed_out}. USE timeout<=45 AND LOOP. The server holds up to 600s, but an MCP CLIENT deadline kills the call between 45s and 60s: measured 2026-08-29 by two independent callers, 45s returned cleanly at 45075ms while 60s and 120s both returned a bare transport error with NO envelope. A caller past that wall loses the tool result AND any message that arrived during the hold, so a longer hold is strictly worse than a loop of short ones. The 600 ceiling below is the SERVER cap and is reachable only over direct HTTP, not through MCP.',
    inputSchema: {
      type: 'object',
      properties: {
        topic: { type: 'string' },
        // maximum stays 600 (the real server cap, valid over direct HTTP); default drops to
        // 45 so an MCP caller that omits timeout lands UNDER the client wall instead of on
        // the far side of it. The old default of 300 could never return through MCP.
        timeout: { type: 'integer', minimum: 1, maximum: 600, default: 45 },
      },
      additionalProperties: true,
    },
  },
  {
    name: 'coord.ack_message',
    description: 'Acknowledge a message you actioned. id = message_id. action_summary = optional human-readable note (max 2000 chars).',
    inputSchema: {
      type: 'object',
      properties: {
        id: { type: 'string' },
        action_summary: { type: 'string', maxLength: 2000 },
      },
      required: ['id'],
      additionalProperties: true,
    },
  },
  {
    name: 'coord.list_workers',
    description: 'List currently-registered worker tabs. `dead` means terminated_at is set, and NOTHING ELSE: include_dead=false (default) hides exactly those. It no longer hides a worker on heartbeat age, which until 2026-09-09 made this call a permanent false absence (every live worker crossed the 90s staleness bound within two minutes of registering, so the default returned the empty set at all times). Heartbeat age is still reported, under its own honest name: `stale_ms` and `heartbeat_stale`. If you actually want an age filter, ask for it with max_stale_ms and pick your own threshold. READ terminated_reason BEFORE TREATING dead:true AS DEATH: terminated_at has two provenances. "signal_done"/"kill" is a real terminal event; "stale_heartbeat" means the 60-minute sweep manufactured it from heartbeat age alone, which is not proof of death (measured 2026-09-08, at least 2 of 4 such rows wrote a transcript turn minutes AFTER the stamp). stale_at_termination_ms carries the age that triggered such a sweep. For a hard liveness answer that does not depend on a worker remembering to heartbeat, use the transcript-mtime oracle in tools/worker-liveness.js.',
    inputSchema: {
      type: 'object',
      properties: {
        include_dead: { type: 'boolean', default: false, description: 'Include workers carrying terminated_at. Default false.' },
        max_stale_ms: { type: 'number', description: 'Optional opt-in heartbeat-age filter: drop workers whose last_heartbeat_at is at least this old. Omit for no age filtering (the default, and the safe one - a live worker wrongly filtered out is what makes a consumer re-dispatch or reap it).' },
      },
      additionalProperties: true,
    },
  },
  {
    name: 'coord.heartbeat',
    description: 'Update your last_heartbeat_at. Call at the start and end of every turn. status = optional one-line summary of what you are doing. in_critical_section = optional bool (mid-write that should not be interrupted).',
    inputSchema: {
      type: 'object',
      properties: {
        status: { type: 'string', maxLength: 500 },
        in_critical_section: { type: 'boolean' },
      },
      additionalProperties: true,
    },
  },
  {
    name: 'coord.report_progress',
    description: 'Record a one-line progress note on YOUR scheduled task row (os_scheduled_tasks.progress_summary). Advisory only: nothing in the dispatch path reads it, it exists so an operator can see a long worker is still moving. Single-valued - each call REPLACES the previous note, it is not a log. Writes nothing to any inbox. The write is refused unless the row is currently dispatched to your tab; a refusal returns {ok:false, error} and is not a failure of your task.',
    inputSchema: {
      type: 'object',
      properties: {
        task_id: { type: 'string' },
        summary: { type: 'string' },
      },
      required: ['summary'],
      additionalProperties: true,
    },
  },
  {
    name: 'coord.verify_paste',
    description: 'Fetch the canonical brief from disk for your task_id. The brief that was pasted into your chat may be truncated by a clipboard race under memory pressure - this tool returns the audit-file version that the dispatcher wrote BEFORE the paste, so it cannot be corrupted by paste-side issues. Call this FIRST on every dispatched worker. Treat the returned brief_body as source-of-truth. Returns {ok, task_id, tab_id, brief_file, brief_size_bytes, brief_sha256, brief_body, registered_at, parent_conductor_tab_id}.',
    inputSchema: {
      type: 'object',
      properties: {
        task_id: { type: 'string', description: 'Optional; defaults to your worker row\'s task_id.' },
      },
      additionalProperties: true,
    },
  },
  {
    name: 'coord.signal_done',
    description: 'Signal task completion. Writes the completion onto YOUR scheduled task row (done_at/done_status/done_summary/done_pointer) - that row write is what the scheduler acts on, and the write is REFUSED unless the row is currently dispatched to your tab. Also posts a human-facing {type:"worker_report"} notice to chat.conductor.inbox so the conductor wake surfaces it; nothing machine-readable depends on that notice. status is load-bearing: omit it and success is assumed; pass "failed" to route through markFailed, or "stood_down" for a terminal non-failure. If terminate=true, marks your worker row terminated_at. result_pointer can name a file path or status_board row id holding the full output. Returns {ok, signal, message_id, close_owed, next_action, close_handoff} where ok reflects whether the SCHEDULER-VISIBLE completion landed. YOUR ARC IS NOT OVER WHEN THIS RETURNS: close_owed:true and next_action:"coord.close_my_tab" mean the tab you are running in is still open and is yours to close, so call coord.close_my_tab as the very next action. Measured over 14 days on this host, 176 of 1,584 sessions that reached signal_done never called it and left their tab open. This call now also re-captures your tab\'s stable IDE id (close_handoff) so the 7-minute cleanup_orphan_workers sweep can close the tab if you do not, but that backstop runs minutes later and behind stricter belts; closing it yourself is still the correct ending.',
    inputSchema: {
      type: 'object',
      properties: {
        task_id: { type: 'string' },
        result_summary: { type: 'string' },
        result_pointer: { type: 'string' },
        terminate: { type: 'boolean', default: false },
      },
      additionalProperties: true,
    },
  },
  {
    name: 'coord.signal_bound',
    description: 'Confirm you launched, read your brief and reached MCP. Call this on your FIRST turn: it releases the scheduler launch-lock, which is held serially across the whole fleet, so a missing bound stalls every other pending dispatch for the full timeout. Stamps bound_at on YOUR scheduled task row; posts NO message. The write is REFUSED unless the row is currently dispatched to your tab, so a stale worker cannot release a lock it does not hold. Does NOT terminate your worker row or unlink your .spawned marker (those happen at signal_done). Returns {ok, task_id, tab_id} or {ok:false, error}.',
    inputSchema: {
      type: 'object',
      properties: {
        task_id: { type: 'string', description: 'Task id from your brief or worker registration.' },
      },
      additionalProperties: true,
    },
  },
  {
    name: 'coord.close_my_tab',
    description: 'Close your own IDE chat tab. A DISPATCHED WORKER calls this as the FINAL action right after coord.signal_done({terminate:true}); it resolves the worker\'s own tab from its registry row, so pass nothing. A PLAIN CHAT (never dispatched, so it has no tab_id and coord.list_channels reports your_tab_id:null) can ALSO close itself now: call it with no tab_id and it resolves the chat the user most recently typed into, which is you when you are answering the turn that asked you to close. Pass session_id (or the session_address coord.list_channels gives you) to assert WHICH chat you are, and the call refuses on any disagreement rather than closing a stranger; without it the call additionally requires that turn to be recent. Best-effort; failures do not crash the arc. Returns {ok, tab_id, session_id, closed, strategy, refused, error}. READ `closed`, NOT `ok`: ok:true with closed:false is a REFUSED close and a leaked tab, and `refused` names exactly what stopped it (a stale or contested identity, a tab that is not focused, an anchor Claude Code has retitled, or the registered conductor tab, which can never be closed this way). NEVER pass a tab_id you were not issued to force a close: a stored id checked for life rather than ownership is what closed a live human chat labelled "Crons" while reporting closed:true. WHY THIS EXISTS: every unclosed tab accumulates in the IDE and burns memory (each Claude Code chat is a webview ~50-200MB). Measured over 14 days: of 1,432 calls, 1,303 closed and 118 refused, while a further 176 sessions ended without ever calling it.',
    inputSchema: {
      type: 'object',
      properties: {
        session_id: {
          type: 'string',
          description: 'PLAIN CHATS ONLY (a worker leaves this out and is resolved by its tab_id). Your own Claude Code session id, asserting which chat is calling. It must match the chat that most recently took a user turn, or the call REFUSES with not_recent_active_session rather than closing anything. Supplying it also lifts the recency requirement that an unasserted close carries.',
        },
        session_address: {
          type: 'string',
          description: 'Alternative spelling of session_id: the `chat.session:<id>.inbox` address coord.list_channels hands out, accepted verbatim so you can paste back exactly what you were given.',
        },
      },
      additionalProperties: true,
    },
  },
  {
    name: 'coord.kill_worker',
    description: 'HARD-stop ONE dispatched worker by closing its IDE chat tab. The conductor-side counterpart to the worker-side stand_down: use it when a worker will not stand down on its own (never polls its inbox, wedged, or mid-run on work you have cancelled). Cancelling the os_scheduled_tasks row only stops re-dispatch, it does not touch the live tab. Prefer a stand_down message first (coord.send_message body {type:"stand_down"}) so the worker can stop cleanly and report what it did; kill_worker is the enforcement when that goes unread. Targets EXACTLY the one worker named by target_tab_id and can never match more than one tab: it takes an exact id, not a filter, and resolves it through the worker registry to a single stored tab handle. Refuses an absent/wildcard target, the conductor tab, and any id with no registry row. Returns {ok, target_tab_id, closed, refused, error, killed_by}. closed=false with a refused reason means the tab was left alone on purpose (better a leaked tab than a wrong close).',
    inputSchema: {
      type: 'object',
      properties: {
        target_tab_id: { type: 'string', description: 'The EXACT tab_id of the worker to close, e.g. "tab_1784292867909_94dddf94" (from coord.list_workers). Never a filter, glob, label, or "all". Deliberately NOT named tab_id: that arg is the CALLER identity on every coord.* tool.' },
        reason: { type: 'string', description: 'Why this worker is being killed. Recorded in the agent log for audit.' },
      },
      required: ['target_tab_id'],
      additionalProperties: true,
    },
  },
  {
    name: 'coord.pick_account',
    description: 'Select the account with the most VENDOR-MEASURED headroom for the next dispatch. Ranks by 1 - max(measured used_5h, used_weekly) from the OAuth usage endpoint (falls back to the ccusage estimate only when no measurement exists); a capped account is never offered. score is a 0..1 headroom fraction (buffered 0.85, minus the estimate as a 5h fraction), NOT a token count. Returns {account, score, used_5h, used_weekly, source, remaining_5h, remaining_weekly, reason, candidates[], cap_excluded[], flaky_excluded[]}.',
    inputSchema: {
      type: 'object',
      properties: {
        estimated_tokens: { type: 'integer', minimum: 0, description: 'Optional estimate of tokens this dispatch will consume. Defaults to 0.' },
        exclude: { type: 'array', items: { type: 'string' }, description: 'Optional account labels to exclude (e.g. flaky / known-failing).' },
      },
      additionalProperties: true,
    },
  },
  {
    name: 'coord.get_usage_state',
    description: 'Read the latest usage snapshot. Per-account fields are VENDOR-MEASURED: used_5h, used_weekly (from the OAuth usage endpoint, in real.utilization_5h/7d), headroom_score (1 - max of the two), headroom_source (real|real_aged|estimate|ccusage-fallback), and capped. The measured `real` block is refreshed ~60s; the ccusage tokens_* are a raw between-probe estimate and do NOT drive headroom. alerts (current_account_low, all_low, accounts_low, threshold) key off the measured headroom. READ used_5h/used_weekly, not tokens_*/headroom_5h_fraction from memory.',
    inputSchema: { type: 'object', properties: {}, additionalProperties: true },
  },
  {
    name: 'coord.poll_now',
    description: 'Force an immediate usage poll (runs ccusage + rebuilds account state). Normally called only on-demand; the cron runs every 5min. Returns the same shape as get_usage_state.state.',
    inputSchema: { type: 'object', properties: {}, additionalProperties: true },
  },
  {
    name: 'coord.get_active_account',
    description: 'Read the account currently loaded in ~/.claude/.credentials.json. Updated by cowork.swap_creds; the conductor reads at turn-start.',
    inputSchema: { type: 'object', properties: {}, additionalProperties: true },
  },
  {
    name: 'coord.set_active_account',
    description: 'Manually set the active account label (e.g. after Tate performs a manual cred swap). cowork.swap_creds calls this internally. Bootstrap: Tate runs this once with the account whose creds are currently in .credentials.json.',
    inputSchema: {
      type: 'object',
      properties: {
        account: { type: 'string', description: 'tate@ecodia.au | code@ecodia.au | money@ecodia.au' },
        set_by: { type: 'string', description: 'Optional caller label for audit (e.g. "bootstrap-by-tate", "swap-creds-cron").' },
      },
      required: ['account'],
      additionalProperties: true,
    },
  },
  {
    name: 'coord.mark_flaky',
    description: 'Mark an account as flaky after a dispatch failure. Excluded from pick_account for FLAKY_TTL_MS (10min). dispatch_worker calls this after its recovery state machine exhausts attempts on an account. Self-heals on TTL.',
    inputSchema: {
      type: 'object',
      properties: {
        account: { type: 'string' },
        reason: { type: 'string', maxLength: 500 },
      },
      required: ['account'],
      additionalProperties: true,
    },
  },
  {
    name: 'coord.clear_flaky',
    description: 'Manually clear a flaky-account marker (escape hatch; otherwise auto-expires after 10min).',
    inputSchema: {
      type: 'object',
      properties: { account: { type: 'string' } },
      required: ['account'],
      additionalProperties: true,
    },
  },
  {
    name: 'coord.list_flaky',
    description: 'List currently-flaky accounts + expired entries for audit. Returns {active[], expired[], ttl_ms}.',
    inputSchema: { type: 'object', properties: {}, additionalProperties: true },
  },
  {
    name: 'coord.register_conductor',
    description: 'Register THIS Claude Code tab as the conductor. Persists ide + window title-match so the wake hook can flash/focus this tab when a chat.conductor.* message lands. Call once per conductor tab (and after IDE relaunch / window-title change). If title_match is omitted, captures the current foreground window title.',
    inputSchema: {
      type: 'object',
      properties: {
        tab_id: { type: 'string', description: 'Optional explicit conductor tab id; defaults to "conductor"' },
        ide: { type: 'string', description: 'cursor | stable | insiders' },
        title_match: { type: 'string', description: 'Substring used to refind the window during wake. Auto-captured from foreground if omitted.' },
      },
      additionalProperties: true,
    },
  },
  {
    name: 'coord.unregister_conductor',
    description: 'Remove the registered conductor row(s). Wake hook still fires the toast (which does not depend on registration) but skips flash / auto_type.',
    inputSchema: { type: 'object', properties: { tab_id: { type: 'string' } }, additionalProperties: true },
  },
  {
    name: 'coord.get_conductor_state',
    description: 'Read the currently-registered conductor + active wake policy + last-wake timestamp. Diagnostic / preflight.',
    inputSchema: { type: 'object', properties: {}, additionalProperties: true },
  },
  {
    name: 'coord.set_wake_policy',
    description: 'Configure how the conductor is woken when messages land in chat.conductor.*. mode = "toast" (default - notification only) | "flash" (toast + flash conductor taskbar) | "auto_type" (toast + flash + focus + paste wake message into chat, then press enter) | "silent" (disable wake entirely). notify_types filters by body.type. Live types: "worker_report" (a dispatched worker finished), "error", "inbound_sms", "inbound_telegram", or "*" for all. Defaults: mode=toast, notify_types=[worker_report, error, inbound_sms, inbound_telegram]. NOTE the whole list is REPLACED by what you pass, never merged, so omitting a type unsubscribes from it. Legacy: "done" and "progress" are retired types that nothing produces any more (a worker completion is now a row write plus a worker_report notice); a policy still listing "done" is honoured for worker_report so an old on-disk policy does not silently stop waking.',
    inputSchema: {
      type: 'object',
      properties: {
        mode: { type: 'string', enum: ['toast', 'flash', 'auto_type', 'silent'] },
        notify_types: { type: 'array', items: { type: 'string' } },
        toast_duration_ms: { type: 'integer', minimum: 1500, maximum: 15000 },
        rate_limit_ms: { type: 'integer', minimum: 0, maximum: 60000 },
      },
      additionalProperties: true,
    },
  },
  {
    name: 'coord.conductor_heartbeat',
    description: 'Update the registered conductor row last_seen_at to now. Called by the Corazon UserPromptSubmit hook each turn-start so loadActiveConductorRegistration returns the row. Accepts optional refresh fields (title_match, hwnd, exe, claude_port, ide_pid, ide_bridge_port, workspace_root) to update moving values on each beat.',
    inputSchema: {
      type: 'object',
      properties: {
        title_match: { type: 'string' }, hwnd: { type: 'integer' }, exe: { type: 'string' },
        claude_port: { type: 'integer' }, ide_pid: { type: 'integer' },
        ide_bridge_port: { type: 'integer' }, workspace_root: { type: 'string' },
      },
      additionalProperties: true,
    },
  },
  {
    name: 'coord.set_conductor_in_turn',
    description: '2026-05-19 one-conductor-many-channels mutex: set in_turn=true at UserPromptSubmit (heartbeat hook), in_turn=false at Stop (turn-end hook). While in_turn=true, reflex.append_to_conductor defers paste and leaves the message in coord inbox; the Stop hook flushes the inbox at turn end. 10-min TTL auto-clear handles crashed turns.',
    inputSchema: {
      type: 'object',
      properties: { in_turn: { type: 'boolean' } },
      required: ['in_turn'],
      additionalProperties: true,
    },
  },
])

// PATCH 2026-05-18 drift-audit: MCP clients treat inputSchema.properties as an
// allowlist regardless of additionalProperties=true. Workers calling coord.*
// with passthrough {tab_id, tab_credential} had those args stripped by the
// client before they reached extractCtx, so every coord call returned
// "tab_id required". Inject the two ctx props into every tool's properties so
// the MCP client passes them through. Object.freeze is shallow so the inner
// properties objects are mutable.
const CTX_PROPS = Object.freeze({
  tab_id: { type: 'string', description: 'Worker tab id passthrough (MCP transports without X-Tab-Id header).' },
  tab_credential: { type: 'string', description: 'Worker tab credential passthrough (MCP transports without X-Tab-Credential header).' },
})
for (const t of TOOLS) {
  if (!t || !t.inputSchema || t.inputSchema.type !== 'object') continue
  if (!t.inputSchema.properties) t.inputSchema.properties = {}
  if (!t.inputSchema.properties.tab_id) t.inputSchema.properties.tab_id = CTX_PROPS.tab_id
  if (!t.inputSchema.properties.tab_credential) t.inputSchema.properties.tab_credential = CTX_PROPS.tab_credential
}

const TOOL_NAMES = new Set(TOOLS.map(t => t.name))

function rpcError(id, err, data) {
  return { jsonrpc: '2.0', id: id ?? null, error: { code: err.code, message: err.message, ...(data ? { data: data } : {}) } }
}
function rpcResult(id, result) {
  return { jsonrpc: '2.0', id: id ?? null, result: result }
}

function extractCtx(req, params) {
  return {
    tab_id: req.headers['x-tab-id'] || (params && params.tab_id),
    tab_credential: req.headers['x-tab-credential'] || (params && params.tab_credential),
  }
}

// Tool names are "coord.X"; we route to either coord.js (messaging primitives)
// or usage.js (account-balancing primitives) based on a whitelist.
const USAGE_TOOLS = new Set([
  'pick_account',
  'get_usage_state',
  'poll_now',
  'get_active_account',
  'set_active_account',
  'mark_flaky',
  'clear_flaky',
  'list_flaky',
])

async function callTool(toolName, params, ctx) {
  const short = toolName.replace(/^coord\./, '')
  let handler = null
  if (USAGE_TOOLS.has(short)) {
    handler = usage[short]
  } else {
    handler = coord[short]
  }
  if (typeof handler !== 'function' || short.startsWith('_')) {
    return { isError: true, body: { error: 'unknown tool: ' + toolName } }
  }
  try {
    const result = await Promise.resolve(handler(params || {}, ctx))
    // 2026-09-09 lane coord-L1: THE HEARTBEAT WIRE, previously dark on this route.
    // index.js:147 bumps last_heartbeat_at on any successful /api/tool call and
    // SKIPS the coord.* family "to avoid double-write on the explicit heartbeat
    // path". That reasoning assumed workers reach the agent over /api/tool. They
    // do not: a dispatched Claude Code worker has no agent bearer, so its ONLY
    // traffic here is coord.* over this JSON-RPC shim, and its Read/Bash/Edit
    // calls never leave the IDE at all. So the one route a worker actually uses
    // had no touch, and the one that had a touch excluded the only tools that
    // route carries. Measured 2026-09-08: 15 of 15 registered workers carried
    // last_heartbeat_at IDENTICAL to registered_at, including workers that ran
    // 30+ minutes. That is not fleet non-compliance with the "heartbeat every
    // turn" instruction; it is a wire that was never connected. The consequence
    // was a permanent FALSE ABSENCE: every worker read stale within 90s
    // (list_workers) and within 180s (scheduler STALE_WORKER_LIVENESS_MS), so
    // the primary liveness gate answered "no live worker" for every live worker
    // and only the transcript-mtime belt stood between that and a re-dispatch or
    // a worktree delete.
    // A coord call IS proof of life: the tab made it. Touch on every tool except
    // the explicit heartbeat handler, which already wrote. _touchHeartbeatForTab
    // refuses a row carrying terminated_at, so a late call cannot resurrect a
    // worker that already signalled done.
    // 2026-09-09 lane coord-L1 VERIFY-2: the credential travels with the id.
    // ctx.tab_id is ASSERTED, never authenticated (see extractCtx and the header
    // note at the top of this file). While the touch only bumped a clock that
    // was tolerable. Since e7ab40c the same call can also clear a manufactured
    // terminated_at, so the revive half now demands the row's own credential.
    // Passing it here does not gate the bump, only the revive.
    if (ctx && ctx.tab_id && short !== 'heartbeat') {
      try {
        if (typeof coord._touchHeartbeatForTab === 'function') {
          coord._touchHeartbeatForTab(ctx.tab_id, ctx.tab_credential)
        }
      } catch (e) {}
    }
    return { isError: false, body: result }
  } catch (e) {
    return { isError: true, body: { error: e.message, tool: toolName } }
  }
}

async function handleSingle(req, env) {
  const id = env.id
  const method = env.method
  const params = env.params || {}
  const isNotification = id === undefined

  try {
    if (method === 'initialize') {
      return rpcResult(id ?? null, {
        protocolVersion: PROTOCOL_VERSION,
        capabilities: { tools: { listChanged: false }, prompts: { listChanged: false }, resources: { listChanged: false } },
        serverInfo: SERVER_INFO,
      })
    }
    if (method === 'notifications/initialized' || method === 'initialized') return null
    if (method === 'ping') return rpcResult(id ?? null, {})
    if (method === 'tools/list') return rpcResult(id ?? null, { tools: TOOLS })
    if (method === 'prompts/list') return rpcResult(id ?? null, { prompts: [] })
    if (method === 'resources/list') return rpcResult(id ?? null, { resources: [] })

    if (method === 'tools/call') {
      const name = params.name
      const args = params.arguments || {}
      if (typeof name !== 'string') return rpcError(id, RPC_ERR.INVALID_PARAMS, { reason: 'name required' })
      if (!TOOL_NAMES.has(name)) return rpcError(id, RPC_ERR.METHOD_NOT_FOUND, { tool: name })
      const ctx = extractCtx(req, args)
      const out = await callTool(name, args, ctx)
      return rpcResult(id ?? null, {
        content: [{ type: 'text', text: JSON.stringify(out.body) }],
        isError: out.isError,
        _meta: { http_status: out.isError ? 500 : 200 },
      })
    }

    if (isNotification) return null
    return rpcError(id ?? null, RPC_ERR.METHOD_NOT_FOUND, { method: method })
  } catch (e) {
    if (isNotification) return null
    return rpcError(id ?? null, RPC_ERR.INTERNAL_ERROR, { error: e.message })
  }
}

function mount(app, auth) {
  // Long-poll-safe: never let Express time out on wait_for_inbox.
  app.post('/api/mcp/coord', auth, async (req, res) => {
    try {
      req.setTimeout(620 * 1000)  // 620s > 600s max wait timeout
      res.setTimeout(620 * 1000)
    } catch (e) {}

    const body = req.body
    if (body == null || typeof body !== 'object') {
      return res.status(400).json(rpcError(null, RPC_ERR.INVALID_REQUEST, { reason: 'body must be JSON' }))
    }

    if (Array.isArray(body)) {
      if (body.length === 0) return res.status(400).json(rpcError(null, RPC_ERR.INVALID_REQUEST, { reason: 'empty batch' }))
      const out = []
      for (const env of body) {
        if (!env || env.jsonrpc !== '2.0' || typeof env.method !== 'string') {
          out.push(rpcError(env?.id ?? null, RPC_ERR.INVALID_REQUEST))
          continue
        }
        const r = await handleSingle(req, env)
        if (r !== null) out.push(r)
      }
      if (out.length === 0) return res.status(204).end()
      return res.json(out)
    }

    if (body.jsonrpc !== '2.0' || typeof body.method !== 'string') {
      return res.status(400).json(rpcError(body.id ?? null, RPC_ERR.INVALID_REQUEST, { received: { jsonrpc: body.jsonrpc, method: body.method } }))
    }
    const r = await handleSingle(req, body)
    if (r === null) return res.status(204).end()
    return res.json(r)
  })

  // Also expose a GET probe so curl / browser can verify the route is alive
  app.get('/api/mcp/coord/info', (_req, res) => {
    res.json({ ok: true, server: SERVER_INFO, protocolVersion: PROTOCOL_VERSION, tools: TOOLS.map(t => t.name) })
  })
}

// Test seam. callTool is the single place the MCP route reaches a handler, and
// the 2026-09-09 heartbeat touch lives inside it, so a test that exercised the
// handler directly would prove nothing about the wire. Exported so
// coord-worker-liveness-flag.test.js can drive the REAL path.
module.exports = { mount, TOOLS, SERVER_INFO, PROTOCOL_VERSION, _callToolForTest: callTool }
