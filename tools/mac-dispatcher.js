// mac-dispatcher.js - Mac-native cowork.dispatch_worker.
//
// Authored 2026-06-08 (Mac-mini day-1). The Corazon-era cowork.js dispatch
// chain depends on AutoHotkey + Win32 WinActivate/SendInput/GetForegroundWindow
// + ahk_exe Code.exe matching, none of which exist on macOS. Rather than
// branch every line in cowork.js, this module re-implements ONLY the
// dispatch_worker function for darwin and re-exports kill_worker +
// cleanup_orphan_workers from cowork.js (they use cross-platform
// ide.tabs / ide.tabs_close - already platform-agnostic).
//
// Critical architectural shift on Mac: ide.chat_send_message accepts
// {submit: true} which runs the open + populate + submit chain entirely
// inside the extension host. NO OS keystroke, NO focus dependency, NO AHK.
// The bridge's claude-vscode.editor.open does the same work the Windows path
// did across the 1500-line cowork.js dispatch_worker, in one extension-host
// tick.
//
// This module wires into scheduler.js via the existing _setDispatcher seam.
// index.js calls scheduler._setDispatcher(macDispatcher) when process.platform
// === 'darwin'. cowork.js stays untouched (except for the COORD_ROOT env-var
// fix) so the Windows path remains intact for Corazon.

'use strict'

const fs = require('fs')
const os = require('os')
const path = require('path')
const crypto = require('crypto')

const ide = require('./ide')
const applescript = require('./applescript')
const cowork = require('./cowork')  // for kill_worker, cleanup_orphan_workers, helpers

// Match cowork.js path resolution exactly (env var > platform default).
const COORD_ROOT = process.env.COORD_ROOT
  || path.join(os.homedir(), '.ecodiaos', 'coordination')
const BRIEFS_DIR = path.join(COORD_ROOT, 'briefs')
const STATE_DIR = path.join(COORD_ROOT, 'state')
const WORKERS_DIR = path.join(COORD_ROOT, 'workers')
const MESSAGES_DIR = path.join(COORD_ROOT, 'messages')

const BRIEF_INLINE_CAP_BYTES = 100 * 1024
const DEFAULT_WORKER_ACK_TIMEOUT_MS = 180000
const WORKER_ACK_POLL_INTERVAL_MS = 2000

function ensureDirs() {
  for (const d of [COORD_ROOT, BRIEFS_DIR, STATE_DIR, WORKERS_DIR, MESSAGES_DIR]) {
    try { fs.mkdirSync(d, { recursive: true }) } catch (e) {}
  }
}

function uuid() { return crypto.randomUUID() }
async function sleep(ms) { return new Promise(r => setTimeout(r, ms)) }

function loadLaptopAgentToken() {
  try {
    return fs.readFileSync(path.join(os.homedir(), '.ecodiaos', 'laptop-agent.token'), 'utf8').trim()
  } catch (e) { return '' }
}

// HTTP POST (no external deps - mirrors cowork.js postJson).
function postJson(urlStr, body, bearerToken) {
  return new Promise((resolve, reject) => {
    const u = require('url').parse(urlStr)
    const httpMod = u.protocol === 'https:' ? require('https') : require('http')
    const payload = JSON.stringify(body)
    const headers = {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(payload),
    }
    if (bearerToken) headers['Authorization'] = 'Bearer ' + bearerToken
    const req = httpMod.request({
      hostname: u.hostname, port: u.port, path: u.path, method: 'POST', headers, timeout: 5000,
    }, res => {
      let chunks = ''
      res.on('data', c => chunks += c)
      res.on('end', () => {
        try { resolve({ status: res.statusCode, body: JSON.parse(chunks) }) }
        catch (e) { resolve({ status: res.statusCode, body: chunks, parse_error: e.message }) }
      })
    })
    req.on('error', reject)
    req.on('timeout', () => { req.destroy(); reject(new Error('register-worker timed out')) })
    req.write(payload)
    req.end()
  })
}

// composeBrief - the dispatcher's identity + plumbing wrapper around the cron
// prompt body. The cron prompt is itself a fully-formed agentic brief per
// patterns/cron-worker-prompt-template.md (HEADER + CONTEXT + OBJECTIVE +
// AGENCY + HARD CONSTRAINTS + DELIVERABLE + QUALITY BAR). This wrapper must
// NOT contradict the cron's AGENCY section. The old wrapper had
// "Do not orchestrate. Do not spawn workers" which directly contradicted the
// cron template's "You may schedule_delayed in 0m to spawn an immediate
// sibling worker." Workers read both and followed the more restrictive line.
//
// This rewrite keeps ONLY plumbing the worker can't infer from the cron body:
// - Worker identity (tab_id, task_id, tab_credential)
// - MCP coord calling convention (identity-in-args)
// - First action: verify_paste against the audit file (recovery)
// - Closing actions: signal_done + close_my_tab
// Heartbeat is auto-fired by the coord MCP server on any tool call, no
// explicit instruction needed. Everything else is the cron's responsibility.
function composeBrief(opts) {
  const {
    tab_id, task_id, tab_credential, parent_conductor_tab_id, parent_session,
    brief_body, brief_size_bytes, brief_storage, brief_file_path,
    report_back, inbox_topic,
  } = opts

  const headerAttrs = [
    'role="worker"',
    'tab_id="' + tab_id + '"',
    'task_id="' + task_id + '"',
    'tab_credential="' + tab_credential + '"',
    // 2026-08-29. This attribute used to be built by hand as
    // 'chat.' + tab_id + '.inbox'. Once a worker carries a lane the resolver
    // serves it a LANE mailbox instead, and a hand-built attribute would have
    // told the worker one address while the substrate used another: the
    // caller/callee disagreement already found once in routes/comms.js, and
    // worse than either address alone because the brief is the document a
    // worker is instructed to trust over its own chat. inbox_topic is whatever
    // /api/comms/register-worker just answered, which IS the resolver's output.
    // The fallback keeps every pre-lane dispatch byte-identical.
    'inbox="' + (inbox_topic || ('chat.' + tab_id + '.inbox')) + '"',
    // The address a worker copies when it wants to reach "the conductor". It MUST
    // be its own dispatching chat, not the shared `chat.conductor.inbox` slot:
    // that slot is rewritten by every chat's turn-start heartbeat, so a worker
    // that trusted this attribute posted into whichever chat Tate had most
    // recently typed in (proven misroute 2026-08-28, coexist lane Q1 into an
    // unrelated Common Ground chat). coord.message_chat rewrites `conductor` for
    // a worker sender as a belt; this attribute is the braces, so the brief the
    // worker reads is not itself lying.
    'conductor="' + (parent_session ? ('chat.session:' + parent_session + '.inbox') : 'chat.conductor.inbox') + '"',
    'parent_conductor_tab_id="' + (parent_conductor_tab_id || 'unknown') + '"',
    'brief_storage="' + brief_storage + '"',
    'registered="conductor-side"',
  ]
  if (brief_storage === 'file') headerAttrs.push('brief_file="' + brief_file_path + '"')
  const header = '<dispatched ' + headerAttrs.join(' ') + '/>'

  const identity =
    'YOU ARE A DISPATCHED WORKER. Your identity for the coord substrate:\n' +
    '  tab_id: ' + tab_id + '\n' +
    '  task_id: ' + task_id + '\n' +
    '  tab_credential: ' + tab_credential + '\n' +
    'Registration is already done. Do NOT run any curl bootstrap.\n' +
    '\n' +
    'MCP coord calling convention:\n' +
    'The coord MCP connector is workspace-wide so it cannot auto-detect which tab is calling.\n' +
    'Include tab_id + tab_credential as ARGUMENTS in every coord.* call:\n' +
    '  mcp__coord__coord_signal_done({tab_id:"' + tab_id + '", tab_credential:"' + tab_credential + '", task_id:"' + task_id + '", result_summary:"...", status:"success", terminate:true})\n' +
    '  mcp__coord__coord_close_my_tab({tab_id:"' + tab_id + '", tab_credential:"' + tab_credential + '"})\n' +
    'Direct HTTP to localhost:7456 needs the agent bearer (workers do not have it). Use MCP.\n'

  const verifyFirst =
    'FIRST ACTION (recommended, before task work):\n' +
    '  mcp__coord__coord_verify_paste({tab_id:"' + tab_id + '", tab_credential:"' + tab_credential + '", task_id:"' + task_id + '"})\n' +
    'Returns the canonical brief from the dispatcher audit file. If the brief pasted below\n' +
    'looks truncated or corrupted, trust verify_paste.brief_body over the in-chat copy.\n'

  // 2026-07-18. Plumbing, not policy: this is the coord protocol for being
  // recalled, which a worker cannot infer from the cron body. Before this, a
  // dispatched worker never read its inbox mid-run, so a conductor stand_down
  // sat unread until the worker finished anyway and Tate closed the tab by hand.
  // It lives in composeBrief so EVERY dispatch inherits it by construction; a
  // clause each brief author has to remember is a clause that is missing exactly
  // when the fleet is busiest and a stand-down matters most.
  // Doctrine: [[worker-interruptibility-soft-poll-hard-kill-2026-07-18]].
  const standDown =
    'STAND-DOWN CHECK (do this if your task runs longer than a few minutes):\n' +
    'You can be recalled mid-run. Nothing interrupts you, so you must look:\n' +
    '  mcp__coord__coord_peek_inbox({tab_id:"' + tab_id + '", tab_credential:"' + tab_credential + '"})\n' +
    'Peek at natural checkpoints - between phases, before any long or hard-to-undo\n' +
    'step (a push, a deploy, a migration, an outbound send), and after any step that\n' +
    'took a while. It is a cheap non-consuming read; it does not eat your messages.\n' +
    'If any message body has type "stand_down", STOP. Do not finish the current\n' +
    'thing first, do not start anything new. Leave the tree in a safe state\n' +
    '(no half-written file, nothing staged mid-push), then:\n' +
    '  1. mcp__coord__coord_signal_done({tab_id:"' + tab_id + '", tab_credential:"' + tab_credential + '", task_id:"' + task_id + '", status:"stood_down", result_summary:"stood down at <checkpoint>: <what is done, what is not, what the next worker needs>", terminate:true})\n' +
    '  2. mcp__coord__coord_close_my_tab({tab_id:"' + tab_id + '", tab_credential:"' + tab_credential + '"})\n' +
    'status:"stood_down" is required and is NOT a failure - it is terminal and\n' +
    'stops this task being retried or re-dispatched. Reporting "failed" instead\n' +
    'burns a retry and brings you straight back. Your result_summary is the whole\n' +
    'handover, so say what you actually finished and what you did not.\n' +
    'A stand_down is the conductor reclaiming the lane. It is not a question.\n' +
    'Ignore it and the conductor will close this tab under you (coord.kill_worker),\n' +
    'and then nobody learns what you had done.\n'

  // SEND ORIGIN (lane E2, 2026-09-01). The outbound ledger records who
  // initiated a message, and it defaults to "unknown" unless the caller passes
  // it. That default decides real behaviour: a cron- or worker-originated cold
  // message to a client is STAGED AS A DRAFT rather than sent, so the person
  // gets one coherent message from the daily comms pass instead of several
  // fragments, and "unknown" opts out of that batching.
  //
  // The obvious fix, setting ECODIAOS_SEND_ORIGIN in the env this dispatcher
  // hands the worker, DOES NOT EXIST TO BE SET. This dispatcher never spawns a
  // child process. It drives the IDE bridge to open a Claude Code tab and then
  // sends a Return keystroke, so the tab inherits the IDE's environment and
  // nothing here can reach it. resolveOrigin reads process.env in whichever
  // process evaluates it, which is the stdio MCP server (inheriting the IDE env
  // on this Mac, shared with the conductor's own tabs, where a hardcoded "cron"
  // would be a lie) or ecodia-api on the VPS.
  //
  // So the attribution has to travel in the brief, which is the one channel
  // that reaches a dispatched tab. Lane C1 rewrote 18 cron prompts by hand to
  // say this; putting it here makes it true of EVERY dispatch by construction,
  // including the ones nobody remembers to edit.
  const originBlock =
    'SEND ORIGIN (applies to every outbound message you send this run):\n' +
    'You are a DISPATCHED WORKER, not a person at a keyboard. Pass origin:"cron"\n' +
    'on every gmail_send / gmail_reply call. It is not decoration: it is what\n' +
    'routes a cold message to a client into the daily comms pass as a draft\n' +
    'instead of firing a fragment at them now, and it is what the outbound\n' +
    'ledger records. Omit it and it records "unknown" and the batching is off.\n' +
    'If a human decision is genuinely behind the send, pass batch:false and say\n' +
    'why in your result_summary.\n\n'

  const taskBlock = originBlock + (brief_storage === 'file'
    ? 'YOUR TASK (full brief at: ' + brief_file_path + ' - read in full, then execute):\n'
    : 'YOUR TASK:\n' + brief_body + '\n')

  // Plumbing-only closing actions. Agency, scope, scheduling permissions all
  // come from the cron's AGENCY block above. The dispatcher wrapper imposes
  // NO restrictions on what the worker can do - the cron prompt's HARD
  // CONSTRAINTS section is the only authoritative restriction layer.
  // 2026-08-13 (report-back v2). signal_done is a MACHINE signal: it lands in
  // chat.conductor.inbox and NEVER injects a turn (the load-bearing split in
  // [[coord-chat-to-chat-push-delivery-2026-08-02]]). So a chat that scheduled
  // this worker and is idle-waiting is never woken by signal_done alone - it sits
  // waiting forever while the worker acks, finishes, and closes its own tab. The
  // fix is a PUSH: coord.message_chat injects a completion turn straight into the
  // conductor chat, waking it. This is now DEFAULT-ON for interactive schedule_delayed
  // rows (scheduler.js ensureReportBackDefault), and report_back is resolved to the
  // `conductor` keyword above - the one target coord.js resolves reliably (stored
  // label/fingerprint) and fails SAFE to inbox on. Standing crons opt out (or use
  // schedule_cron, which never defaults on), so their completions stay inbox-only and
  // consumed by execute-top / turn-hooks. Like the stand-down block, it lives in
  // composeBrief so the HOW is generated by construction; a brief author supplies only
  // the one address, never the tool-call shape. Origin: 2026-08-13 Tate - a dispatched
  // helper finished a slice of a chat's todo list, closed its tab, and left the
  // originating chat waiting because nothing reported back.
  // 2026-08-13 per-tab identity v3 (re-enabled after the safety lock). report_back
  // is now a `session:<id>` address that resolves to the SPECIFIC originating chat
  // via the chat-tabs registry (conductor_heartbeat anchors each chat by its stable
  // Claude Code session_id on its own genuine user turns). resolveLiveTargetTab
  // resolves it by a UNIQUE exact-label match among live non-worker tabs, or FAILS
  // SAFE to the inbox - so a drifted label or a collision queues rather than
  // misroutes. There is no shared slot for another session to overwrite, which is
  // what caused the earlier misroute. injectTurn holds a cross-process focus lock
  // and focuses the resolved tab by its CURRENT position before pasting. Doctrine:
  // coord-conductor-addressing-per-tab-identity-2026-08-13.
  const reportBackStep = report_back
    ? '1. mcp__coord__coord_message_chat({tab_id:"' + tab_id + '", tab_credential:"' + tab_credential + '", to:"' + report_back + '", from_label:"worker ' + task_id + '", text:"[worker done: ' + task_id + '] <one-line result grounded in a probe, + result_pointer to the durable substrate>"})\n' +
      '   This wakes the chat that scheduled you (its stable session address). Send it BEFORE signal_done, while your result is fresh. If message_chat returns delivery.ok:false it stayed inbox-queued (that chat was not uniquely resolvable right now) - that is fine, signal_done below is the durable record.\n'
    : ''
  const n = reportBackStep ? { done: 2, close: 3 } : { done: 1, close: 2 }
  const closing =
    'CLOSING (mandatory at exit):\n' +
    'TEARDOWN FIRST - close what you opened. Before signal_done, kill any dev server\n' +
    'YOU started (kill -9 the pids), tear down any headless/CDP browser context you\n' +
    'launched (NEVER the canonical Chrome singleton on :9222), and shut down any\n' +
    'simulator you booted (xcrun simctl shutdown all). A worker that leaks these to\n' +
    'launchd risks OOMing the Mac, which crashes the Claude Code tabs and kills the\n' +
    'scheduler. The worker-teardown-hygiene-gate BLOCKS signal_done once if you still\n' +
    'own a running dev server or booted sim; the mac-resource-guard reaper is the\n' +
    'backstop. Doctrine: worker-teardown-discipline-close-what-you-opened-2026-08-16.\n' +
    reportBackStep +
    n.done + '. mcp__coord__coord_signal_done({tab_id:"' + tab_id + '", tab_credential:"' + tab_credential + '", task_id:"' + task_id + '", status:"success"|"failed", result_summary:"...", terminate:true})\n' +
    n.close + '. mcp__coord__coord_close_my_tab({tab_id:"' + tab_id + '", tab_credential:"' + tab_credential + '"})\n' +
    'Without close_my_tab as your final tool call, every worker tab accumulates in the IDE.\n'

  // 2026-08-18 (self-continuation, Tate mandate). The terminal state of a worker
  // is NOT "done". A worker that finishes step one of a multi-step project, or ships
  // a one-step job it never independently proved, and then closes, is the turn-death
  // that kills a project during the away window: nothing reprompts it. So EVERY
  // dispatch now carries the decision by construction (same rationale as standDown:
  // a clause each author must remember is a clause missing exactly when the fleet is
  // busiest). A worker ends in one of two states: a VERIFIED CLOSE (whole scope done
  // AND independently probed this run) or an ARMED SUCCESSOR chat (the next step, or,
  // the default even for one-step work, an independent verification pass). The successor
  // brief carries this clause verbatim so thoroughness compounds across the chain instead
  // of decaying to the minimum. Workers MAY schedule a SINGLE successor of their own work
  // line (this is not the banned parallel fan-out; it is the one successor Tate explicitly
  // blessed 2026-08-18). Enforced by successor-arm-gate.py.
  // Doctrine: [[self-continuation-is-the-default-terminal-state-2026-08-18]].
  const selfContinuation =
    'SELF-CONTINUATION (decide your terminal state BEFORE you signal_done):\n' +
    'Your run does not end in "done". It ends in one of two states, and closing\n' +
    'without choosing is the exact failure this clause exists to stop:\n' +
    '  (A) VERIFIED CLOSE. The WHOLE scope is complete AND you independently proved it\n' +
    '      on the real deployed surface THIS run with a discriminating probe (the\n' +
    '      authed-role query, a CDP shot vs the live url, a test that exercises the real\n' +
    '      path, a build/deploy id). Only then put the token verified-close in your\n' +
    '      signal_done result_summary, together with WHAT the probe was and what it\n' +
    '      returned. Then close.\n' +
    '  (B) ARMED SUCCESSOR. Otherwise arm the next chat FIRST, then close. Two kinds:\n' +
    '      1. CONTINUATION: more steps remain in this project. Arm the next step.\n' +
    '      2. VERIFICATION (the DEFAULT, even for a one-step job): the work looks done\n' +
    '         but you have not independently proved it, or a fresh pair of eyes would\n' +
    '         raise the bar. Arm a chat that re-derives whether the work is real and\n' +
    '         excellent, drives the live surface, hunts the failure modes you were too\n' +
    '         close to see, then confirms (verified-close) or corrects and re-verifies.\n' +
    'Arm a successor via the scheduler with a FULL self-contained brief (assume no\n' +
    'context carries over). If the scheduler tool is deferred, load it first:\n' +
    '  ToolSearch("select:mcp__ecodia-scheduler__schedule_delayed")\n' +
    '  mcp__ecodia-scheduler__schedule_delayed({\n' +
    '    name: "cowork.<lowercase-project>-lane-<Letter><digit>-<next-or-verify>",\n' +
    '          THE NAME SHAPE IS LOAD-BEARING, not cosmetic. Only a name matching\n' +
    '            cowork.<lowercase-project>-lane-<Letter><digit>[-<free suffix>]\n' +
    '          (lane ids S0, A3, W2; the project slug must be lowercase; the separator\n' +
    '          is \"-lane-\" WITH the trailing hyphen) is seen by the migration-147\n' +
    '          trigger trg_os_scheduled_tasks_one_live_row_per_lane, which cancels any\n' +
    '          EARLIER pending row holding the same lane. Any other name parses to NULL\n' +
    '          and gets NO collision guard, so a second successor armed for the same job\n' +
    '          opens a second worker tab against the same repo and both do the work.\n' +
    '          Carry forward the lane id of the row that armed you; vary only the suffix.\n' +
    '    delay: "in 1m",\n' +
    '    prompt: "<FULL brief: what you shipped + the exact commit or url, what is still\n' +
    '             unproven, the exact verify gate or next step, recipe/pattern path and\n' +
    '             any bearer, AND this SELF-CONTINUATION clause verbatim>"\n' +
    '  })\n' +
    'The successor brief MUST carry this clause verbatim; that recursion is what makes\n' +
    'thoroughness compound instead of decaying to the minimum. Releasing work without an\n' +
    'independent second pass IS the minimum. A verification chat that independently\n' +
    'confirms the work is solid closes with verified-close and does NOT chain another\n' +
    'verifier: that is where the chain naturally ends. You MAY schedule this one successor\n' +
    'of your own work line (it is not the banned parallel fan-out). The only clean close\n' +
    'with neither a successor nor verified-close is a genuine external block (vendor\n' +
    'outage, sealed credential, a Tate-gated step): name it in result_summary, and where\n' +
    'the block has a known reset, arm a re-check successor at that time.\n'


  // 2026-08-29. THE FIRST CALLER OF coord.wait_for_inbox, which shipped with a
  // 600s long-poll and zero callers, so a worker's only way to ask anything was
  // to post and end its turn. That is not a question, it is a note left for
  // nobody: this very conductor woke to three worker gate-questions already
  // dead in its inbox, each from a tab that had finished and closed.
  //
  // Two facts make holding correct NOW and not before. (1) message_chat returns
  // ok:true with delivered:false when the parent cannot be injected into, so a
  // worker that checks `ok` believes it was heard when it was merely queued
  // (measured: five verify passes each escalating into a queue nobody drained).
  // (2) the mailbox is lane-keyed as of this same commit, so an answer arriving
  // after this tab dies is still READ by the next pass of this job. Before that,
  // holding was the only option because the answer was otherwise lost forever.
  const gateQuestion =
    'IF YOU HIT A GENUINELY BLOCKING QUESTION (do not end your turn on one):\n' +
    'A question you post and then walk away from is a note left for nobody. The\n' +
    'answer lands in a mailbox after your tab is gone. Ask, then HOLD:\n' +
    '  1. mcp__coord__coord_message_chat({tab_id:"' + tab_id + '", tab_credential:"' + tab_credential + '", to:"' + (parent_session ? ('chat.session:' + parent_session + '.inbox') : 'conductor') + '", text:"<the question, the options you see, and your recommendation>"})\n' +
    '     Read `delivered` in the result, NOT `ok`. ok:true with delivered:false\n' +
    '     means it persisted to an inbox and nothing woke anyone: still worth\n' +
    '     holding for, but do not report it as asked-and-answered.\n' +
    '  2. mcp__coord__coord_wait_for_inbox({tab_id:"' + tab_id + '", tab_credential:"' + tab_credential + '", timeout:45})\n' +
    '     This BLOCKS and returns the moment a reply lands, costing no tokens while\n' +
    '     it holds. Omit `topic` - it resolves to your own durable mailbox, which is\n' +
    '     keyed to your work LANE and not to this tab.\n' +
    '     45 IS A CEILING, NOT A PREFERENCE. The server holds up to 600s but an MCP\n' +
    '     client deadline kills the call between 45s and 60s (measured 2026-08-29 by\n' +
    '     two independent callers), and past that wall you lose the tool result AND\n' +
    '     any message that arrived while you held. To wait longer, LOOP the 45s call;\n' +
    '     never raise the number.\n' +
    '  3. On timed_out:true, re-issue the 45s hold a few times if the answer is\n' +
    '     genuinely the only thing left (that loop is how you wait minutes). Otherwise\n' +
    '     do every part of the job that does NOT depend on the answer, then close with\n' +
    '     the question in your result_summary.\n' +
    'Because your mailbox is lane-keyed it OUTLIVES this tab, so a reply that\n' +
    'arrives after you close is read by the next pass of this same job. That is\n' +
    'why closing with an unanswered question is now a handover and not a loss.\n' +
    'When you answer a message rather than ask one, pass its id as in_reply_to so\n' +
    'the thread is followable.\n'

  return [header, '', identity, '', verifyFirst, '', standDown, '', gateQuestion, '', taskBlock, '', selfContinuation, '', closing].join('\n')
}

// ── Stable tab id, stamped at spawn ──────────────────────────────────────────
//
// WHY THE DISPATCHER AND NOT signal_bound. coord._captureStableTabId resolves a
// worker's tab by matching the spawn sentinel against live tab LABELS, on the
// premise that bind is "the one moment the tab still shows its unique sentinel".
// That premise is false for a recurring cron. The sentinel is derived from the
// brief, a cron re-fires one brief verbatim, so when fires N-1 and N-2 are still
// open, fire N is ambiguous AT BIND TIME, captures nothing, and is itself
// unclosable. The leak is self-sustaining rather than decaying: the reaper
// drains the backlog and the source refills it.
//
// The dispatcher does not have to match anything. It knows which tab it just
// opened because it is standing on both sides of the open, so the identification
// is unambiguous BY CONSTRUCTION rather than by luck. Snapshot the set of live
// CC-chat ttab_ ids before ide.chat_send_message and again after; the single id
// that appeared is ours. It never reads a label, so no number of same-sentinel
// corpses can make it ambiguous, which is exactly the case that defeats bind.
//
// Cost measured 2026-08-29: ide.tabs() is 18ms and returned 61 of 61 CC chat
// tabs carrying ids, so the pair adds about 36ms to a dispatch.
//
// SAFE BY OMISSION. Every failure returns no id and stamps nothing, leaving the
// signal_bound capture to run exactly as it does today. This can withhold a
// stamp; it cannot produce a wrong one, and a wrong one closes a live worker.
// The id space is coord's own (_liveCcTabsWithIds), so capture and close still
// speak one coordinate system.

const _CC_CHAT_VIEW_TYPE = 'mainThreadWebview-claudeVSCodePanel'

// Live CC-chat tabs carrying a bridge-minted ttab_ id. Delegates to coord so the
// dispatcher and the close path cannot drift into two different id spaces.
// Returns null (not []) when the bridge is unreachable, because "no snapshot"
// and "no tabs" must not collapse into the same value: differencing against an
// empty set would call every live tab new.
const _SPAWN_DIFF_SNAPSHOT_TIMEOUT_MS =
  Number(process.env.DISPATCH_SPAWN_DIFF_TIMEOUT_MS || 2500)

async function _ccTabIdSnapshot() {
  try {
    const coord = require('./coord')
    if (typeof coord._liveCcTabsWithIds !== 'function') return null
    // TIME-BOXED, and this is not theoretical. Probed live 2026-08-29 the bridge
    // answered in 18ms on one call and 3844ms on another, same 58 to 61 tabs, so
    // the variance is contention and not size. Two unbounded calls would put up
    // to 7.7s of someone else's load on the dispatch path, and a dispatch that
    // stalls is worse than a tab that leaks. Losing the race yields null, which
    // is already the refuse-and-keep-the-ladder branch.
    const timeout = new Promise((resolve) =>
      setTimeout(() => resolve(null), _SPAWN_DIFF_SNAPSHOT_TIMEOUT_MS))
    const tabs = await Promise.race([coord._liveCcTabsWithIds(undefined), timeout])
    if (!Array.isArray(tabs)) return null
    return tabs.filter((t) => t && t.tabId)
  } catch (e) {
    return null
  }
}

// The set difference. Exactly one new id is the answer; anything else refuses.
//
// fresh > 1 means something else opened a chat inside our window. dispatchOne is
// serial under the launch lock, so the realistic author is a human opening a tab
// mid-dispatch. Narrowing by our own sentinel is a tiebreak ONLY, never the
// primary key, so a same-sentinel corpse cannot re-enter the decision: a corpse
// is not new, so it was already excluded by the id diff before any label is read.
//
// fresh === 0 does not prove no tab was created. assignStableTabIds reconciles a
// tab to a prior id by (viewColumn, index), so a tab closing and ours opening at
// the same slot inside our window would inherit the departed id. Rare, and the
// consequence of treating it as "no tab" would be failing a dispatch that in fact
// succeeded, so it declines to stamp and says why rather than raising an error.
function _diffSpawnedCcTab(before, after, sentinel_prefix) {
  if (!Array.isArray(before) || !Array.isArray(after)) {
    return { ok: false, reason: 'no_snapshot' }
  }
  // EXACTLY ONE, or nothing. Confirmed with lane W1 2026-08-29: their orphan
  // sweep Pass 0 has no fallthrough, so a row carrying a stored id that does not
  // resolve is refused and leaked rather than handed back to the ladder. Before
  // this stamp existed only 6 of 111 terminated rows carried an id and a bad one
  // cost a single ghost. Once every new row carries one, a WRONG stamp is a
  // permanently unreapable tab, which is the failure we are both here to kill.
  // So every uncertain branch below leaves tabId unset and keeps the ladder.
  const beforeIds = new Set(before.map((t) => t.tabId))
  const fresh = after.filter((t) => !beforeIds.has(t.tabId))
  if (fresh.length === 1) return { ok: true, tab: fresh[0], via: 'dispatch_spawn_diff' }
  if (fresh.length === 0) {
    return { ok: false, reason: 'no_new_tab_id', before: before.length, after: after.length }
  }
  let narrowed = []
  try {
    const coord = require('./coord')
    if (sentinel_prefix && typeof coord._labelMatchesStored === 'function') {
      narrowed = fresh.filter((t) => coord._labelMatchesStored(t.label, sentinel_prefix))
    }
  } catch (e) { narrowed = [] }
  if (narrowed.length === 1) return { ok: true, tab: narrowed[0], via: 'dispatch_spawn_diff_sentinel' }
  return { ok: false, reason: 'ambiguous_new_tabs', fresh: fresh.length, narrowed: narrowed.length }
}

async function dispatch_worker(params) {
  params = params || {}
  const account = params.account || 'current'
  const brief_body = params.brief || ''
  const task_id = params.task_id || uuid()
  let parent_conductor_tab_id = params.parent_conductor_tab_id || null
  if (!parent_conductor_tab_id) {
    try {
      const coord = require('./coord')
      const reg = coord._loadConductorRegistration && coord._loadConductorRegistration()
      if (reg && reg.tab_id) parent_conductor_tab_id = reg.tab_id
    } catch (e) {}
  }
  const coord_url = params.coord_url || 'http://localhost:7456'
  // Raw scheduled-row name (e.g. cowork.daycrew-lane-S2-deploy-verify).
  // Forwarded untouched: laneKeyOf in coord.js is the ONE place that decides
  // what a lane is, and it is a character-exact twin of the Postgres
  // os_sched_lane_key the scheduler's own trigger uses.
  const lane_name = params.lane_name || null

  // 2026-08-13 (report-back v2). Resolve the report-back address. A dispatching
  // chat asks the worker to push a completion turn back to it - the wake signal_done
  // cannot give - either programmatically (params.report_back) or, more commonly, by
  // a `REPORT-BACK: <addr>` directive line anywhere in the brief it authored. The
  // directive is parsed HERE (not in composeBrief) because in file-storage mode
  // composeBrief never sees the brief body.
  //
  // The ONLY safe+reliable wake target is the `conductor` keyword. coord.js resolves
  // it by stored-label/fingerprint and FAILS SAFE to the conductor inbox, never
  // guessing a foreground chat. Freezing origin/parent/self to a `chat.label:<slug>`
  // from a stale title_match risked the exact misroute this ends (2026-08-13
  // incident), so all of origin|parent|self|conductor collapse to the literal string
  // "conductor" (composeBrief emits it as `to:"conductor"`). `none` is an explicit
  // opt-out -> no report-back push (signal_done stays the durable record). Anything
  // else (an author-chosen tab_id or `chat.<...>.inbox`) passes through verbatim.
  let report_back = params.report_back || null
  if (!report_back) {
    const m = brief_body.match(/^\s*REPORT-BACK:\s*(.+?)\s*$/mi)
    if (m) report_back = m[1].trim()
  }
  // Captured BEFORE the branch below, which nulls a bare `origin|parent|self|
  // conductor` directive. Only a directive that NAMED a session counts as an
  // explicit parent; nothing here may infer one from which chat was recently
  // active. Doctrine: conductor-is-a-slot-not-an-identity-2026-08-28.
  const rbExplicitSession = (report_back && /^\s*session:/i.test(String(report_back)))
    ? String(report_back).trim().slice('session:'.length)
    : null
  if (report_back) {
    const rb = String(report_back).trim()
    if (/^none$/i.test(rb)) {
      report_back = null
    } else if (/^session:/i.test(rb)) {
      // Explicit specific-chat target (v3, the reliable path): the dispatching chat
      // named its own stable session_id. Pass through verbatim.
      report_back = rb
    } else if (/^(origin|parent|self|conductor)$/i.test(rb)) {
      // A BARE `origin|parent|self|conductor` names no chat, and there is no
      // honest way to infer one here.
      //
      // This used to resolve through _recentActiveSession, on the reasoning that
      // the chat the user most recently typed in is the dispatcher. That is true
      // only when a chat arms a row and it fires immediately. For a
      // scheduler-leased row it is false, and the failure is not theoretical:
      // on 2026-08-28 worker 790d5247 (current-cause lane M1 verify) was
      // dispatched at 11:43 from a row armed earlier, the recently-active chat
      // at that moment was "Open my travel itinerary", and the worker's entire
      // verified-close was addressed to that unrelated chat. Same defect class
      // as the `conductor` slot itself: an address inferred from "who was
      // active" rather than bound to who actually dispatched.
      //
      // So a bare directive now DISABLES the report-back push. signal_done
      // remains the durable record and the conductor-inbox peek still surfaces
      // it. An author who wants the push writes `REPORT-BACK: session:<id>`
      // with their own session id, which the dispatcher passes through verbatim
      // and which cannot drift.
      // Doctrine: conductor-is-a-slot-not-an-identity-2026-08-28.
      report_back = null
    } else {
      report_back = rb
    }
  }

  // parent_session (2026-08-28): the stable session_id of the chat DISPATCHING
  // this worker, recorded on the worker row so a later ad-hoc `to:"conductor"`
  // send resolves to that chat instead of the shared singleton slot that every
  // chat's turn-start heartbeat overwrites.
  //
  // EXPLICIT SOURCES ONLY: a caller-passed session, or a REPORT-BACK directive
  // that NAMED one. The tempting fallback - "the most-recently-active anchored
  // chat is the dispatcher" - is false for the population that dominates this
  // dispatcher: a scheduler-leased row was armed minutes or hours earlier, so
  // the recently-active chat at LEASE time is whoever is typing now. Adopting it
  // would reintroduce the exact defect this field closes, one layer down. No
  // explicit parent means null, and null means the worker's `conductor` sends
  // QUEUE to the durable inbox. Late and correct beats prompt and wrong.
  // Doctrine: conductor-is-a-slot-not-an-identity-2026-08-28.
  const parent_session = params.parent_session || rbExplicitSession || null

  if (!brief_body) throw new Error('brief required')
  ensureDirs()

  const tab_id = 'tab_' + Date.now() + '_' + crypto.randomBytes(4).toString('hex')
  const tab_credential = uuid()

  // Sentinel-prefix label pinning - same shape as cowork.js so kill_worker /
  // close_my_tab tab-resolution code works identically.
  const sentinel_inner = (params.worker_name && /^[A-Za-z0-9_\- ]{1,40}$/.test(String(params.worker_name)))
    ? String(params.worker_name)
    : ('EOS-W-' + tab_id.slice(-8))
  const sentinel_prefix = '[' + sentinel_inner + ']'
  const briefed_body = sentinel_prefix + ' ' + brief_body

  // 2026-06-22: auto-title fingerprint so close_my_tab / kill_worker can find
  // this tab after Claude Code summarises the brief into the title and strips
  // the sentinel. Uniqueness-gated at match time (better leak than wrong-close).
  // Doctrine: cc-auto-title-summarizer-strips-eos-w-sentinel-tabs-leak-2026-06-08.
  let autotitle_fingerprint = null
  try { autotitle_fingerprint = require('./tab-title-match').computeFingerprint(brief_body) } catch (_e) {}

  const brief_size_bytes = Buffer.byteLength(briefed_body, 'utf8')
  let brief_storage = 'inline'
  let brief_file_path = null
  if (brief_size_bytes > BRIEF_INLINE_CAP_BYTES) {
    brief_storage = 'file'
    brief_file_path = path.join(BRIEFS_DIR, task_id + '.md')
  }

  // Always write audit copy for verify_paste recovery.
  const auditFilePath = path.join(BRIEFS_DIR, task_id + '.md')
  try { fs.writeFileSync(auditFilePath, briefed_body, 'utf8') } catch (e) {}

  // Resolve current account label.
  let resolved_account = account
  if (account === 'current') {
    try {
      const usage = require('./usage')
      resolved_account = usage._getActiveAccount()
    } catch (e) {
      resolved_account = 'current-process'
    }
  }
  const account_active_when_spawned = resolved_account

  // Synchronous worker registration (conductor-side).
  let register_result = null
  try {
    register_result = await postJson(
      coord_url + '/api/comms/register-worker',
      {
        tab_id, task_id, tab_credential,
        parent_conductor_tab_id,
        parent_session,
        account_active_when_spawned,
        // The os_scheduled_tasks row name this worker came from. The registry
        // derives a lane-keyed mailbox from it that outlives this tab, so the
        // next pass of the same job inherits the inbox instead of starting deaf.
        lane_name: lane_name,
      },
      loadLaptopAgentToken()
    )
    if (register_result.status !== 200) {
      return {
        ok: false, tab_id, task_id,
        error: 'register-worker rejected: ' + register_result.status,
        register_body: register_result.body,
      }
    }
  } catch (e) {
    return { ok: false, tab_id, task_id, error: 'register-worker failed: ' + e.message }
  }

  // Compose brief.
  // The address the REGISTRY resolved for this worker, taken from the
  // registration response rather than re-derived here. Re-deriving would put a
  // second implementation of lane resolution in the dispatcher, and the whole
  // failure this fixes is two places disagreeing about one address.
  const resolved_inbox = (register_result && register_result.body && register_result.body.inbox) || null

  const composedBriefInner = composeBrief({
    tab_id, task_id, tab_credential, parent_conductor_tab_id, parent_session,
    brief_body: brief_storage === 'inline' ? brief_body : '',
    brief_size_bytes,
    brief_storage,
    brief_file_path: brief_storage === 'file' ? brief_file_path : null,
    report_back,
    inbox_topic: resolved_inbox,
  })
  const composedBrief = sentinel_prefix + '\n' + composedBriefInner

  // THE LOAD-BEARING MAC SHIFT: ide.chat_send_message opens + populates the
  // chat input in extension-host space (no AHK, no focus race for that part).
  // BUT submit is harder: workbench.action.chat.submit targets VS Code's
  // built-in Copilot chat surface, not the CC webview, and the extension host
  // cannot dispatch DOM events into another extension's webview iframe. So
  // the bridge leaves submit_command_ok: false and the caller (this dispatcher)
  // has to send one Enter via System Events. Mirror cowork.js's
  // window.focus_and_send AHK chain with applescript.activate_app +
  // applescript.keystroke. Apple Events activate is the gentlest available
  // path - doesn't steal keyboard focus the way Win32 WinActivate does, but
  // does ensure VS Code is the keystroke target.
  let tab_handle = null
  let spawn_error = null
  let submit_path = null
  // Snapshot the live CC-chat id set immediately before the open. Taken INSIDE
  // the try so a bridge outage here fails the dispatch the same way the open
  // itself would, rather than half-arming the diff.
  const ccTabsBefore = await _ccTabIdSnapshot()
  try {
    const sendRes = await ide.chat_send_message({ prompt: composedBrief, submit: false })
    const inner = (sendRes && (sendRes.result || sendRes)) || {}
    if (inner.open_command_ok === false) {
      spawn_error = 'editor.open failed: ' + (inner.open_error || 'unknown')
    }
    const ot = inner.opened_tab
    if (ot && ot.viewColumn != null) {
      tab_handle = {
        sentinel_prefix,
        viewColumn: ot.viewColumn,
        viewType: ot.viewType || 'mainThreadWebview-claudeVSCodePanel',
        label_at_spawn: ot.label,
        tabIndex: (typeof ot.index === 'number') ? ot.index : null,
        autotitle_fingerprint,
        captured_via: 'bridge_chat_send_message',
        captured_label_is_provisional: true,
      }
    }
    // Stamp the stable tab id from the spawn diff. This is the identification
    // that a recurring cron cannot defeat, so it runs whenever a tab_handle
    // exists, including when the bridge fell back to the active tab: the id is
    // derived independently of the bridge's own label-keyed diff, and the close
    // path reads viewColumn and index off the LIVE resolved tab rather than off
    // this handle, so a correct id repairs a handle whose position is wrong.
    if (tab_handle) {
      const ccTabsAfter = await _ccTabIdSnapshot()
      const spawned = _diffSpawnedCcTab(ccTabsBefore, ccTabsAfter, sentinel_prefix)
      if (spawned.ok) {
        tab_handle.tabId = spawned.tab.tabId
        tab_handle.tabId_captured_at = new Date().toISOString()
        tab_handle.tabId_captured_label = spawned.tab.label
        tab_handle.tabId_captured_via = spawned.via
      } else {
        // No id. coord._captureStableTabId at signal_bound still runs its own
        // capture unchanged, so this is a missing improvement, not a regression.
        tab_handle.tabId_spawn_diff_declined = spawned.reason
      }
      // DIAGNOSTIC ONLY, deliberately changing no behaviour this pass. The
      // bridge's /ide/chat/send_message keys its own new-tab diff on
      // viewColumn + '|' + label, so fire N of a cron whose fire N-1 corpse is
      // still open is filtered out as already-seen and it falls through to
      // `active_fallback`, which picks whichever CC chat is ACTIVE. That is how
      // a dispatch can register a worker whose stored position points at the
      // conductor's own tab. Recording it makes the size of that population
      // measurable for the first time; correcting the submit target is a
      // separate change against the most load-bearing path in the fleet and
      // wants its own evidence before it ships.
      if (ot && ot.via) tab_handle.bridge_opened_via = ot.via
      if (spawned.ok && ot && typeof ot.viewColumn === 'number'
          && spawned.tab.viewColumn !== ot.viewColumn) {
        tab_handle.bridge_position_disagrees = 'bridge_vc=' + ot.viewColumn
          + ' spawn_diff_vc=' + spawned.tab.viewColumn
      }
    }
    // Submit step. The bridge has populated the textarea; the 1200ms settle
    // below guarantees populate has finished before the Enter lands, so a
    // single Return reliably submits the already-prefilled brief. (History:
    // this used to fire 4x Enter spaced 800ms as belt-and-suspenders against a
    // populate/submit race, but the settle already closes that race and the
    // first Enter is the only one that ever submitted - the extra 3 were
    // no-ops landing on whatever surface had focus. Cut to 1x on 2026-06-21
    // per Tate: the trailing presses were disruptive, not load-bearing.)
    // Mac mirror:
    //   1) bridge ide.command focusNthEditorGroup (by viewColumn) - already
    //      moves keyboard focus into that group from the extension host
    //   2) applescript.activate_app (Apple Events activate, no focus steal
    //      beyond bringing VS Code forward)
    //   3) 1200ms settle for populate to finish
    //   4) 1x applescript.keystroke 'return'
    if (tab_handle && !spawn_error) {
      try {
        // 1. Focus the editor group hosting the new chat tab via bridge.
        const focusGroupCmd = (vc) => {
          if (vc === 1) return 'workbench.action.focusFirstEditorGroup'
          if (vc === 2) return 'workbench.action.focusSecondEditorGroup'
          if (vc === 3) return 'workbench.action.focusThirdEditorGroup'
          if (vc === 4) return 'workbench.action.focusFourthEditorGroup'
          if (vc === 5) return 'workbench.action.focusFifthEditorGroup'
          return null
        }
        const focusCmd = focusGroupCmd(tab_handle.viewColumn)
        if (focusCmd) {
          try {
            await Promise.race([
              ide.command({ cmd: focusCmd }),
              new Promise((_, rej) => setTimeout(() => rej(new Error('focus_cmd_timeout_3s')), 3000)),
            ])
            await sleep(200)
          } catch (_e) { /* tolerate */ }
        }
        // 1b. Re-activate the worker tab by index (guards against the human
        //     switching tabs between the bridge open and this submit chain -
        //     the window is shared with Tate's live session).
        //     workbench.action.openEditorAtIndexN is 1-based over the active
        //     group; opened_tab.index is 0-based.
        if (typeof tab_handle.tabIndex === 'number' && tab_handle.tabIndex >= 0 && tab_handle.tabIndex < 40) {
          try {
            // The numbered workbench.action.openEditorAtIndex<N> only exists for
            // N=1..9, so re-asserting a worker tab past the 9th position silently
            // no-oped and the brief could submit into whatever tab was active
            // (wrong-chat bug). Use the numbered command for index<9 and the
            // GENERIC openEditorAtIndex with an index arg for index>=9 (works at
            // any position). Doctrine: coord-deliver-by-session-not-editor-index-2026-08-21.
            const idx = tab_handle.tabIndex
            const cmd = idx < 9
              ? { cmd: 'workbench.action.openEditorAtIndex' + (idx + 1) }
              : { cmd: 'workbench.action.openEditorAtIndex', args: [idx] }
            await Promise.race([
              ide.command(cmd),
              new Promise((_, rej) => setTimeout(() => rej(new Error('open_at_index_timeout_3s')), 3000)),
            ])
            await sleep(150)
          } catch (_e) { /* tolerate */ }
        }
        // 1c. Explicitly focus the Claude Code chat INPUT. Since CC extension
        //     2.1.211 (loaded at the 2026-07-17 08:17 window restart),
        //     claude-vscode.editor.open populates the input but no longer
        //     leaves keyboard focus inside the webview textarea, so a naked
        //     Return dies at the editor-group wrapper and the brief sits
        //     unsubmitted forever (zero worker sessions, tab labels showing
        //     raw brief text). claude-vscode.focus is CC's own "Focus input"
        //     command and restores the pre-2.1.211 focus state. Proven
        //     end-to-end 2026-07-17: without it 3/3 probes never submitted;
        //     with it 2/2 probes submitted and a transcript appeared in <5s.
        try {
          await Promise.race([
            ide.command({ cmd: 'claude-vscode.focus' }),
            new Promise((_, rej) => setTimeout(() => rej(new Error('cc_focus_timeout_3s')), 3000)),
          ])
        } catch (e) {
          process.stderr.write('[mac-dispatcher] claude-vscode.focus failed for ' + tab_id + ': ' + e.message + '\n')
        }
        // 2. Activate VS Code (Apple Events; gentler than Win32 WinActivate).
        await applescript.activate_app({ app: 'Visual Studio Code' })
        // 3. Settle for the bridge's editor.open + textarea-populate to finish.
        await sleep(1200)
        // 4. Single Enter. The 1200ms settle above guarantees the populated
        //    textarea is ready, so one Return submits the brief.
        try {
          const kr = await applescript.keystroke({ key: 36 })  // key code 36 = Return; passing string 'return' types the literal word
          if (kr && kr.ok === false) {
            process.stderr.write('[mac-dispatcher] submit Return keystroke not ok for ' + tab_id + ': ' + JSON.stringify(kr).slice(0, 300) + '\n')
          }
        } catch (e) {
          // tab is open + prefilled, recoverable by hand - but never swallow silently
          process.stderr.write('[mac-dispatcher] submit Return keystroke threw for ' + tab_id + ': ' + e.message + '\n')
        }
        submit_path = 'focus_group+open_at_index+cc_focus_input+activate+1200ms_settle+1x_return'
      } catch (e) {
        submit_path = 'submit_chain_threw: ' + e.message
      }
    }
  } catch (e) {
    spawn_error = e.message
  }

  if (!tab_handle) {
    try {
      if (account_active_when_spawned && account_active_when_spawned !== 'current-process') {
        const usage = require('./usage')
        usage._markFlaky(account_active_when_spawned, 'mac_dispatch_populate_failed: ' + (spawn_error || 'unknown'))
      }
    } catch (e) {}
    return {
      ok: false, tab_id,
      error: 'populate failed (editor.open): ' + (spawn_error || 'no opened_tab returned'),
      account_marked_flaky: account_active_when_spawned,
    }
  }

  // Persist tab_handle for kill_worker / close_my_tab.
  try {
    const coord = require('./coord')
    if (typeof coord.setWorkerTabHandle === 'function') {
      coord.setWorkerTabHandle(tab_id, tab_handle)
    }
  } catch (e) {}

  // Write paste-verify flag (same shape as cowork.js).
  try {
    const verifyFlag = {
      task_id, tab_id,
      brief_size_bytes,
      // 2026-09-07 GATE2-mac: hash the SAME string the audit file holds
      // (briefed_body, sentinel-prefixed), not the bare brief_body. The
      // size field beside it has always measured briefed_body, so the pair
      // described two different strings and coord.verify_paste (which hashes
      // the file) could never agree with it. Measured before the fix: 0 of
      // 4,735 flags on disk verified, so the one documented use of this field
      // - a worker checking its paste was not truncated - returned a false
      // corruption signal every single time it was ever exercised.
      brief_sha256: crypto.createHash('sha256').update(briefed_body).digest('hex'),
      pasted_at: new Date().toISOString(),
    }
    fs.writeFileSync(path.join(BRIEFS_DIR, task_id + '-PASTE-VERIFY.flag'),
      JSON.stringify(verifyFlag, null, 2), 'utf8')
  } catch (e) {}

  // Orphan-tab detection: wait for the worker to issue ANY coord.* call.
  const ackTimeoutMs = (typeof params.worker_acknowledgment_timeout_ms === 'number')
    ? Math.max(0, Math.min(600000, params.worker_acknowledgment_timeout_ms))
    : DEFAULT_WORKER_ACK_TIMEOUT_MS
  let acknowledged = false
  let ack_via = null
  let ack_elapsed_ms = 0
  if (ackTimeoutMs > 0) {
    const workerFile = path.join(WORKERS_DIR, tab_id + '.json')
    const start = Date.now()
    let baseline_heartbeat = null
    try {
      const data = JSON.parse(fs.readFileSync(workerFile, 'utf8'))
      baseline_heartbeat = data.last_heartbeat_at
    } catch (e) {}
    while (Date.now() - start < ackTimeoutMs) {
      try {
        const data = JSON.parse(fs.readFileSync(workerFile, 'utf8'))
        if (data.last_heartbeat_at && data.last_heartbeat_at > baseline_heartbeat) {
          acknowledged = true
          ack_via = 'heartbeat'
          break
        }
      } catch (e) {}
      try {
        const files = fs.readdirSync(MESSAGES_DIR).filter(f => f.endsWith('.json'))
        files.sort((a, b) => b.localeCompare(a))
        for (const mf of files.slice(0, 60)) {
          try {
            const m = JSON.parse(fs.readFileSync(path.join(MESSAGES_DIR, mf), 'utf8'))
            const fromTab = (m && m.body && m.body.from) || (m && m.from)
            if (fromTab === tab_id) {
              acknowledged = true
              ack_via = 'message:' + ((m && m.body && m.body.type) || 'unknown')
              break
            }
          } catch (e) {}
        }
      } catch (e) {}
      if (acknowledged) break
      await sleep(WORKER_ACK_POLL_INTERVAL_MS)
    }
    ack_elapsed_ms = Date.now() - start
  }

  if (ackTimeoutMs > 0 && !acknowledged) {
    return {
      ok: false, tab_id, tab_credential, task_id,
      account_active_when_spawned,
      registered_at: register_result.body.registered_at,
      tab_handle,
      orphan: true,
      orphan_reason: 'no coord.* call from spawned worker within ' + ackTimeoutMs + 'ms',
      ack_elapsed_ms,
      brief_file_audit: auditFilePath,
      note: 'Mac dispatch: worker tab spawned via ide.chat_send_message({submit:true}) ' +
            'but model never sent a coord.* call (heartbeat/progress/done). Causes: model never started, ' +
            'extension host wedge, OOM, or auth gate. Call cowork.kill_worker({tab_id}) and retry.',
    }
  }

  return {
    ok: true,
    tab_id, tab_credential, task_id,
    account_active_when_spawned,
    registered_at: register_result.body.registered_at,
    brief_size_bytes,
    brief_storage,
    brief_file_audit: auditFilePath,
    role: 'worker',
    recovery_attempts: 0,
    tab_handle,
    coord_url,
    acknowledged,
    ack_via,
    ack_elapsed_ms,
    dispatcher: 'mac-dispatcher',
    submit_path,
    note: ackTimeoutMs > 0
      ? ('Worker acknowledged in ' + ack_elapsed_ms + 'ms via ' + ack_via +
         '. Mac path: ide.chat_send_message + applescript Enter.')
      : 'Fire-and-forget mode (ack timeout=0). Worker registered + brief populated + Enter sent.',
  }
}

// Re-export the cross-platform kill / cleanup / list from cowork.js.
// Those use ide.tabs / ide.tabs_close which run in extension-host space,
// no platform dependency.
module.exports = {
  dispatch_worker,
  // Exposed for tests only: the stand-down clause is inherited by every dispatch
  // BECAUSE it lives in here, so a test has to be able to assert on the composed
  // brief directly rather than trust that callers remember it.
  _composeBrief: composeBrief,
  // Exposed for tests only: the spawn-diff is the half of the tab-leak fix that
  // a recurring cron cannot defeat, and its whole safety argument is which
  // states it REFUSES, so those refusals have to be assertable directly.
  _ccTabIdSnapshot: _ccTabIdSnapshot,
  _diffSpawnedCcTab: _diffSpawnedCcTab,
  kill_worker: cowork.kill_worker,
  cleanup_orphan_workers: cowork.cleanup_orphan_workers,
  list_workers: cowork.list_workers,
  swap_creds: cowork.swap_creds,
  swap_history: cowork.swap_history,
}
