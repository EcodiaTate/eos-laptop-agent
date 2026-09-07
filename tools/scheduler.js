// scheduler.js - autonomy substrate scheduler module (Phase 3).
//
// Polls os_scheduled_tasks in Supabase Postgres every 30s, rotates
// ~/.claude/.credentials.json to the healthiest account, dispatches a CC chat
// tab via cowork.dispatch_worker, and tracks completion via coord inbox.
//
// Feature flag: SCHEDULER_ENABLED=true must be set explicitly. The scheduler
// does NOT start automatically to prevent unintended Postgres polling on agent
// restart before the substrate is ready.
//
// Architecture:
//   - dispatch loop (every 30s): leaseDueRows -> dispatchOne (serial, launch-lock)
//   - completion pass (every 5s): check coord inbox for done signals
//   - stale-lease recovery (every 60s): release leaked leases + mark orphans
//
// Invariants:
//   - launch-lock is always released in finally blocks.
//   - Status transitions: active -> dispatching -> running -> (completed|failed|orphaned)
//   - AllAccountsCappedError defers, does not fail.

'use strict'

const { Pool } = require('pg')
const { execFile } = require('child_process')
const { promisify } = require('util')
const execFileP = promisify(execFile)
// Single source of truth for schedule semantics (vendored byte-identical from
// ecodiaos-backend/src/lib/schedule-core.js, pinned by schedule-core-identity.test.js).
// Handles intervals of ANY size, daily, weekly, and raw cron. Replaced the
// in-file parseSchedule + scattered cron-parser dances that mis-fired intervals
// over 23h (every Nh was faked into "0 */N * * *" -> cron-parser collapsed it to
// daily). Doctrine: scheduler-one-schedule-engine-2026-06-20.
const scheduleCore = require('../lib/schedule-core')
let _credsModule = require('./creds')
function getCreds() { return _credsModule }
exports._setCredsModule = function (m) { _credsModule = m }
const coord = require('./coord')
// 2026-08-28 lane R1 item 2: owns the worker-lifecycle columns on
// os_scheduled_tasks and the SET fragment dispatchOne uses to clear them, so
// the writer (coord.signal_*) and the reader (here) cannot drift apart.
const taskSignals = require('./task-signals')
// Injection seam: tests pass a stub coord implementing { list_workers }.
// Only the stale-lease liveness check routes through getCoord(); the other
// coord call sites (completionPass) read coord directly to avoid behaviour
// drift in well-tested paths.
let _coordOverride = null
function getCoord() { return _coordOverride || coord }
exports._setCoord = function (c) { _coordOverride = c }

// ── Austerity band table (single source of truth, cross-repo) ────────────────
// The dispatch-time austerity gate must catch suppressed crons that carry NO
// austerity_paused marker: the `set <N>` lever is a POINT-IN-TIME snapshot, so a
// cron created OR re-registered after the last set (registration upserts reset
// austerity_paused to its column DEFAULT false) is suppressed-by-band yet
// marker-less, and a marker-only gate is blind to it (2026-07-15: 20 such crons
// fired past L4, incl status-board-execute-top cascading worker-tab spawns). We
// compute the band live from (name -> band, live level) instead of trusting the
// marker. cronAusterity.js is a pure module (no requires); required by absolute
// path so the band definitions stay single-source in the backend. If the path
// ever moves the require fails and the gate degrades to marker-only (prior
// behaviour) rather than crashing dispatch.
// Doctrine: austerity-gate-needs-dispatch-time-twin-not-just-lease-time (band-aware extension).
let _austerityCfg = null
try {
  _austerityCfg = require('/Users/ecodia/.code/ecodiaos/backend/src/config/cronAusterity')
} catch (e) {
  process.stderr.write('[scheduler] austerity band table unavailable (' +
    (e && e.message || e) + '); dispatch gate falls back to marker-only\n')
}
exports._setAusterityCfg = function (c) { _austerityCfg = c } // test seam

// Read the live PRESENCE POSTURE {mode,lean,frozen} from kv_store (the same key
// the lever writes: POSTURE_KV_KEY = scheduler.presence_posture). Returns a
// normalized state object, or null when the key is absent / read fails - the
// caller then proceeds on the austerity_paused marker alone.
// This is the dispatch-time twin of the lever's OWN suppression predicate: the
// presence taper writes austerity_paused markers from decidePosture(), so the
// dispatch gate MUST also use decidePosture() or the two diverge and any row the
// two classify differently gets leased-then-skipped forever. That is exactly what
// stranded 11 always-on/compliance crons (neo4j-maintenance, self-evolution,
// decision-quality-pass, the two *-review compliance rows ...) from Jun 2026 to
// 2026-08-12 - leased every 30s, blocked at dispatch, never run. The legacy
// numeric-level gate this used to fall back to was removed 2026-08-15 (posture is
// the single austerity vocabulary now).
// Doctrine: austerity-gate-needs-dispatch-time-twin-not-just-lease-time
//         + scheduler-presence-mode-taper-2026-08-11.
async function readAusterityPosture(pool) {
  if (!_austerityCfg || !_austerityCfg.POSTURE_KV_KEY ||
      typeof _austerityCfg.decidePosture !== 'function') return null
  try {
    const r = await pool.query(
      `SELECT value FROM kv_store WHERE key = $1 LIMIT 1`, [_austerityCfg.POSTURE_KV_KEY])
    const raw = r.rows[0] && r.rows[0].value
    if (raw == null) return null
    let parsed = raw
    if (typeof raw === 'string') { try { parsed = JSON.parse(raw) } catch (_) { return null } }
    return _austerityCfg.normalizeState(parsed)
  } catch (_) { return null }
}
exports._readAusterityPosture = readAusterityPosture

// ── constants ────────────────────────────────────────────────────────────────

// 2026-06-10 branch-thrash guard: each dispatched worker gets its own git
// worktree off origin/main so its branch flips and commits cannot reach the
// conductor's shared tree. Paired with the reference-transaction hook in
// backend/scripts/branch-thrash-guard.sh which is the runtime backstop.
//
// SHARED_TREE: the conductor's working tree. We invoke `git worktree add` from
// here so the worktree is registered against the shared .git directory.
// WORKTREE_ROOT: parent path for per-task linked worktrees. Predictable per
// row.id so allocate+prune is idempotent across retries and crashes.
//
// Doctrine: backend/patterns/branch-thrash-guard-on-shared-tree-2026-06-10.md
const SHARED_TREE = process.env.SCHEDULER_SHARED_TREE || '/Users/ecodia/.code/ecodiaos/backend'
const WORKTREE_ROOT = process.env.SCHEDULER_WORKTREE_ROOT || '/Users/ecodia/.code/ecodiaos/_worktrees/dispatched'
const WORKTREE_GIT_TIMEOUT_MS = 30_000

const POLL_INTERVAL_MS = 30_000
const STALE_LEASE_INTERVAL_MS = 60_000
const DISPATCH_LIMIT = 5
// 2026-05-29 ultracode audit H5 fix. Was 30000, below the 84.5s cold-MCP
// observed floor + spike margin per worker-ack-timeout doctrine. On timeout
// the launch-lock released, defeating the cred-rotation serialisation.
// 2026-06-08 self-evolution bump: empirical Mac cold-MCP signal_bound
// latency (n=60 across last 4h on this agent) p50=24s p75=32s p90=77s
// p95=402s max=1283s. The 180s cap was missing the entire p90-p99 tail and
// most dispatches were logging signal_bound timeout despite the worker
// having successfully paste'd + read its brief; the rows still ran but
// recovery + cleanup-orphan churn was real. 600s catches p95 cleanly while
// keeping launch-lock throughput acceptable (next dispatch can still proceed
// every 10min worst-case). Tunable via env without code change.
const SIGNAL_BOUND_TIMEOUT_MS = parseInt(process.env.SCHEDULER_SIGNAL_BOUND_TIMEOUT_MS, 10) || 600_000
const ORPHAN_TIMEOUT_MS = 6 * 60 * 60 * 1000
// 2026-06-22 cadence-aware running-orphan reclaim. ORPHAN_TIMEOUT_MS (6h) is the
// correct backstop for a one-shot delayed worker (orphaning it is terminal and
// destructive, and a one-shot can legitimately run long), but it is far too slow
// for a fast CRON whose worker binds (flips to status='running') then dies
// silently without coord.signal_done: the row sits stuck 'running' and BLOCKS
// every subsequent fire until 6h elapse. For an hourly cron that is ~6 lost
// cycles. Observed twice in the same shape on gmail-inbox-poll (heal 2026-06-21
// 11:43Z, recurred 2026-06-22 ~07:16Z lease, inbox triage cadence blind ~6h).
// The branch-3 liveness gate (hasLiveWorkerForTask) already refuses to reclaim a
// worker that is still heartbeating, so a SHORTER window for cron rows cannot
// thunder-herd a live worker; it only lets a provably-dead cron worker (heartbeat
// stale > STALE_WORKER_LIVENESS_MS, or absent from coord) recover within minutes
// instead of hours, and cron reclaim is non-destructive (defer to next interval,
// never terminal orphan). 30min sits decisively above the ~21min p99 cold-bind
// tail (a running row has already bound, so that tail does not even apply) yet is
// 12x faster than 6h. Non-cron rows keep the full 6h ORPHAN_TIMEOUT_MS. Tunable.
// Doctrine: scheduler-running-orphan-reclaim-must-be-cadence-aware-2026-06-22.
const RUNNING_CRON_ORPHAN_MS = parseInt(process.env.SCHEDULER_RUNNING_CRON_ORPHAN_MS, 10) || 30 * 60 * 1000
const COMPLETION_POLL_INTERVAL_MS = 5_000
// 2026-06-18 half-state race fix. Must stay ABOVE SIGNAL_BOUND_TIMEOUT_MS (600s)
// so staleLeaseRecovery branch-1 cannot reap a row that dispatchOne is still
// legitimately waiting on for signal_bound. When the two windows were equal
// (both 10min) a slow-but-live cold-MCP bind got reclaimed mid-wait, and the
// running-flip then produced a status=running + leased_by=NULL zombie. 15min
// gives 5min of headroom over the bind timeout; the p99 bind tail (~21min) is
// covered by the dispatchOne running-flip lease guard, not by this window.
// 2026-07-17: STALE_DISPATCHING_MS is ALSO the dispatch-prep grace ceiling: no
// reclaim path (staleLeaseRecovery branches 1/2a/2b) may touch a 'dispatching'
// row whose lease is younger than this. Observed prep time on this Mac (brief
// build + worktree alloc + IDE tab spawn + paste + signal_bound wait): p50 ~24s,
// p95 ~402s, hard bind timeout 600s - so 15 min = observed floor-safe ceiling
// (600s bind timeout + 5 min headroom). The ceiling only measures REAL prep now:
// dispatchOne refreshes leased_at at dispatch-start (step 2c), and rows queued
// in-process are additionally shielded by the _inFlightDispatchIds registry
// below, so queue wait can no longer age a lease into reclaim eligibility.
const STALE_DISPATCHING_MS = 15 * 60 * 1000   // 15 min -> retry (> 600s bind timeout); dispatch-prep grace ceiling
const MAX_RETRY_COUNT = 3
// 2026-08-29 lane D1 pass 4. Settle window for a row SUSPENDED MID-LEASE, i.e.
// status still 'dispatching' or 'running' while last_status says 'paused'.
// Deliberately LONGER than STALE_DISPATCHING_MS and not derived from it: the
// observed p99 cold-bind tail is ~21min, so a 15min settle could retire the row
// out from under a tab that is still legitimately launching, and a signal_bound
// refused against a settled row holds the SERIAL fleet-wide launch-lock until it
// times out. 30min clears that tail with margin. The strand this settles is
// PERMANENT, so waiting is free.
const SUSPENDED_LEASE_SETTLE_MS = parseInt(process.env.SCHEDULER_SUSPENDED_LEASE_SETTLE_MS, 10) || 30 * 60 * 1000
// 2026-06-19 next_run_at-recompute fix. staleLeaseRecovery branch 1 (retryable
// stale-dispatch) used to free the row back to 'active' while leaving next_run_at
// at its past-due value, so the row was due on the very next 30s poll and re-fired
// off-cadence up to MAX_RETRY times. Per
// [[cron-rearm-must-recompute-next-run-at-or-guard-reentry-per-period-2026-06-19]]
// a retry must use a BOUNDED short delay, never an unbounded immediate re-due.
// 5 min preserves up to MAX_RETRY quick re-attempts for a transient bind failure
// (3 retries spread over ~15 min) without hammering every poll, and is short
// enough that a genuinely-due cron loses no cadence. The leaseDueRows re-entry
// guard is the second layer: a cron that already RAN this period is dropped from
// dispatch regardless of how next_run_at got clobbered.
const RETRY_BACKOFF_MS = 5 * 60 * 1000   // 5 min bounded retry delay
// 2026-05-29 ultracode audit C3 fix. cleanup_orphan_workers is the only
// backstop the chain's refuse-and-leak posture relies on; the audit caught
// it wired to no cron at all. 7 min picks up leaked tabs within a worker
// lifetime, doesn't thrash ide.tabs.
const CLEANUP_ORPHAN_INTERVAL_MS = 7 * 60 * 1000

// 2026-06-20 sleep-resilience. The agent runs on a Mac laptop with no caffeinate
// wrapper; KeepAlive in the launchd plist only restarts the process on EXIT, not
// when the OS freezes it during system sleep / hibernate. When the host sleeps,
// the Node event loop stops and ALL setInterval loops below (dispatch, completion,
// stale-lease recovery, cap observer, orphan cleanup) stop ticking. Crons leased
// into status='dispatching'/'running' just before the freeze then sit stuck until
// the agent next gets sustained runtime. Verified root cause of the 2026-06-18 and
// 2026-06-19 fleet-wide stuck-running recurrences: pmset shows the host hibernated
// on a 1% battery for ~9h ('Low Power Sleep' TCPKeepAlive=inactive), waking only on
// acattach, and the named crons recovered only once the host was sustainedly awake.
// We cannot stop the host sleeping from inside the process (critical-battery
// hibernate ignores caffeinate), but we CAN make the first resumed tick reconcile
// immediately and emit an OBSERVABLE wake-stall signal instead of a silent stall.
// A gap between consecutive stale-lease ticks far exceeding STALE_LEASE_INTERVAL_MS
// is the fingerprint of a frozen-then-resumed loop. Doctrine:
// [[scheduler-must-survive-mac-sleep-2026-06-20]].
const WAKE_STALL_FACTOR = 3

// ── Postgres pool (injection seam for tests) ─────────────────────────────────

let _pool = null

function getPool() {
  if (!_pool) {
    const connStr = process.env.DATABASE_URL
    if (!connStr) throw new Error('scheduler: DATABASE_URL env var is required')
    _pool = new Pool({
      connectionString: connStr,
      keepAlive: true,
      idleTimeoutMillis: 30_000,
      // 2026-07-12: raised from 10s. macOS mDNSResponder degrades after sleep/wake and takes
      // 5 to 9 seconds to resolve a name that 1.1.1.1 answers in 50ms. TLS and auth then land
      // on top, so a 10s budget expired mid-handshake and EVERY lease failed with
      // "Connection terminated due to connection timeout". The scheduler went silent for 88h
      // and no cron fired. The DB was healthy the whole time; the resolver was not.
      connectionTimeoutMillis: 30_000,
      // 2026-07-17 silent-dispatch-death root-cause fix. The dispatch loop is a
      // NON-REENTRANT setInterval guarded by `dispatchPassRunning`, reset ONLY in
      // its own finally. A query that STALLS server-side (a lock wait, a Supavisor
      // hiccup, post-sleep mDNS lag mid-statement) neither resolves nor rejects, so
      // the pass's await never settles, finally never runs, and dispatchPassRunning
      // latches TRUE forever - every later tick then no-ops at `if (running) return`.
      // The interval keeps firing and the process stays alive (health + API still
      // serve), so launchd KeepAlive never restarts it: the loop is DEAD while the
      // process is GREEN. The pool 'error' handler only catches connection DROPS,
      // not query HANGS, so it could not save us. query_timeout is a pure client-side
      // JS timer that REJECTS a query that overruns (works on any pooler mode);
      // statement_timeout is the server-side belt that also cancels the zombie query
      // (safe here: port 5432 is Supavisor SESSION mode, so the startup param sticks).
      // A rejected lease query now hits leaseDueRows' inner catch -> the pass returns
      // -> finally resets the guard -> the next 30s tick re-leases. Self-healing.
      statement_timeout: 25_000,
      query_timeout: 30_000,
    })
    // 2026-06-14 restart-loop root-cause fix. node-postgres emits an 'error'
    // event on the Pool when an IDLE pooled client's TCP connection dies. The
    // Supabase transaction/session pooler drops idle connections routinely,
    // surfacing as "Connection terminated unexpectedly". With NO listener on
    // this event, Node treats it as an uncaughtException and KILLS THE PROCESS;
    // launchd respawns the agent, the next idle connection drops again, and the
    // whole laptop-agent restart-loops (42 boots observed in one log). A
    // no-op-but-present listener makes the drop non-fatal - node-postgres
    // transparently opens a fresh connection on the next query.
    _pool.on('error', (err) => {
      process.stderr.write('[scheduler] pg pool idle-client error (non-fatal, pool reconnects): ' + (err && err.message || err) + '\n')
    })
  }
  return _pool
}

// Injection seam: tests pass a stub pool object implementing { query(sql, params) }
exports._setPool = function (p) { _pool = p }

// ── dispatcher (injection seam for tests) ────────────────────────────────────

let _dispatcher = null

function getDispatcher() {
  if (!_dispatcher) {
    _dispatcher = require('./cowork')
  }
  return _dispatcher
}

// Injection seam: tests pass a stub dispatcher implementing { dispatch_worker }
exports._setDispatcher = function (d) { _dispatcher = d }

// ── worktree allocator (injection seam for tests) ────────────────────────────
//
// 2026-06-10 branch-thrash guard. Per-row isolated worktree off origin/main so
// the dispatched worker tab can `git checkout -b`, commit, even `git reset
// --hard` without touching the conductor's shared tree. The shared tree's
// .git/hooks/reference-transaction is the backstop when a worker still tries
// the shared path despite the brief.
//
// Path scheme: WORKTREE_ROOT/<row.id> - predictable across retries so
// allocate+prune is idempotent. allocateWorktreeForRow first force-prunes any
// stale entry at that path, then `git worktree add -B worker/<row.id>` off
// origin/main. The branch name uses the FULL row.id (not a truncated prefix)
// so two concurrent dispatches with similar id prefixes do not collide on the
// branch ref - a 2026-06-10 verify-gate failure caught this exact case.
// pruneWorktreeForRow is callable on completion, failure, orphan, and
// stale-lease paths without state-tracking.

const fs = require('fs')
const path = require('path')

async function defaultAllocateWorktreeForRow(row) {
  const wtPath = path.join(WORKTREE_ROOT, String(row.id))
  fs.mkdirSync(path.dirname(wtPath), { recursive: true })

  // Idempotent cleanup: forget any stale worktree at that path first.
  await runGit(['worktree', 'remove', '--force', wtPath]).catch(() => {})
  await runGit(['worktree', 'prune']).catch(() => {})

  // 2026-06-20: `worktree remove --force` is a silent no-op when the path
  // exists on disk but is no longer a REGISTERED worktree (crashed prior
  // dispatch, or git metadata pruned out from under it). The swallowed error
  // left the directory standing, so the `worktree add` below died with
  // "<path> already exists" and the row fell back to an UNISOLATED shared-tree
  // dispatch - a branch-thrash hazard that is acute when several conductor
  // chats run concurrently. Force-clear any surviving directory so add always
  // lands on a clean path. Same destroy-on-redispatch posture as the
  // remove --force above (the path is keyed to THIS row.id, whose prior attempt
  // is dead); scoped strictly under WORKTREE_ROOT as a safety bound.
  if (fs.existsSync(wtPath) && wtPath.startsWith(WORKTREE_ROOT + path.sep)) {
    fs.rmSync(wtPath, { recursive: true, force: true })
    await runGit(['worktree', 'prune']).catch(() => {})
  }

  // Refresh origin/main so the worktree branches off recent base.
  await runGit(['fetch', 'origin', 'main', '--quiet']).catch(() => {})

  const branchName = 'worker/' + String(row.id)

  // 2026-08-30 lane H1. -B RESETS THE BRANCH REF, AND A RE-DISPATCH IS A SILENT
  // DESTRUCTION. defaultPruneWorktreeForRow harvests on the way OUT, which covers
  // completion, failure, orphan and stale-lease. It does not cover the way back
  // IN: the dead-row reaper re-arms rows, the row is leased again, and this
  // function opens with `worktree remove --force` and then `worktree add -B`,
  // which moves the ref to origin/main and drops every commit the prior attempt
  // made. No prune ran, so no harvest ran, and the doctrine is gone with no audit
  // line naming it. Harvesting here is the same add-only, em-dash-refusing,
  // never-commits call made one step earlier, and it is the ONLY point where the
  // prior attempt's commits are still reachable.
  //
  // Failure never blocks the allocation, for the same reason it never blocks the
  // prune: a worker that cannot start is a worse outcome than doctrine that stays
  // stranded one more cycle.
  try {
    const { harvestDoctrine } = require('./doctrine-harvest')
    const res = await harvestDoctrine({
      sharedTree: SHARED_TREE,
      branch: branchName,
      rowId: String(row.id),
    })
    if (res && res.landed && res.landed.length) {
      process.stderr.write('[scheduler] doctrine-harvest (pre-realloc): rescued ' +
        res.landed.length + ' pattern file(s) from ' + (res.branch || branchName) +
        ' before -B reset it\n')
    }
  } catch (e) {
    process.stderr.write('[scheduler] doctrine-harvest (pre-realloc) failed for ' + row.id +
      ': ' + (e && e.message || e) + ' (allocating anyway)\n')
  }

  await runGit(['worktree', 'add', '-B', branchName, wtPath, 'origin/main'])
  return wtPath
}

// 2026-08-23 doctrine-harvest. A dispatched worker CANNOT push (the worker
// capability ceiling denies `git ... push`, correctly: a worker is the likeliest
// injection pivot). So any pattern file it authored lived only on its branch,
// and pruning the worktree made that doctrine invisible to knowledge.lookup
// forever. Measured cost: 184 distinct pattern files stranded across 60
// branches, one method codified and lost three separate times. Board ad25d3fb.
//
// Harvest runs BEFORE the remove, which is the only ordering where the branch
// is still guaranteed readable. It is add-only (top-level patterns/*.md whose
// basename is absent from origin/main), never touches _archived, refuses
// em-dashes, and runs on pure git plumbing with a temp index so it can neither
// move HEAD nor write into any working tree. Failure NEVER blocks the prune.
async function defaultPruneWorktreeForRow(row) {
  const wtPath = path.join(WORKTREE_ROOT, String(row.id))
  try {
    const { harvestDoctrine } = require('./doctrine-harvest')
    const res = await harvestDoctrine({
      sharedTree: SHARED_TREE,
      branch: 'worker/' + String(row.id),
      rowId: String(row.id),
    })
    if (res && res.landed && res.landed.length) {
      process.stderr.write('[scheduler] doctrine-harvest: landed ' + res.landed.length +
        ' pattern file(s) on main from worker/' + row.id + ' (' + res.commit + ')\n')
    }
  } catch (e) {
    process.stderr.write('[scheduler] doctrine-harvest failed for ' + row.id + ': ' +
      (e && e.message || e) + ' (pruning anyway)\n')
  }
  await runGit(['worktree', 'remove', '--force', wtPath]).catch(() => {})
  await runGit(['worktree', 'prune']).catch(() => {})
}

async function runGit(args) {
  // ECODIAOS_BRANCH_OK=1 is load-bearing here, not future-proofing.
  // Measured 2026-08-27 on git 2.50.1: allocating a worktree from the shared
  // tree DOES open a HEAD ref update on the shared tree itself (ref=HEAD,
  // old=all-zeros, new=ref:refs/heads/<newbranch>), evaluated by that tree's
  // reference-transaction hook with GIT_DIR empty. The previous claim here,
  // that "worktree add does not move HEAD there", was wrong, and that error is
  // why the branch-thrash guard silently refused every MANUAL allocation for
  // months while automated dispatch kept working: this env var was the only
  // thing exempting us. The guard now also exempts an allocation by inspecting
  // the invoking command, so the manual path works too; this stays as the
  // explicit, process-independent guarantee for the dispatcher.
  // Doctrine: ecodiaos/backend/patterns/branch-thrash-guard-on-shared-tree-2026-06-10.md
  const env = Object.assign({}, process.env, { ECODIAOS_BRANCH_OK: '1' })
  return execFileP('git', ['-C', SHARED_TREE].concat(args), {
    timeout: WORKTREE_GIT_TIMEOUT_MS,
    env,
  })
}

let _allocateWorktreeForRow = defaultAllocateWorktreeForRow
let _pruneWorktreeForRow = defaultPruneWorktreeForRow

exports.allocateWorktreeForRow = function (row) { return _allocateWorktreeForRow(row) }
exports.pruneWorktreeForRow = function (row) { return _pruneWorktreeForRow(row) }

// Injection seam: tests pass {allocate, prune} stubs to skip subprocess git.
exports._setWorktreeFns = function (fns) {
  if (fns && typeof fns.allocate === 'function') _allocateWorktreeForRow = fns.allocate
  if (fns && typeof fns.prune === 'function') _pruneWorktreeForRow = fns.prune
}
exports._resetWorktreeFns = function () {
  _allocateWorktreeForRow = defaultAllocateWorktreeForRow
  _pruneWorktreeForRow = defaultPruneWorktreeForRow
}

// ── launch-lock (in-memory async mutex) ──────────────────────────────────────
//
// Makes dispatching serial even when the dispatch loop fires multiple rows.
// MUST be acquired BEFORE cred rotation and released AFTER coord signal_bound
// is received (or timeout). Released in finally blocks to prevent deadlock.

const _launchLockQueue = []
let _launchLockHeld = false

async function launchLockAcquire() {
  if (!_launchLockHeld) {
    _launchLockHeld = true
    return _launchLockRelease
  }
  return new Promise(resolve => {
    _launchLockQueue.push(resolve)
  })
}

function _launchLockRelease() {
  if (_launchLockQueue.length > 0) {
    const next = _launchLockQueue.shift()
    next(_launchLockRelease)
  } else {
    _launchLockHeld = false
  }
}

exports._launchLock = { acquire: launchLockAcquire }

// ── in-flight dispatch registry (self-lease-steal race fix, 2026-07-17) ─────
//
// THE RACE (root cause of the 2026-07-17 dispatch-eating incident; canonical
// comment, referenced from staleLeaseRecovery + start()):
// The dispatch loop was a re-entrant setInterval: with a large due-row backlog
// (77 rows observed) every 30s tick ran leaseDueRows and stacked 5 MORE
// dispatchOne calls onto the serial launch-lock, whose holder can legitimately
// take up to SIGNAL_BOUND_TIMEOUT_MS (10 min) per row. leased_at is stamped at
// QUEUE-ENTRY, so queued rows aged past STALE_DISPATCHING_MS before their prep
// ever started. staleLeaseRecovery branch-1 then reclaimed them (status='active',
// retry_count++, next_run_at=+5min) OUT FROM UNDER this process's own queued
// dispatchOne; a later lease pass re-leased them under a NEW token (same pid,
// different timestamp - the log fingerprint), the old dispatchOne aborted at its
// startGuard AFTER burning a full launch-lock hold on prep, and rows cycled
// active->dispatching->active until retries exhausted (one-shot rows died at
// status='failed' with run_count=0) or wedged at 'dispatching'. When a first
// spawn had actually succeeded before the steal, the re-lease double-ran the
// task (cowork.friend-mcp-memory-tools-morning-reexec, runs=2).
//
// THE FIX, three layers:
//  1. start()'s dispatch loop is non-reentrant (skip the tick when a pass is
//     still draining) so the launch-lock queue is bounded to one batch.
//  2. This registry: every row currently inside dispatchOne (queued OR prepping)
//     is listed here; staleLeaseRecovery's 'dispatching' sweeps skip listed rows
//     no matter how old their lease is. In-process prep is alive by definition.
//     Crash-safe: if this process dies, the Set dies with it and the normal
//     leased_at-age reclaim (grace ceiling STALE_DISPATCHING_MS) takes over.
//  3. dispatchOne's pre-spawn guard re-checks run_count and coord worker
//     liveness so a stolen-then-recovered row can never double-spawn
//     (doctrine: scheduler-stale-lease-must-check-coord-worker-liveness-
//     before-redispatch-2026-06-10).
const _inFlightDispatchIds = new Set()
exports._inFlightDispatchIds = _inFlightDispatchIds  // test seam / introspection

// ── dispatch-outage pager ────────────────────────────────────────────────────
//
// 2026-08-13 survival-critical resilience (Tate overseas from ~2026-09-24). When
// VSCode is dead, every dispatch hits the transient-IDE-bridge path below and
// defers 5min FOREVER with no retry increment, no failed mark, and no alert.
// scheduler-health only writes into the NEXT CC session, which never starts when
// the conductor is dead, so the outage is totally silent and ALL scheduled work
// stalls unseen. The imessage-agent has its own launchd KeepAlive and survives,
// so text-tate.js is the one channel that still reaches Tate when the IDE is gone.
//
// Track consecutive transient-bridge defers on the dispatch path. When N in a row
// (default 3, about 15min at the 5min defer cadence) fire EXACTLY ONE page, then
// latch so we never spam. The counter + latch reset on the first successful
// dispatch (a spawned worker tab = the bridge is alive again), so a fresh outage
// pages again. Doctrine: mac-unattended-resilience-conductor-survival-2026-08-13.
const OUTAGE_PAGE_THRESHOLD = parseInt(process.env.SCHEDULER_OUTAGE_PAGE_THRESHOLD, 10) || 3
const TEXT_TATE_SCRIPT = process.env.SCHEDULER_TEXT_TATE_SCRIPT ||
  '/Users/ecodia/.code/ecodiaos/backend/imessage-agent/text-tate.js'
let _consecutiveTransientDefers = 0
let _outagePageSent = false

// Default sender: spawn text-tate.js via node, no shell (args array so the message
// can never be shell-interpreted). Overridable for tests so the unit harness asserts
// on the CONSTRUCTED command without ever texting Tate.
//
// 2026-08-13 delivery-proven (Hunter 3 #3): NOT detached/unref any more - we wait for
// the exit code and report it via done(err, code) so the caller can retry a FAILED
// send instead of latching a page that never landed. text-tate.js exits fast (pure
// osascript); a 30s watchdog treats a hung send as a failure so a wedged sender can
// never leave the outage silently un-paged. This runs inside the long-lived daemon,
// so waiting on a sub-second child is free (the crash-loop pager in index.js stays
// detached because ITS boot process may exit immediately).
let _pagerSender = function defaultPagerSender(scriptPath, args, done) {
  let settled = false
  const finish = (err, code) => {
    if (settled) return
    settled = true
    if (typeof done === 'function') { try { done(err, code) } catch (_e) {} }
  }
  try {
    const child = require('child_process').spawn('node', [scriptPath, ...args], {
      stdio: 'ignore',
    })
    const watchdog = setTimeout(() => {
      process.stderr.write('[scheduler] outage pager timed out (30s); treating as failed\n')
      try { child.kill('SIGKILL') } catch (_e) {}
      finish(new Error('pager send timed out'), null)
    }, 30000)
    if (watchdog.unref) watchdog.unref()
    child.on('error', (e) => {
      process.stderr.write('[scheduler] outage pager spawn error: ' + (e && e.message || e) + '\n')
      clearTimeout(watchdog)
      finish(e, null)
    })
    child.on('exit', (code) => {
      clearTimeout(watchdog)
      finish(null, code)
    })
  } catch (e) {
    process.stderr.write('[scheduler] outage pager send error: ' + (e && e.message || e) + '\n')
    finish(e, null)
  }
}
exports._setPagerSender = function (fn) { _pagerSender = fn }  // test seam
exports._getOutageState = function () {
  return { consecutive: _consecutiveTransientDefers, sent: _outagePageSent }
}
exports._resetOutageState = function () {
  _consecutiveTransientDefers = 0
  _outagePageSent = false
}

// Called from the transient-bridge defer branch. Increments the consecutive
// counter and, once it crosses the threshold and no page has been sent for THIS
// outage, constructs + fires exactly one page. Returns the constructed command
// (or null when no page fired) so tests can assert on it.
exports.noteTransientDefer = function noteTransientDefer(errMsg) {
  _consecutiveTransientDefers += 1
  if (_consecutiveTransientDefers >= OUTAGE_PAGE_THRESHOLD && !_outagePageSent) {
    // Latch OPTIMISTICALLY so a burst of defers during the in-flight send does not
    // fire a second page. But this is provisional: if text-tate.js does not exit 0,
    // the done callback UNLATCHES so the next 5min defer tick retries. Only a
    // confirmed exit-0 keeps the latch, preserving one-page-per-outage on success.
    // 2026-08-13 delivery-proven (Hunter 3 #3): the old code latched unconditionally
    // before a fire-and-forget spawn, so a broken send path meant the outage was
    // never reported and never retried.
    _outagePageSent = true
    const msg = 'DISPATCH OUTAGE: no live IDE for ' + _consecutiveTransientDefers +
      ' consecutive dispatch attempts (~' + (_consecutiveTransientDefers * 5) +
      'min). All scheduled work is STALLED (transient IDE-bridge, deferring 5min). ' +
      'Last error: ' + String(errMsg || '').slice(0, 120) +
      '. Reopen VSCode / the conductor to resume dispatch.'
  // --urgency critical is LOAD-BEARING. These pagers are the last-resort channel
    // (see the TEXT_TATE_SCRIPT comment above: the one path that still reaches Tate
    // when the IDE is gone), and every one of them named fleet machinery in its own
    // alarm text. text-tate defaults to fyi, which the send gate lints for
    // operator vocabulary, caps at 450 chars and makes compete for the daily chatter
    // budget. Measured 2026-09-05: the DISPATCH OUTAGE page was refused 62 times over
    // four days and landed ZERO, every one on the word "DISPATCH" in its first line,
    // while the retry loop below faithfully unlatched and tried again into a wall.
    // The gate already holds the right belief - it warns rather than blocks on a
    // critical alarm ("never mute a page over word choice") and never refuses one for
    // length - so the defect was the tier, not the gate and not the words.
    const args = ['--from', 'scheduler watchdog', '--urgency', 'critical', msg]
    _pagerSender(TEXT_TATE_SCRIPT, args, function (err, code) {
      if (err || code !== 0) {
        // Send did not confirm delivery: unlatch so the next defer retries. Guard
        // against clobbering a latch that a successful dispatch already cleared
        // (noteSuccessfulDispatch resets both) - unlatching then is harmless.
        _outagePageSent = false
        process.stderr.write('[scheduler] OUTAGE PAGE send FAILED (code=' +
          code + (err ? ', err=' + (err.message || err) : '') + '); unlatched, will retry next defer\n')
      } else {
        process.stderr.write('[scheduler] OUTAGE PAGE delivered (text-tate exit 0); latched for this outage\n')
      }
    })
    process.stderr.write('[scheduler] OUTAGE PAGE fired after ' +
      _consecutiveTransientDefers + ' consecutive transient defers\n')
    return { command: 'node ' + TEXT_TATE_SCRIPT + ' ' + args.map(a => JSON.stringify(a)).join(' '), args, message: msg }
  }
  return null
}

// Called at the successful-dispatch point (a worker tab was spawned, so the IDE
// bridge is alive). Resets the outage tracker so a later outage pages afresh.
exports.noteSuccessfulDispatch = function noteSuccessfulDispatch() {
  if (_consecutiveTransientDefers !== 0 || _outagePageSent) {
    process.stderr.write('[scheduler] dispatch recovered; clearing outage tracker (was ' +
      _consecutiveTransientDefers + ' defers, paged=' + _outagePageSent + ')\n')
  }
  _consecutiveTransientDefers = 0
  _outagePageSent = false
  // A spawned tab proves an account was available, so it ends a capped outage too.
  if (_cappedFirstDeferAt !== null || _cappedPageSent) {
    process.stderr.write('[scheduler] dispatch recovered; clearing CAPPED outage tracker (was ' +
      _cappedDefers + ' defers, paged=' + _cappedPageSent + ')\n')
  }
  exports._resetCappedOutageState()
}

// -- all-accounts-capped outage pager (2026-08-29 lane D1) ---------------------
//
// The failure this closes. When pick_healthiest_account throws
// AllAccountsCappedError, dispatchOne deferred the row and returned. It emitted
// no page, no status_board row and no stderr line: the only process.stderr.write
// in that block was on a Postgres error. Three mechanisms exist to notice that
// dispatch has stopped and every one of them missed this class.
//   - The pager saw one failure class. noteTransientDefer had exactly ONE
//     non-test call site, in the transient-IDE-bridge branch immediately below
//     the capped branch. Fifteen minutes with no IDE texts Tate. Five weeks with
//     no account texted nobody.
//   - The runaway breaker cannot see it, correctly: breakerRecordDispatch runs
//     AFTER the confirmed tab spawn (moved there 2026-08-26 so spawn outages stop
//     manufacturing false quarantines), and the capped throw precedes the spawn.
//   - scheduler-health.sh's capped_churn counts retry_count >= 2 AND last_error
//     ILIKE AllAccountsCappedError. The defer touched neither, so the ONE
//     detector named for this failure always read 0 and reported green.
// Measured on the live agent log, 24.5MB over its whole lifetime: 36
// AllAccountsCapped lines, 2,311 "breaker: recorded dispatch" lines, and ZERO
// "OUTAGE PAGE" lines. The breaker instrumentation is demonstrably alive; the
// capped path had never once produced a page.
//
// Why the page is TIME-gated, not count-gated like noteTransientDefer. A capped
// defer fires once per DUE ROW, and 90-odd rows are dispatchable, so a count
// threshold crosses in seconds and cannot honestly claim "this has persisted".
// The elapsed clock since the first defer of THIS outage can. The count still
// travels in the message because it says how much work is stalled.
const CAPPED_PAGE_MIN_MS = parseInt(process.env.SCHEDULER_CAPPED_PAGE_MIN_MS, 10) || 10 * 60 * 1000
// The defer's bound on retry_count. It MUST equal scheduler-health.sh:103's
// threshold (retry_count >= 2): lower and the detector stays blind, higher and a
// cap outage eats the MAX_RETRY_COUNT failure budget it is borrowing the column
// from. markFailed refuses to inherit a count still carrying the cap marker,
// which is the other half of keeping those two meanings apart.
exports.CAPPED_RETRY_MARK = 2

// The ONE spelling of the marker. The JS predicate below and the SQL expression
// under it are both built from this, because they encode the same rule in two
// languages and a hand-copied literal in either one drifts silently.
exports.CAPPED_MARKER_TOKEN = 'AllAccountsCappedError'

// The retry budget a row ACTUALLY has left, as SQL.
//
// retry_count carries two meanings at once. Its own is "how many times this row
// has failed", which MAX_RETRY_COUNT spends to permanently fail a one-shot row.
// The capped defer BORROWS it to make an outage visible to scheduler-health.sh's
// capped_churn detector (retry_count >= 2 AND last_error ILIKE the marker).
// A cap mark is not three failed attempts and must not be read as any.
//
// markFailed already reads it this way (see priorRetryCount). staleLeaseRecovery
// is the PARALLEL path and did not, which is the hazard this expression closes:
// leaseDueRows' UPDATE sets only status/leased_by/leased_at/updated_at, so a
// cap-marked row re-enters 'dispatching' still carrying retry_count=2 and the
// marker. Branch 1 then granted it ONE reclaim (2 -> 3) and the next stale pass
// handed it to branch 2b, which permanently fails it. Three reclaims became one.
// Measured 2026-08-29: 121 of the 128 unarchived rows carrying the marker are
// type='delayed', exactly the type 2b fails. And a cap outage ENDS in a
// thundering herd (~90 rows lease the moment it clears), which is when a spawn
// is likeliest to go stale, so the correlation runs the wrong way.
//
// Why neutralise the count rather than clear last_error at lease time: the
// detector greps last_error over a 24h window, so clearing it starves the very
// alarm the mark exists to feed. Neutralising keeps the mark visible to the
// detector and invisible to the budget.
// [[enumerate-all-trigger-paths-when-fixing-data-flow-bugs]]
exports.EFFECTIVE_RETRY_COUNT_SQL =
  "(CASE WHEN last_error ILIKE '%" + exports.CAPPED_MARKER_TOKEN + "%' THEN 0 ELSE COALESCE(retry_count, 0) END)"

let _cappedFirstDeferAt = null
let _cappedDefers = 0
let _cappedPageSent = false
let _cappedLastDeferAt = null

// THE LATCH USED TO DIE WITH THE DAEMON, and a cap outage is exactly when the
// daemon restarts: a cap is what fires account-switch.sh and what wakes the
// watchdogs. Both directions were wrong. Page at 10min, restart, clock zeroes,
// page again at 20min. Or restart at 9min and the page is pushed out another 10.
// The clock and the latch therefore live on disk, next to the canary heartbeats.
//
// Every transition mirrors to the file, not just the set points: the unlatch on
// an unconfirmed send and the clear on a recovered dispatch both write. A
// pageSent=true left behind by a finished outage would SILENCE the next one,
// which is strictly worse than the double page this replaces, so
// noteSuccessfulDispatch deletes the file and a persisted outage whose last
// defer is older than CAPPED_OUTAGE_STALE_MS is discarded rather than adopted.
// Every read and write is wrapped: the dispatcher must never crash-loop on its
// own state file, so a corrupt or unwritable one degrades to in-memory
// behaviour, which is exactly what shipped before this.
const CAPPED_OUTAGE_STALE_MS = 30 * 60 * 1000
let _cappedStatePath = null
function cappedStatePath() {
  if (_cappedStatePath) return _cappedStatePath
  return require('path').join(require('os').homedir(), '.ecodiaos', 'canary-state', 'scheduler-capped-outage.json')
}
exports._setCappedStatePath = function (p) { _cappedStatePath = p }

function persistCappedOutageState() {
  // WRITE TO A TEMP FILE AND RENAME. (lane D1 pass 3, 2026-08-29.) The reader's
  // fallback is correct: a truncated or corrupt file fails JSON.parse, is caught,
  // and the latch degrades to in-memory, which is the pre-persistence behaviour.
  // Verified by handing it a file cut off mid-number: it adopted nothing and did
  // not throw. But that correct fallback is a LOSS, and it fires exactly when it
  // costs most, because the restart that most needs the persisted clock is the one
  // during an outage. A bare writeFileSync is not atomic, so a full disk, a kill
  // mid-write, or the agent and a test writing at once all leave a torn file and
  // spend the latch. rename(2) on the same filesystem is atomic, so a reader sees
  // either the whole previous state or the whole new one and never a partial. The
  // temp name carries the pid so two writers cannot share a scratch file; the last
  // rename wins, which is the same outcome as the last write winning, minus the
  // window where a reader can catch it half-done.
  let tmp = null
  try {
    const fs = require('fs')
    const p = cappedStatePath()
    if (_cappedFirstDeferAt === null) { try { fs.unlinkSync(p) } catch (_e) {} ; return }
    fs.mkdirSync(require('path').dirname(p), { recursive: true })
    tmp = p + '.tmp.' + process.pid
    fs.writeFileSync(tmp, JSON.stringify({
      firstDeferAt: _cappedFirstDeferAt,
      defers: _cappedDefers,
      pageSent: _cappedPageSent,
      lastDeferAt: _cappedLastDeferAt,
    }))
    fs.renameSync(tmp, p)
    tmp = null
  } catch (e) {
    if (tmp) { try { require('fs').unlinkSync(tmp) } catch (_e) {} }
    process.stderr.write('[scheduler] capped outage state persist failed (' + (e && e.message || e) +
      '); falling back to in-memory latch for this outage\n')
  }
}

function loadCappedOutageState(nowMs) {
  try {
    const st = JSON.parse(require('fs').readFileSync(cappedStatePath(), 'utf8'))
    if (!st || typeof st.firstDeferAt !== 'number') return
    const last = typeof st.lastDeferAt === 'number' ? st.lastDeferAt : st.firstDeferAt
    if ((nowMs || Date.now()) - last > CAPPED_OUTAGE_STALE_MS) return  // that outage ended
    _cappedFirstDeferAt = st.firstDeferAt
    _cappedDefers = typeof st.defers === 'number' ? st.defers : 0
    _cappedPageSent = st.pageSent === true
    _cappedLastDeferAt = last
    process.stderr.write('[scheduler] adopted an in-progress capped outage from disk (first defer ' +
      new Date(_cappedFirstDeferAt).toISOString() + ', paged=' + _cappedPageSent + ')\n')
  } catch (_e) { /* absent or corrupt: in-memory behaviour, which is the old behaviour */ }
}
exports._loadCappedOutageState = loadCappedOutageState
// Adopt an outage already in progress. A restart mid-cap is the common case,
// not the exotic one, because the cap is what triggered the restart.
loadCappedOutageState()

// Test seam: drop the in-memory latch and re-read from disk, which is exactly
// what a daemon restart does.
exports._simulateRestart = function (nowMs) {
  _cappedFirstDeferAt = null
  _cappedDefers = 0
  _cappedPageSent = false
  _cappedLastDeferAt = null
  loadCappedOutageState(nowMs)
}

exports._getCappedOutageState = function () {
  return { firstDeferAt: _cappedFirstDeferAt, defers: _cappedDefers, sent: _cappedPageSent }
}
exports._resetCappedOutageState = function () {
  _cappedFirstDeferAt = null
  _cappedDefers = 0
  _cappedPageSent = false
  _cappedLastDeferAt = null
  persistCappedOutageState()   // clears the file; a stale latch silences the NEXT outage
}

// True when a row's last_error is the marker the capped defer writes. Used by
// markFailed and by staleLeaseRecovery's SQL twin to tell a cap mark from a
// spent failure budget.
exports.isAllAccountsCappedMarker = function (lastError) {
  return new RegExp(exports.CAPPED_MARKER_TOKEN, 'i').test(String(lastError || ''))
}

// Tate reads these at 3am. Doctrine is AEST to humans, UTC to machines, and the
// ISO stamp this used to print (2026-08-29T08:00:00.435Z) is neither readable
// nor actionable at that hour.
function aest(ms) {
  try {
    const p = new Intl.DateTimeFormat('en-AU', {
      timeZone: 'Australia/Brisbane', year: 'numeric', month: '2-digit', day: '2-digit',
      hour: '2-digit', minute: '2-digit', hour12: false,
    }).formatToParts(new Date(ms)).reduce((a, x) => (a[x.type] = x.value, a), {})
    return p.year + '-' + p.month + '-' + p.day + ' ' + p.hour + ':' + p.minute + ' AEST'
  } catch (_e) { return new Date(ms).toISOString() }
}

// Counts one capped defer and, once the outage has persisted CAPPED_PAGE_MIN_MS
// and no page has been sent for THIS outage, fires exactly one. Returns the
// constructed page (or null) so tests can assert on it, mirroring
// noteTransientDefer. Latch semantics are the same: optimistic latch, unlatched
// by a send that does not confirm exit 0, so a broken send path retries.
exports.noteAllAccountsCapped = function noteAllAccountsCapped(err, nowMs) {
  const now = nowMs || Date.now()
  if (_cappedFirstDeferAt === null) _cappedFirstDeferAt = now
  _cappedDefers += 1
  _cappedLastDeferAt = now
  const persistedMs = now - _cappedFirstDeferAt
  if (persistedMs < CAPPED_PAGE_MIN_MS || _cappedPageSent) { persistCappedOutageState(); return null }
  _cappedPageSent = true
  persistCappedOutageState()

  let resetTxt = 'unknown (no reset time reported)'
  const resets = err && err.resets
  if (resets) {
    const times = Object.values(resets).filter(Boolean).map(t => new Date(t).getTime()).filter(t => !isNaN(t))
    if (times.length) resetTxt = aest(Math.min(...times))
  }
  const msg = 'ACCOUNT OUTAGE: every Claude account is capped. Dispatch has deferred ' +
    _cappedDefers + ' times over ~' + Math.round(persistedMs / 60000) +
    'min and ALL scheduled work is stalled. Earliest reset: ' + resetTxt +
    '. This does not clear itself: switch or re-auth an account ' +
    '(bash ~/.code/eos-laptop-agent/scripts/account-switch.sh <tate|code|money>).'
  // --urgency critical: see noteTransientDefer. Same last-resort class.
  const args = ['--from', 'scheduler capped-outage', '--urgency', 'critical', msg]
  _pagerSender(TEXT_TATE_SCRIPT, args, function (err2, code) {
    if (err2 || code !== 0) {
      _cappedPageSent = false
      persistCappedOutageState()   // the unlatch mirrors to disk, or a restart adopts a page that never landed
      process.stderr.write('[scheduler] CAPPED OUTAGE PAGE send FAILED (code=' + code +
        (err2 ? ', err=' + (err2.message || err2) : '') + '); unlatched, will retry next defer\n')
    } else {
      process.stderr.write('[scheduler] CAPPED OUTAGE PAGE delivered (text-tate exit 0); latched for this outage\n')
    }
  })
  process.stderr.write('[scheduler] CAPPED OUTAGE PAGE fired after ' + _cappedDefers +
    ' capped defers over ' + Math.round(persistedMs / 60000) + 'min\n')
  return { command: 'node ' + TEXT_TATE_SCRIPT + ' ' + args.map(a => JSON.stringify(a)).join(' '), args, message: msg }
}

// The whole AllAccountsCappedError arm of dispatchOne, extracted so a test can
// drive the REAL branch rather than a lookalike. Defers the row to the earliest
// reset (floored at 60s), marks retry_count so the health detector can see it,
// and notes the outage for the pager. The row write and the page are independent
// legs on purpose: a Postgres blip must not suppress the alert.
exports.handleAllAccountsCappedDefer = async function handleAllAccountsCappedDefer(row, err, nowMs) {
  const now = nowMs || Date.now()
  let deferMs = 60 * 60 * 1000  // default 1h
  if (err && err.resets) {
    const times = Object.values(err.resets)
      .filter(Boolean)
      .map(t => new Date(t).getTime())
      .filter(t => !isNaN(t))
    if (times.length > 0) {
      deferMs = Math.min(...times) - now + 60_000
      if (deferMs < 60_000) deferMs = 60_000
    }
  }
  const nextRun = new Date(now + deferMs)
  try {
    await getPool().query(
      `UPDATE os_scheduled_tasks
       SET status = 'active', leased_by = NULL, leased_at = NULL,
           retry_count = LEAST(COALESCE(retry_count, 0) + 1, $4),
           next_run_at = $1, last_error = $2, updated_at = NOW()
       WHERE id = $3`,
      [nextRun.toISOString(), 'AllAccountsCappedError - deferred ' + Math.round(deferMs / 60000) + 'min',
        row.id, exports.CAPPED_RETRY_MARK]
    )
  } catch (pgErr) {
    process.stderr.write('[scheduler] capped-defer pg error: ' + pgErr.message + '\n')
  }
  try {
    return exports.noteAllAccountsCapped(err, now)
  } catch (pageErr) {
    process.stderr.write('[scheduler] capped outage pager note error: ' + pageErr.message + '\n')
    return null
  }
}

// -- lease-path exception pager (2026-09-07 lane D1) ---------------------------
//
// THE FAILURE THIS CLOSES. On 2026-09-06 the dispatch loop's leaseDueRows call
// threw `getaddrinfo ENOTFOUND aws-1-ap-southeast-2.pooler.supabase.com` on every
// 30s pass from 14:00Z to 21:55Z. Zero rows were leased for 7h55m: no cron fired,
// no worker opened, the whole fleet was dead. It produced 24,273 stderr lines and
// ZERO alerts, and nobody found it for eight hours.
//
// Every mechanism built to notice a dispatch stoppage missed it, each for its own
// reason, which is why the fix belongs HERE and not at the next trigger:
//   - noteTransientDefer pages on the IDE-bridge branch. Never reached: the throw
//     happens in the lease, two hundred lines before any dispatch branch runs.
//   - noteAllAccountsCapped pages on the cap branch. Same reason.
//   - breakerRecordDispatch counts only AFTER a confirmed tab spawn, so a fleet
//     that spawns nothing counts nothing and the breaker reads idle, not broken.
//   - scheduler-health.sh counts retry_count on cap markers. The lease throw never
//     reaches a row, so no row is marked and the detector reads green.
//   - the watchdog at checkDispatchWedge keys on _dispatchPassRunning. The pass was
//     not wedged. It ran, threw, and returned cleanly every 30s, which is the one
//     shape a hang detector cannot see.
// Same shape as [[a-cap-defer-that-increments-nothing-is-invisible-to-the-detector-built-for-it-2026-08-29]],
// reached by a different trigger. So this counts EVERY lease exception, not DNS.
//
// WHY THE PAGE IS THE PRIMARY ALERT AND THE BOARD ROW IS NOT. status_board lives
// behind the SAME Supabase pooler whose name resolution is the most likely cause of
// a lease throw. An alert that writes to the board first is an alert that is silent
// in exactly the case it was built for. text-tate.js is pure osascript into
// Messages.app: no network, no DB, no Claude budget, so it fires while the pooler is
// unreachable. The board row is attempted anyway (a lease throw has other causes),
// and when that write fails the outage is held in a LOCAL breadcrumb file and
// flushed to the board by noteLeaseOk the moment leasing recovers. The board
// therefore always gets the row; on a DB-caused outage it gets it on recovery.

const LEASE_ERROR_PAGE_THRESHOLD =
  parseInt(process.env.SCHEDULER_LEASE_ERROR_PAGE_THRESHOLD, 10) || 10
const LEASE_BREADCRUMB_FILE = process.env.SCHEDULER_LEASE_BREADCRUMB_FILE ||
  '/Users/ecodia/.ecodiaos/coordination/lease-outage.json'

exports.LEASE_ERROR_PAGE_THRESHOLD = LEASE_ERROR_PAGE_THRESHOLD
exports.LEASE_BREADCRUMB_FILE = LEASE_BREADCRUMB_FILE

// Test seam, same shape as _setPagerSender. A suite that exercises the pager must
// not reach Postgres: the board write is a separate leg on purpose and its failure
// is a tested path, so a real connection attempt would make the suite depend on the
// very substrate whose absence this pager exists to survive.
let _leaseBoardWriter = null
exports._setLeaseBoardWriter = function (fn) { _leaseBoardWriter = fn }

let _consecutiveLeaseErrors = 0
let _leaseErrorFirstAt = null
let _leaseErrorLastMsg = ''
let _leaseErrorPageSent = false
let _lastLeaseOkAt = 0

exports._getLeaseErrorState = function () {
  return {
    consecutive: _consecutiveLeaseErrors,
    firstAt: _leaseErrorFirstAt,
    lastMsg: _leaseErrorLastMsg,
    paged: _leaseErrorPageSent,
    lastOkAt: _lastLeaseOkAt,
  }
}
exports._resetLeaseErrorState = function (o) {
  o = o || {}
  _consecutiveLeaseErrors = o.consecutive || 0
  _leaseErrorFirstAt = o.firstAt === undefined ? null : o.firstAt
  _leaseErrorLastMsg = o.lastMsg || ''
  _leaseErrorPageSent = !!o.paged
  _lastLeaseOkAt = o.lastOkAt || 0
}

// An exception's message is NOT reliably present. The live 2026-09-06 log carries
// `[scheduler] leaseDueRows error: ` with an EMPTY message, so a detector that keys
// on message content reads a real outage as no error at all. Normalise once, here.
function _leaseErrDetail(errMsg) {
  if (errMsg == null) return '(exception carried an empty message)'
  // `errMsg.message || errMsg` is the tempting form and it is WRONG for exactly the
  // shape this exists for: an Error whose message is '' is falsy, so it falls back to
  // String(err) = 'Error' and the empty message is laundered into a plausible-looking
  // word. Read .message when the property EXISTS, however empty it is.
  const raw = (typeof errMsg === 'object' && 'message' in errMsg) ? errMsg.message : errMsg
  const s = String(raw == null ? '' : raw).trim()
  return s || '(exception carried an empty message)'
}

// Durable, LOCAL, and unable to throw. This is the leg that survives the DB being
// the cause. Guarded at every step: a breadcrumb failure must never take down the
// dispatch loop it is reporting on.
function _writeLeaseBreadcrumb(payload) {
  try {
    const fs = require('fs')
    const path = require('path')
    try { fs.mkdirSync(path.dirname(LEASE_BREADCRUMB_FILE), { recursive: true }) } catch (_e) {}
    const tmp = LEASE_BREADCRUMB_FILE + '.tmp'
    fs.writeFileSync(tmp, JSON.stringify(payload, null, 2))
    fs.renameSync(tmp, LEASE_BREADCRUMB_FILE)   // atomic: no torn read by a hook
    return true
  } catch (_e) { return false }
}
function _readLeaseBreadcrumb() {
  try { return JSON.parse(require('fs').readFileSync(LEASE_BREADCRUMB_FILE, 'utf8')) }
  catch (_e) { return null }
}
function _clearLeaseBreadcrumb() {
  try { require('fs').unlinkSync(LEASE_BREADCRUMB_FILE) } catch (_e) {}
}
exports._readLeaseBreadcrumb = _readLeaseBreadcrumb
exports._clearLeaseBreadcrumb = _clearLeaseBreadcrumb

// Best-effort board write. Never throws, never awaited on the dispatch path.
// Returns true only when a row actually landed, so the breadcrumb is cleared on
// proof rather than on intent.
exports.writeLeaseOutageBoardRow = async function writeLeaseOutageBoardRow(crumb) {
  if (!crumb) return false
  if (_leaseBoardWriter) return await _leaseBoardWriter(crumb)
  try {
    const started = crumb.first_error_at ? new Date(crumb.first_error_at).toISOString() : 'unknown'
    const ctx = 'Scheduler lease path threw on ' + crumb.consecutive +
      ' consecutive passes starting ' + started +
      '. No rows leased for that whole period, so every cron and every worker was stopped. ' +
      'Last exception: ' + String(crumb.last_error || '').slice(0, 300) +
      (crumb.recovered_at ? '. Leasing recovered ' + crumb.recovered_at + '.' : '. STILL STALLED at write time.')
    await getPool().query(
      `INSERT INTO status_board (entity_type, entity_ref, name, status, next_action, next_action_by, context, priority, last_touched)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8, now())`,
      ['incident', 'ecodiaos-scheduler',
        'Scheduler lease path threw: fleet dispatch stalled',
        crumb.recovered_at ? 'active' : 'blocked',
        'Re-derive the cause of the lease exception and confirm dispatch is flowing (brief-file histogram per UTC hour, not last_run_at)',
        'ecodiaos', ctx, 1]
    )
    return true
  } catch (e) {
    process.stderr.write('[scheduler] lease outage board write failed (held in breadcrumb): ' +
      ((e && e.message) || e) + '\n')
    return false
  }
}

// Called from the dispatch loop's leaseDueRows catch. Counts consecutive throws
// and, at the threshold, fires exactly one page for this outage. PURE apart from
// the pager seam and the local breadcrumb, so a test drives it with no DB and no
// send. Returns the constructed page (or null) so tests assert on it.
exports.noteLeaseError = function noteLeaseError(errMsg, nowMs) {
  const now = nowMs || Date.now()
  _consecutiveLeaseErrors += 1
  if (_leaseErrorFirstAt === null) _leaseErrorFirstAt = now
  _leaseErrorLastMsg = _leaseErrDetail(errMsg)

  const crumb = {
    kind: 'scheduler_lease_outage',
    consecutive: _consecutiveLeaseErrors,
    first_error_at: new Date(_leaseErrorFirstAt).toISOString(),
    last_error_at: new Date(now).toISOString(),
    last_error: _leaseErrorLastMsg,
    threshold: LEASE_ERROR_PAGE_THRESHOLD,
    paged: _leaseErrorPageSent,
    pid: process.pid,
  }
  _writeLeaseBreadcrumb(crumb)

  if (_consecutiveLeaseErrors < LEASE_ERROR_PAGE_THRESHOLD || _leaseErrorPageSent) return null

  // Optimistic latch, unlatched by a non-zero send, exactly as noteTransientDefer.
  _leaseErrorPageSent = true
  crumb.paged = true
  _writeLeaseBreadcrumb(crumb)

  const stalledMin = Math.round((now - _leaseErrorFirstAt) / 60000)
  const msg = 'FLEET STALLED: the scheduler lease path has thrown on ' +
    _consecutiveLeaseErrors + ' consecutive passes (~' + stalledMin + 'min). ' +
    'No rows are being leased, so every cron and every worker is stopped. ' +
    'The agent process is ALIVE, so no crash or wedge alert will fire. ' +
    'Cause: ' + _leaseErrorLastMsg.slice(0, 140)
  // --urgency critical is load-bearing: see noteTransientDefer. The fyi tier lints
  // for operator vocabulary and refused the DISPATCH OUTAGE page 62 times over four
  // days on the word "DISPATCH" alone.
  const args = ['--from', 'scheduler lease watchdog', '--urgency', 'critical', msg]
  _pagerSender(TEXT_TATE_SCRIPT, args, function (err, code) {
    if (err || code !== 0) {
      _leaseErrorPageSent = false
      process.stderr.write('[scheduler] LEASE OUTAGE PAGE send FAILED (code=' + code +
        (err ? ', err=' + (err.message || err) : '') + '); unlatched, will retry next pass\n')
    } else {
      process.stderr.write('[scheduler] LEASE OUTAGE PAGE delivered (text-tate exit 0); latched for this outage\n')
    }
  })
  // Independent leg. A pooler-caused outage cannot write this, which is the whole
  // reason the page above does not depend on it.
  exports.writeLeaseOutageBoardRow(crumb).then(function (ok) {
    if (ok) process.stderr.write('[scheduler] lease outage board row written\n')
  }).catch(function () {})
  process.stderr.write('[scheduler] LEASE OUTAGE PAGE fired after ' + _consecutiveLeaseErrors +
    ' consecutive lease exceptions (~' + stalledMin + 'min stalled)\n')
  return { command: 'node ' + TEXT_TATE_SCRIPT + ' ' + args.map(a => JSON.stringify(a)).join(' '), args, message: msg }
}

// Called after a lease pass RETURNS (the only proof leasing works). Resets the
// tracker and flushes a held breadcrumb to the board, which is how a DB-caused
// outage still gets its row. Cannot throw: it runs on the hot dispatch path.
exports.noteLeaseOk = function noteLeaseOk(nowMs) {
  const now = nowMs || Date.now()
  _lastLeaseOkAt = now
  if (_consecutiveLeaseErrors === 0 && !_leaseErrorPageSent) return null
  const recovered = {
    consecutive: _consecutiveLeaseErrors,
    paged: _leaseErrorPageSent,
    firstAt: _leaseErrorFirstAt,
    lastMsg: _leaseErrorLastMsg,
  }
  process.stderr.write('[scheduler] lease path recovered; clearing lease-outage tracker (was ' +
    _consecutiveLeaseErrors + ' consecutive errors, paged=' + _leaseErrorPageSent + ')\n')
  const held = _readLeaseBreadcrumb()
  _consecutiveLeaseErrors = 0
  _leaseErrorFirstAt = null
  _leaseErrorLastMsg = ''
  _leaseErrorPageSent = false
  if (held && held.paged) {
    held.recovered_at = new Date(now).toISOString()
    exports.writeLeaseOutageBoardRow(held).then(function (ok) {
      if (ok) { _clearLeaseBreadcrumb(); process.stderr.write('[scheduler] held lease-outage breadcrumb flushed to status_board\n') }
    }).catch(function () {})
  } else {
    _clearLeaseBreadcrumb()
  }
  return recovered
}

// ── Runaway dispatch circuit breaker (Hunter 4 #7, 2026-08-13) ────────────────
//
// Survival-critical. The ONLY pre-existing protection is account-level: the usage
// poller auto-switches at 90% and the 20B/wk cap is a soft ceiling. Nothing kills a
// per-RUN runaway. A wedged retry loop, or a self-re-arming chain (continuity-advance-
// chain has 371 rows accreted), can spawn worker tab after worker tab and drain the
// whole weekly token budget mid-trip with no per-run brake. Two independent brakes,
// both set FAR above normal fleet rate so honest multi-worker operation is never
// falsely frozen, and every count is logged:
//
//   1. Rolling-window aggregate breaker. If total dispatches in the last
//      BREAKER_WINDOW_MS reach BREAKER_MAX_DISPATCHES, FREEZE all new dispatch and
//      page Tate ONCE. Auto-clears when the window rolls (old timestamps age out
//      below the cap) or when the operator touches BREAKER_RESET_FILE.
//   2. Same-name re-dispatch guard. If ONE task name has already dispatched
//      BREAKER_SAMENAME_MAX times within BREAKER_SAMENAME_WINDOW_MS, QUARANTINE that
//      row (last_status='paused' - the schedule_pause exclusion leaseDueRows honours)
//      and page once. The one looping row is stopped; the rest of the fleet flows.
//
// Both brakes reuse the delivery-proven _pagerSender seam (text-tate exit-0 confirmed)
// so a test asserts on the constructed page without ever texting Tate. All thresholds
// are env-tunable. The decision functions are PURE (in-memory + pager only, no DB) so
// they unit-test with an injected nowMs; the DB quarantine lives in quarantineRow.

const BREAKER_WINDOW_MS = parseInt(process.env.SCHEDULER_BREAKER_WINDOW_MS, 10) || 60 * 60 * 1000
const BREAKER_MAX_DISPATCHES = parseInt(process.env.SCHEDULER_BREAKER_MAX_DISPATCHES, 10) || 150
const BREAKER_SAMENAME_WINDOW_MS = parseInt(process.env.SCHEDULER_BREAKER_SAMENAME_WINDOW_MS, 10) || 10 * 60 * 1000
const BREAKER_SAMENAME_MAX = parseInt(process.env.SCHEDULER_BREAKER_SAMENAME_MAX, 10) || 5
const BREAKER_RESET_FILE = process.env.SCHEDULER_BREAKER_RESET_FILE ||
  '/Users/ecodia/.ecodiaos/coordination/breaker/RESET'

let _dispatchTimes = []            // ms timestamps of recent dispatches (aggregate window)
let _nameDispatchTimes = new Map() // name -> [ms timestamps] (same-name window)
let _breakerFrozen = false         // aggregate freeze latch
let _breakerPageSent = false       // one aggregate page per freeze episode (unlatched on send failure)
let _quarantinedNames = new Set()  // names paged after quarantine (one page each, per process)

// Drop timestamps older than the window. Returns a new array (or the same one if
// nothing aged out) so callers can reassign cheaply.
function _pruneWindow(arr, nowMs, windowMs) {
  const cutoff = nowMs - windowMs
  let i = 0
  while (i < arr.length && arr[i] < cutoff) i++
  return i > 0 ? arr.slice(i) : arr
}

// Operator escape hatch: presence of the reset file force-clears the window and
// unfreezes. Consumed (unlinked) so it is one-shot. All fs access is guarded so a
// missing dir / permission error can never crash the dispatch pass.
function _consumeBreakerResetFile() {
  try {
    const fs = require('fs')
    if (fs.existsSync(BREAKER_RESET_FILE)) {
      try { fs.unlinkSync(BREAKER_RESET_FILE) } catch (_e) {}
      return true
    }
  } catch (_e) {}
  return false
}

// Fire one breaker page via the shared, delivery-proven sender. `latched` mirrors
// the outage pager: on send failure it unlatches _breakerPageSent so the next frozen
// tick retries (aggregate freeze); the same-name page passes latched=false because
// the quarantine itself is the real protection and one attempt is enough. Returns the
// constructed command so tests assert on it without spawning text-tate.js.
function _fireBreakerPage(msg, latched) {
  // --urgency critical: see noteTransientDefer. Same last-resort class.
  const args = ['--from', 'runaway breaker', '--urgency', 'critical', msg]
  _pagerSender(TEXT_TATE_SCRIPT, args, function (err, code) {
    if (err || code !== 0) {
      if (latched) _breakerPageSent = false
      process.stderr.write('[scheduler] BREAKER PAGE send FAILED (code=' + code +
        (err ? ', err=' + (err.message || err) : '') + ')' +
        (latched ? '; unlatched, will retry next frozen tick' : '') + '\n')
    } else {
      process.stderr.write('[scheduler] BREAKER PAGE delivered (text-tate exit 0)' +
        (latched ? '; latched for this freeze' : '') + '\n')
    }
  })
  return { command: 'node ' + TEXT_TATE_SCRIPT + ' ' + args.map(a => JSON.stringify(a)).join(' '), args, message: msg }
}

// Same-name re-dispatch guard (PURE). Prunes the name's window and returns
// { allow, reason, count }. allow:false means the name has ALREADY dispatched
// >= BREAKER_SAMENAME_MAX times in the window (a "more than N in M min" loop) and the
// caller must quarantine the row. Does NOT record the current attempt - the caller
// records via breakerRecordDispatch only when BOTH brakes allow.
exports.breakerCheckSameName = function breakerCheckSameName(name, nowMs) {
  nowMs = (typeof nowMs === 'number') ? nowMs : Date.now()
  if (!name) return { allow: true, count: 0 }
  const arr = _pruneWindow(_nameDispatchTimes.get(name) || [], nowMs, BREAKER_SAMENAME_WINDOW_MS)
  _nameDispatchTimes.set(name, arr)
  if (arr.length >= BREAKER_SAMENAME_MAX) {
    return { allow: false, reason: 'same-name-loop', count: arr.length, name }
  }
  return { allow: true, count: arr.length, name }
}

// Rolling-window aggregate breaker (PURE). Honours the operator reset file, prunes the
// window, and returns { allow, reason, count, page }. allow:false FREEZES dispatch. The
// first frozen check of an episode fires exactly one page (latched); subsequent frozen
// checks stay silent until the window rolls back under the cap, which clears the latch.
exports.breakerCheckAggregate = function breakerCheckAggregate(nowMs) {
  nowMs = (typeof nowMs === 'number') ? nowMs : Date.now()
  if (_consumeBreakerResetFile()) {
    process.stderr.write('[scheduler] BREAKER manual reset file consumed; clearing window + unfreezing\n')
    _dispatchTimes = []
    _breakerFrozen = false
    _breakerPageSent = false
  }
  _dispatchTimes = _pruneWindow(_dispatchTimes, nowMs, BREAKER_WINDOW_MS)
  if (_dispatchTimes.length >= BREAKER_MAX_DISPATCHES) {
    const firstFreeze = !_breakerFrozen
    _breakerFrozen = true
    let page = null
    if (!_breakerPageSent) {
      _breakerPageSent = true
      const windowMin = Math.round(BREAKER_WINDOW_MS / 60000)
      const msg = 'RUNAWAY DISPATCH BREAKER TRIPPED: ' + _dispatchTimes.length +
        ' dispatches in the last ' + windowMin + 'min (cap ' + BREAKER_MAX_DISPATCHES +
        '). New dispatch FROZEN to protect the weekly token budget. Auto-clears when the ' +
        'window rolls; force-resume by touching ' + BREAKER_RESET_FILE + '. Check for a ' +
        'wedged retry loop or a self-re-arming chain.'
      page = _fireBreakerPage(msg, true)
    }
    process.stderr.write('[scheduler] BREAKER FROZEN: ' + _dispatchTimes.length +
      ' dispatches in ' + Math.round(BREAKER_WINDOW_MS / 60000) + 'min >= cap ' +
      BREAKER_MAX_DISPATCHES + (firstFreeze ? ' (first freeze this episode)' : '') + '\n')
    return { allow: false, reason: 'window-freeze', count: _dispatchTimes.length, page }
  }
  if (_breakerFrozen) {
    process.stderr.write('[scheduler] BREAKER CLEARED: window rolled to ' + _dispatchTimes.length +
      ' < cap ' + BREAKER_MAX_DISPATCHES + '; dispatch resumes\n')
    _breakerFrozen = false
    _breakerPageSent = false
  }
  return { allow: true, count: _dispatchTimes.length }
}

// Record a dispatch that passed BOTH brakes, into the aggregate and same-name windows.
exports.breakerRecordDispatch = function breakerRecordDispatch(name, nowMs) {
  nowMs = (typeof nowMs === 'number') ? nowMs : Date.now()
  _dispatchTimes = _pruneWindow(_dispatchTimes, nowMs, BREAKER_WINDOW_MS)
  _dispatchTimes.push(nowMs)
  const arr = _pruneWindow(_nameDispatchTimes.get(name) || [], nowMs, BREAKER_SAMENAME_WINDOW_MS)
  arr.push(nowMs)
  _nameDispatchTimes.set(name, arr)
  process.stderr.write('[scheduler] breaker: recorded dispatch "' + name + '" (window=' +
    _dispatchTimes.length + '/' + BREAKER_MAX_DISPATCHES + ', name=' + arr.length + '/' +
    BREAKER_SAMENAME_MAX + ')\n')
  return { window: _dispatchTimes.length, name: arr.length }
}

// Fire exactly one quarantine page for a name (idempotent per process). Best-effort
// (latched=false) - the DB pause in quarantineRow is the real protection.
exports.breakerNoteQuarantine = function breakerNoteQuarantine(name, count) {
  if (_quarantinedNames.has(name)) return null
  _quarantinedNames.add(name)
  const windowMin = Math.round(BREAKER_SAMENAME_WINDOW_MS / 60000)
  const msg = 'RUNAWAY TASK QUARANTINED: "' + name + '" dispatched ' + count +
    ' times in ' + windowMin + 'min (limit ' + BREAKER_SAMENAME_MAX + '). Row PAUSED ' +
    '(last_status=paused) to stop the loop and protect the token budget. Resume with ' +
    'schedule_resume once the cause is fixed.'
  return _fireBreakerPage(msg, false)
}

// DB action for the same-name guard: pause the runaway row so leaseDueRows stops
// picking it, release any lease, and record why. Separated from dispatchOne so it
// integration-tests against a stub pool. Never throws to the caller - a DB hiccup
// here must not crash the dispatch pass (the aggregate breaker is the fleet-wide
// backstop).
//
// status = 'paused', NOT 'active' (changed 2026-08-26, task daead163). It wrote
// last_status='paused' with status='active' to mirror schedule_pause, and that one
// word made this the most invisible failure in the fleet: leaseDueRows excludes
// last_status IN ('paused','cancelled'), so a quarantined row NEVER fires again,
// while every ordinary status query reads it as a healthy active row. On
// 2026-08-26 that hid 35 silently-dead rows for hours, including gmail-inbox-poll,
// which left inbound client mail dark. Two nets missed them for the same reason:
// scheduler-spawn-fail-guard.sh deliberately skips status='active' rows as 'still
// owned by the live dispatcher', and scheduler-lease-stall-canary.cjs excludes
// last_status='paused' as deliberate. Both were right about the field they read
// and wrong about the row. 'paused' is a valid status enum value (the CHECK
// constraint allows active/paused/cancelled/dispatching/running/completed/failed/
// orphaned) and schedule_resume clears BOTH columns, so a quarantine is now
// visible in any status query and still resumable by the normal path.
exports.quarantineRow = async function quarantineRow(pool, row, count) {
  const reason = 'runaway breaker: same-name re-dispatch guard quarantined this row (' +
    count + ' dispatches in ' + Math.round(BREAKER_SAMENAME_WINDOW_MS / 60000) +
    'min); resume with schedule_resume after fixing the loop'
  try {
    await pool.query(
      `UPDATE os_scheduled_tasks
       SET last_status = 'paused', status = 'paused',
           leased_by = NULL, leased_at = NULL,
           last_error = $2, updated_at = NOW()
       WHERE id = $1`,
      [row.id, reason]
    )
    return { ok: true }
  } catch (e) {
    process.stderr.write('[scheduler] quarantineRow UPDATE failed for ' + row.id + ': ' +
      (e && e.message || e) + '\n')
    return { ok: false, error: (e && e.message) || String(e) }
  }
}

exports._getBreakerState = function () {
  return {
    dispatchCount: _dispatchTimes.length,
    frozen: _breakerFrozen,
    pageSent: _breakerPageSent,
    quarantined: Array.from(_quarantinedNames),
    nameCounts: Object.fromEntries(Array.from(_nameDispatchTimes.entries()).map(([k, v]) => [k, v.length])),
  }
}
exports._resetBreakerState = function () {
  _dispatchTimes = []
  _nameDispatchTimes = new Map()
  _breakerFrozen = false
  _breakerPageSent = false
  _quarantinedNames = new Set()
}
exports._breakerConfig = function () {
  return {
    windowMs: BREAKER_WINDOW_MS,
    maxDispatches: BREAKER_MAX_DISPATCHES,
    sameNameWindowMs: BREAKER_SAMENAME_WINDOW_MS,
    sameNameMax: BREAKER_SAMENAME_MAX,
    resetFile: BREAKER_RESET_FILE,
  }
}

// ── buildBrief ───────────────────────────────────────────────────────────────
//
// Composes the brief pasted into the spawned CC chat tab. Workers MUST call
// coord.signal_bound as their first action (releases the scheduler launch-lock)
// and coord.signal_done when the task is complete.

exports.buildBrief = function buildBrief(row) {
  const taskId = row.id || row.task_id || 'unknown'
  // No account line in the brief. The scheduler no longer rotates accounts
  // (2026-06-29 consolidation, see the dispatchOne comment block); it dispatches
  // on whatever account is already live. A brief that said 'Account rotated to: X'
  // was therefore always false - nothing was rotated. Removed 2026-08-12 (Tate).
  const orphanHours = Math.round(ORPHAN_TIMEOUT_MS / (60 * 60 * 1000))

  const lines = []

  // 2026-06-10 branch-thrash guard. When the dispatcher allocated an isolated
  // worktree for this row, the brief carries the path so the worker operates
  // there and never touches the conductor's shared tree. The hook in the
  // shared tree's .git/hooks/reference-transaction is the backstop if the
  // worker still tries.
  if (row.worktree_path) {
    lines.push(
      'WORKTREE: ' + row.worktree_path,
      '',
      'Your isolated working tree is at the path above. Use ABSOLUTE paths under',
      'it for every file operation. Run all git commands with',
      '  git -C ' + row.worktree_path + ' ...',
      'or by first changing into that directory.',
      '',
      'Do NOT operate on /Users/ecodia/.code/ecodiaos/backend - that is the',
      "conductor's shared working tree and its reference-transaction hook will",
      'reject branch flips there (see backend/patterns/branch-thrash-guard-on-',
      'shared-tree-2026-06-10.md). The dispatcher prunes this worktree on your',
      'signal_done.',
      '',
    )
  }

  lines.push(
    // FIRST instruction: signal_bound with task_id so scheduler release launch-lock.
    'FIRST ACTION (mandatory, before any task work):',
    'Call coord.signal_bound now with { task_id: "' + taskId + '" } to confirm you received this brief.',
    'This releases the scheduler launch-lock and transitions the task row to status=running.',
    '',
    // Task prompt verbatim.
    'TASK:',
    row.prompt || '(no prompt set on this task row)',
    '',
    // signal_done instructions.
    'COMPLETION (mandatory):',
    'When the task is complete (success or failure), call coord.signal_done with:',
    '  { task_id: "' + taskId + '", result_summary: "<brief summary>", status: "success"|"failed", terminate: true }',
    'This updates the task row and (for cron tasks) schedules the next run.',
    '',
    'If this task has not called signal_done within ' + orphanHours + 'h it will be marked orphaned.',
  )

  return lines.join('\n')
}

// ── cron re-entry guard helpers ──────────────────────────────────────────────
//
// Root-cause defenses for
// [[cron-rearm-must-recompute-next-run-at-or-guard-reentry-per-period-2026-06-19]].
// All three are PURE and FAIL-OPEN: any parse error or missing field returns the
// "dispatch normally" answer, so the guard can never make a row un-dispatchable.
// Worst case a helper is a no-op; it can never stall the fleet.

// True iff a cron row has already run for its current scheduled period. A row is
// "due" in leaseDueRows when next_run_at <= NOW(); if any path clobbers next_run_at
// into the past while last_run_at still points at the run that already happened this
// period, the row would re-dispatch every poll. This compares last_run_at against
// the most recent scheduled boundary at/before `now`: a legitimately-due row (boundary
// just passed, not yet run) has last_run_at from the PRIOR period (strictly before the
// boundary); a clobbered already-ran row has last_run_at at/after it.
exports.cronAlreadyRanThisPeriod = function cronAlreadyRanThisPeriod(row, now) {
  try {
    if (!row || row.type !== 'cron' || !row.cron_expression) return false
    if (!row.last_run_at) return false // never ran -> genuinely due
    const ref = now instanceof Date ? now : new Date()
    const tz = row.tz || 'Australia/Brisbane'
    const prevBoundary = scheduleCore.prevRun(row.cron_expression, ref, tz)
    if (!prevBoundary) return false
    const lastRun = new Date(row.last_run_at)
    if (isNaN(lastRun.getTime())) return false
    return lastRun.getTime() >= prevBoundary.getTime()
  } catch (_e) {
    return false
  }
}

// One-shot analogue of cronAlreadyRanThisPeriod. A delayed / one_shot / chain row
// has no cadence, so "already ran this period" has to be expressed against its
// single due-event instead: next_run_at IS that due-event, and last_run_at at or
// after it means markComplete already ran the row for this very firing. Re-leasing
// it is a double-run of one-shot work.
//
// Deliberately expressed as last_run_at >= next_run_at rather than as a run_count
// test. schedule_run_now RE-ARMS a completed one-shot from any terminal status and
// does NOT reset run_count (see its UPDATE in backend/mcp-servers/scheduler and
// tools/scheduler.js schedule_run_now: it sets next_run_at = NOW()), so a
// run_count > 0 refusal would break the sanctioned forced re-run. A forced re-run
// moves next_run_at to NOW(), strictly after the old last_run_at, so it passes this
// guard cleanly while a genuine re-lease off an already-served due-event does not.
//
// Pure and FAIL-OPEN like its siblings: any missing field or unparseable date
// returns the "dispatch normally" answer, so the guard can never strand a row.
exports.oneShotAlreadyRanThisDueEvent = function oneShotAlreadyRanThisDueEvent(row) {
  try {
    if (!row || row.type === 'cron') return false
    if (!row.last_run_at || !row.next_run_at) return false
    const lastRun = new Date(row.last_run_at)
    const dueAt = new Date(row.next_run_at)
    if (isNaN(lastRun.getTime()) || isNaN(dueAt.getTime())) return false
    return lastRun.getTime() >= dueAt.getTime()
  } catch (_e) {
    return false
  }
}

// Next scheduled boundary for a cron row as an ISO string. Fails open to a 1h
// fallback (mirrors the catch in staleLeaseRecovery branches 2a/3).
exports.computeNextRunAt = function computeNextRunAt(row, now) {
  const baseMs = now instanceof Date ? now.getTime() : Date.now()
  const tz = (row && row.tz) || 'Australia/Brisbane'
  const d = scheduleCore.nextRun(row && row.cron_expression, new Date(baseMs), tz)
  return d ? d.toISOString() : new Date(baseMs + 60 * 60 * 1000).toISOString()
}

// Run-count anomaly detector (observability). A cron's run_count is a LIFETIME
// counter; a value far above what the schedule could have produced since the row
// was created is the fingerprint of off-cadence re-firing (an annual cron created
// two weeks ago with run_count > 1 is impossible). Returns null for non-cron rows
// or when the period can't be derived; otherwise {anomalous, runCount, expectedMax,
// periodMs, ageMs, name, id}. Pure + fail-open.
exports.runCountAnomalyForRow = function runCountAnomalyForRow(row, now) {
  try {
    if (!row || row.type !== 'cron' || !row.cron_expression) return null
    if (!row.created_at) return null
    const ref = now instanceof Date ? now : new Date()
    const tz = row.tz || 'Australia/Brisbane'
    // Period = gap between two consecutive occurrences (interval ms, or two cron iterations).
    const periodMs = scheduleCore.periodMs(row.cron_expression, tz)
    if (!(periodMs > 0)) return null
    const ageMs = ref.getTime() - new Date(row.created_at).getTime()
    if (!(ageMs >= 0)) return null
    // Physical maximum lifetime fires: whole periods elapsed since creation, +1
    // for a possible fire on the creation boundary.
    const expectedMax = Math.floor(ageMs / periodMs) + 1
    const runCount = Number(row.run_count || 0)
    // Alarm when run_count exceeds DOUBLE the physical maximum AND the row is
    // still young (few periods elapsed). The young-row gate is deliberate: the
    // bug signature is "a newly-created cron firing impossibly often" (an annual
    // cron created two weeks ago has expectedMax=1, so run_count>=3 trips; the ten
    // 2026-06-19 smoking guns were all <=22-day-old low-frequency crons with
    // expectedMax=1). For a MATURE row the lifetime run_count is dominated by
    // history and by any cron_expression edit made mid-life (live 2026-06-19:
    // research-batch + self-evolution carry 220/153 lifetime fires against a now-
    // weekly expression, a schedule edit, not a bug), so flagging it would be
    // recurring noise. Capping at MAX_AUDIT_PERIODS keeps the canary to the
    // first ~6 periods of a cron's life, where the off-cadence bug actually shows.
    const MAX_AUDIT_PERIODS = 6
    const anomalous = expectedMax <= MAX_AUDIT_PERIODS && runCount > expectedMax * 2
    return { anomalous, runCount, expectedMax, periodMs, ageMs, name: row.name || null, id: row.id }
  } catch (_e) {
    return null
  }
}

// ── leaseDueRows ─────────────────────────────────────────────────────────────
//
// Atomically transitions up to `limit` due active rows to status=dispatching
// using FOR UPDATE SKIP LOCKED so concurrent schedulers (if any) don't
// double-dispatch the same row.
//
// "Due" means: status='active' AND next_run_at <= NOW() (or next_run_at IS NULL)
// Ordered by priority ASC (1=highest), then next_run_at ASC.

// Pool seam for tools/worker-liveness.js report(). The liveness probe reads
// os_scheduled_tasks and must not open a second pool against the same database.
exports._poolForLiveness = function _poolForLiveness() { return getPool() }

exports.leaseDueRows = async function leaseDueRows(limit) {
  const n = (typeof limit === 'number' && limit > 0) ? limit : DISPATCH_LIMIT
  const pool = getPool()
  const leaseId = 'scheduler-' + process.pid + '-' + Date.now()
  // 2026-06-01 Phase A2: exclude paused (last_status='paused') and cancelled
  // (archived_at IS NOT NULL OR last_status='cancelled') rows. User-control
  // surface for the new CRUD tools (schedule_pause/cancel/resume).
  //
  // Chain rows with chain_after set sit parked at next_run_at=NULL until the
  // parent completes (markComplete wakes them). Without the NULL-guard, the
  // existing "next_run_at IS NULL" branch would lease them immediately.
  const sql = `
    WITH due AS (
      SELECT id FROM os_scheduled_tasks d
      WHERE status = 'active'
        AND archived_at IS NULL
        -- 2026-08-28 lane-defer (coord rebuild R1). The migration-147 insert
        -- trigger supersedes PENDING siblings on a lane and never looks at
        -- 'running', so the moment a worker tab opened and its row flipped, the
        -- lane went unguarded and the next arrival opened a SECOND tab on the
        -- same client. Observed live on cowork.daycrew-lane-S1 (two running
        -- rows) and again on cowork.client-app-health-lane-v1.
        --
        -- The fix is deliberately NOT "add 'running' to the trigger's supersede
        -- clause". Cancelling a running row does not close its tab: the worker
        -- keeps burning usage and writing against a row marked cancelled. The
        -- holder is the incumbent and the new arrival is the duplicate, so the
        -- ARRIVAL defers. Deferral needs no bookkeeping and no resume sweep: the
        -- row stays status='active' and past-due, so the first tick after the
        -- lane frees leases it. That is why this is a lease-side predicate and
        -- not a status change.
        --
        -- A dead holder would otherwise pin its lane shut forever, which is what
        -- livenessReapPass exists to prevent: it settles provably-dead running
        -- rows from an OBSERVED signal (transcript mtime), so the lane reopens.
        -- Doctrine: patterns/one-live-scheduled-row-per-work-lane-2026-08-26.md
        AND NOT EXISTS (
          SELECT 1 FROM os_scheduled_tasks h
           WHERE h.id <> d.id
             AND h.archived_at IS NULL
             AND h.status IN ('running', 'dispatching')
             AND os_sched_lane_key(h.name) IS NOT NULL
             AND os_sched_lane_key(h.name) = os_sched_lane_key(d.name)
        )
        AND (last_status IS NULL OR last_status NOT IN ('paused', 'cancelled'))
        -- Austerity suppresses RECURRING crons only. One-shot delayed/chain/followup
        -- rows have no austerity band (L4 doctrine: "only one-off delayed tasks +
        -- followups" fire = delayed tasks survive at EVERY level), so the marker must
        -- never strand them. Without the type guard, a stray marker on a delayed row
        -- (a manual bulk UPDATE over-reaching, as happened 2026-06-15) excludes it
        -- forever because the cron-only austerity resume sweep can never clear it.
        AND (austerity_paused IS NOT TRUE OR type <> 'cron')
        AND (next_run_at IS NULL OR next_run_at <= NOW())
        AND (chain_after IS NULL OR next_run_at IS NOT NULL)
      ORDER BY priority ASC, next_run_at ASC NULLS FIRST
      LIMIT $1
      FOR UPDATE SKIP LOCKED
    )
    UPDATE os_scheduled_tasks t
    SET status = 'dispatching',
        leased_by = $2,
        leased_at = NOW(),
        updated_at = NOW()
    FROM due
    WHERE t.id = due.id
    RETURNING t.*
  `
  const result = await pool.query(sql, [n, leaseId])

  // Post-lease re-entry guard. The due-query above tests next_run_at <= NOW()
  // only; a row whose next_run_at was clobbered into the past while it already
  // ran this period (the staleLeaseRecovery branch-1 history, or any manual /
  // script re-arm) would re-dispatch every poll. Catch it here: recompute
  // next_run_at to the true next boundary, release the lease, and drop it from
  // this batch. This is the clobber-source-independent layer that protects the
  // fleet no matter HOW next_run_at got mangled. Only cron rows, only the few
  // just leased, so the cost is negligible. The release UPDATE carries the same
  // cancellation/pause guard as every other re-arm site, and is scoped to the
  // row WE just leased (status='dispatching' AND leased_by=$leaseId) so a
  // concurrent sweep cannot be clobbered.
  // [[cron-rearm-must-recompute-next-run-at-or-guard-reentry-per-period-2026-06-19]]
  const nowRef = new Date()
  const dispatchable = []
  for (const row of result.rows) {
    if (exports.cronAlreadyRanThisPeriod(row, nowRef)) {
      const nextRunAt = exports.computeNextRunAt(row, nowRef)
      await pool.query(
        `UPDATE os_scheduled_tasks
         SET status = 'active', next_run_at = $1,
             last_error = 'reentry-guard: already ran this period, next_run_at recomputed (clobber-safe)',
             leased_by = NULL, leased_at = NULL, updated_at = NOW()
         WHERE id = $2 AND status = 'dispatching' AND leased_by = $3
           AND archived_at IS NULL
           AND (last_status IS NULL OR last_status NOT IN ('paused', 'cancelled'))`,
        [nextRunAt, row.id, leaseId]
      )
      process.stderr.write('[scheduler] leaseDueRows: re-entry guard skipped ' + row.id +
        ' (' + (row.name || '?') + ') - already ran this period; next_run_at -> ' + nextRunAt + '\n')
      continue
    }
    // One-shot twin of the guard above. A delayed / one_shot / chain row whose
    // last_run_at is at or after its next_run_at already ran for this due-event,
    // so dispatching it now double-runs one-shot work. Settle it to 'completed'
    // rather than releasing it back to 'active': 'active' with an unchanged
    // past-due next_run_at is precisely the re-leasable state this whole fix
    // exists to remove, so it would spin every poll. A forced re-run via
    // schedule_run_now sets next_run_at = NOW(), which is strictly after the old
    // last_run_at, so it never reaches this branch. Same cancellation/pause guard
    // and same lease scoping as every other re-arm site.
    if (exports.oneShotAlreadyRanThisDueEvent(row)) {
      await pool.query(
        `UPDATE os_scheduled_tasks
         SET status = 'completed',
             last_error = 'reentry-guard: one-shot already ran this due-event, not re-dispatched',
             leased_by = NULL, leased_at = NULL, updated_at = NOW()
         WHERE id = $1 AND status = 'dispatching' AND leased_by = $2
           AND archived_at IS NULL
           AND (last_status IS NULL OR last_status NOT IN ('paused', 'cancelled'))`,
        [row.id, leaseId]
      )
      process.stderr.write('[scheduler] leaseDueRows: one-shot re-entry guard skipped ' + row.id +
        ' (' + (row.name || '?') + ') - last_run_at ' + row.last_run_at +
        ' is at/after due-event ' + row.next_run_at + '; settled to completed\n')
      continue
    }
    dispatchable.push(row)
  }
  return dispatchable
}

// ── markFailed ───────────────────────────────────────────────────────────────
//
// Called when dispatchOne encounters an error that is NOT AllAccountsCappedError.
// Increments retry_count, clears the lease, and leaves status=active so the
// row will be picked up again. After MAX_RETRY_COUNT retries, sets status=failed.

// ── consumeSuspendedSignal ───────────────────────────────────────────────────
//
// 2026-08-29 lane R1, seventh-worker verification pass. THE GUARDED TERMINAL
// UPDATE CAN MATCH ZERO ROWS, AND NOTHING INSPECTED rowCount.
//
// markComplete's cron arm and markFailed's cron-defer and retryable arms are all
// guarded on `archived_at IS NULL AND last_status NOT IN ('paused','cancelled')`.
// That guard is right for what it was written for: it stops a terminal transition
// flipping a paused or cancelled row back to status='active' (the 2026-06-19
// cancellation-durability fix). But the CONSUME added on 2026-08-29 rides inside
// those same statements, so the guard silently covers it too. On a suspended row
// the whole statement is a no-op: the status is not advanced AND done_at is not
// consumed.
//
// completionPass selects `status='running' AND done_at IS NOT NULL` and carries no
// such guard, so it re-selects that row every COMPLETION_POLL_INTERVAL_MS (5s)
// forever. Each pass calls kill_worker on the tab and wakes the chain children off
// a completion that never landed, while run_count never moves.
//
// THE STATE IS REACHABLE THROUGH A SHIPPED TOOL. schedule_pause sets
// last_status='paused' and deliberately does NOT touch status, and it refuses only
// archived rows, so pausing a task whose worker is mid-flight produces exactly
// `status='running' AND last_status='paused'`. Measured 2026-08-29 on the real
// table inside a rolled-back transaction: 3 completionPass passes produced 3
// kill_worker calls on the live tab, the chain child re-woken on every pass, and
// run_count frozen at 0. A negative control with a clean last_status completed and
// consumed normally on the first pass.
//
// This consume touches ONLY the lifecycle columns. It never writes status,
// next_run_at or last_status, so it cannot resurrect a paused or cancelled row -
// and leaseDueRows gates on `last_status NOT IN ('paused','cancelled')`
// independently of status, so the suspension holds regardless.
//
// RESIDUAL, named rather than hidden: the row is left at status='running' with its
// lease. What this removes is the 5-second storm, not the half-state.
//
// CORRECTED 2026-08-29 (lane D1 pass 4). This block used to name staleLeaseRecovery
// branch 3 as the designed recovery for that half-state at ORPHAN_TIMEOUT_MS. It is
// not, and never was. Branch 3's SELECT carries no last_status filter so it DOES
// select the row, but both of its UPDATEs re-assert
// `last_status NOT IN ('paused','cancelled')`, so every pass probes liveness, fires
// kill_worker on the tab and prunes the worktree, then no-ops the write. The row
// never settles, and while it sits in 'running' it holds its LANE against every
// future row on that lane. The real recovery is staleLeaseRecovery branch 0
// (suspended-lease settle), which settles the row to status=last_status once it is
// past SUSPENDED_LEASE_SETTLE_MS with no live worker.
async function consumeSuspendedSignal(pool, rowId) {
  await pool.query(
    `UPDATE os_scheduled_tasks
     SET ${taskSignals.CLEAR_SQL_FRAGMENT}, updated_at = NOW()
     WHERE id = $1`,
    [rowId]
  )
}

exports.markFailed = async function markFailed(row, err) {
  const pool = getPool()
  // 2026-08-29 lane D1. The capped defer now WRITES retry_count so
  // scheduler-health.sh:103's capped_churn detector can match at all. That
  // borrows a column whose other meaning is the failure budget, and a cap outage
  // defers every due row roughly once a minute, so an inherited count would hand
  // the first real error after the outage a row already at MAX_RETRY_COUNT and
  // permanently fail a one-shot row on try one. That correlation is not
  // hypothetical: nothing dispatches during a cap, so the moment it clears is a
  // thundering herd, which is exactly when a transient dispatch error is likeliest.
  // A count still carrying the cap marker is a cap signal, not a spent budget.
  const priorRetryCount = exports.isAllAccountsCappedMarker(row.last_error) ? 0 : (row.retry_count || 0)
  const newRetryCount = priorRetryCount + 1
  const errMsg = String(err && err.message || err).slice(0, 2000)

  // 2026-06-10 branch-thrash guard cleanup: drop the per-row isolated worktree.
  // Even on cron-defer-to-next-interval the worktree should be cleaned: the
  // next fire will allocate a fresh one off origin/main (the FF history may
  // have advanced) and idempotent allocate would tolerate either path.
  //
  // 2026-08-26 (status_board 2c95a094): GATED on coord worker liveness, exactly
  // as staleLeaseRecovery branches 2a/2b/3 already are. This was the ONE
  // unconditional prune of the six call sites, and it sits at the TOP of
  // markFailed, before the cron-defer branch. markFailed fires on ANY dispatchOne
  // error that is not AllAccountsCappedError, and several of those are raised
  // while a dispatched worker is alive and mid-run INSIDE that very worktree, so
  // the cleanup deleted a live worker's tree out from under it and its
  // uncommitted work with it.
  //
  // A prune is a cleanup, never urgent. Skipping one leaks a directory that
  // reapOrphanWorktrees later collects, which is strictly cheaper than
  // destroying in-flight work. hasLiveWorkerForTask fails OPEN (returns null when
  // coord is unreachable), so a coord outage preserves the previous behaviour
  // rather than leaking every tree.
  try {
    const liveWorker = await hasLiveWorkerForTask(String(row.id), PROTECT_LIVE_WORKERS)  // a prune is never urgent; skipping one leaks a dir reapOrphanWorktrees collects
    if (liveWorker) {
      process.stderr.write(
        '[scheduler] task ' + row.id + ' has live worker ' + liveWorker.tab_id +
        ' (stale_ms=' + liveWorker.stale_ms + '), skipping markFailed worktree prune\n'
      )
    } else {
      await exports.pruneWorktreeForRow(row)
    }
  } catch (_e) {}
  if (newRetryCount >= MAX_RETRY_COUNT) {
    // 2026-06-02 P0 fix. Cron rows MUST NOT permanently die after 3 retries -
    // that loses every future interval of recurring work. Defer to the next
    // cron interval and reset retry_count instead. Non-cron rows still
    // permanently fail (one-shot work is genuinely done after 3 failed tries).
    if (row.type === 'cron' && row.cron_expression) {
      let nextRunAt = null
      try {
        nextRunAt = exports.computeNextRunAt(row)
      } catch (_e) {
        nextRunAt = new Date(Date.now() + 60 * 60 * 1000).toISOString()
      }
      const deferRes = await pool.query(
        `UPDATE os_scheduled_tasks
         SET status = 'active', retry_count = 0, last_error = $1, next_run_at = $2,
             leased_by = NULL, leased_at = NULL,
             ${taskSignals.CLEAR_SQL_FRAGMENT}, updated_at = NOW()
         WHERE id = $3
           AND archived_at IS NULL
           AND (last_status IS NULL OR last_status NOT IN ('paused', 'cancelled'))`,
        [errMsg + ' (cron: deferred to next interval, retry_count reset)', nextRunAt, row.id]
      )
      if (!deferRes.rowCount) await consumeSuspendedSignal(pool, row.id)
    } else {
      await pool.query(
        `UPDATE os_scheduled_tasks
         SET status = 'failed', retry_count = $1, last_error = $2,
             leased_by = NULL, leased_at = NULL,
             ${taskSignals.CLEAR_SQL_FRAGMENT}, updated_at = NOW()
         WHERE id = $3`,
        [newRetryCount, errMsg, row.id]
      )
    }
  } else {
    // 2026-08-26 re-lease-while-live fix (status_board 76393b47). This retryable
    // branch was the ONE re-arm path of four that left next_run_at untouched.
    // Its three siblings all recompute: staleLeaseRecovery branch 1 uses the same
    // bounded RETRY_BACKOFF_MS below, branch 2a and the cron arm above use
    // computeNextRunAt. Leaving next_run_at at its original (now past-due) value
    // put the row straight back into leaseDueRows' due-window on the very next
    // 30s poll, WHILE the worker spawned by the failed dispatch was still alive
    // and working: a second tab opens on the same task_id and both run the brief.
    // Live evidence 2026-08-26, task 1acff6af (cowork.seedtree-lane-S2b-enrichment
    // -build, a lane that WRITES to a client production database): next_run_at
    // frozen at 12:20:00Z with last_run_at NULL for the whole 50min run, tab
    // ...f1fe7f54 dispatched 12:23:12Z, tab ...19c302c6 dispatched 13:12:38Z on
    // the SAME row, final run_count=2 retry_count=2 on a ONE-SHOT row. The frozen
    // next_run_at is what discriminates this path from branch 1, which would have
    // overwritten it with NOW()+backoff.
    //
    // The leaseDueRows re-entry guard could not catch it: cronAlreadyRanThisPeriod
    // returns false for type!=='cron' AND returns false when last_run_at is NULL,
    // and this row was both. Fixing the re-arm at source is the layer that holds
    // for one-shot rows.
    //
    // Bounded backoff (not computeNextRunAt) is deliberate and matches branch 1:
    // it preserves up to MAX_RETRY_COUNT quick re-attempts for a transient bind
    // failure while bounding re-dispatch to once per window. A one-shot row has
    // no next interval to defer to, so the full-interval recompute has no meaning
    // here. Doctrine:
    // [[cron-rearm-must-recompute-next-run-at-or-guard-reentry-per-period-2026-06-19]].
    const retryRes = await pool.query(
      `UPDATE os_scheduled_tasks
       SET status = 'active', retry_count = $1, last_error = $2,
           next_run_at = NOW() + ($4 || ' milliseconds')::interval,
           leased_by = NULL, leased_at = NULL,
           ${taskSignals.CLEAR_SQL_FRAGMENT}, updated_at = NOW()
       WHERE id = $3
         AND archived_at IS NULL
         AND (last_status IS NULL OR last_status NOT IN ('paused', 'cancelled'))`,
      [newRetryCount, errMsg, row.id, RETRY_BACKOFF_MS]
    )
    if (!retryRes.rowCount) await consumeSuspendedSignal(pool, row.id)
  }
}

// ── isTransientBridgeError ───────────────────────────────────────────────────
//
// 2026-06-20 autonomy audit finding (c). The dispatch_worker editor.open path
// can fail not only with "no IDE instances registered" (no IDE bound) but with
// a socket-class error when the IDE bridge blips mid-open: "populate failed
// (editor.open): socket hang up", ECONNRESET, ECONNREFUSED, ETIMEDOUT, EPIPE.
// All are TRANSIENT infra unavailability, not a task fault. Before this fix only
// the literal "no IDE instances registered" string got the gentle 5min defer;
// the socket-class errors fell through to markFailed, which PERMANENTLY fails a
// one-shot / delayed row once retry_count hits MAX (silent loss of scheduled
// work - the self-scheduling backbone). Classify all of them as transient so
// dispatchOne defers instead of failing.
const _TRANSIENT_BRIDGE_RE = /(no IDE instances registered|populate failed|editor\.open|socket hang up|ECONNRESET|ECONNREFUSED|ETIMEDOUT|EHOSTUNREACH|EPIPE|socket disconnected|bridge (?:unavailable|timeout))/i
exports.isTransientBridgeError = function isTransientBridgeError(msg) {
  return _TRANSIENT_BRIDGE_RE.test(String(msg || ''))
}

// ── dispatchOne ──────────────────────────────────────────────────────────────
//
// Dispatches a single leased row. Acquires launch-lock BEFORE cred rotation,
// releases it AFTER coord signal_bound is received (or timeout).
//
// Invariant: launch-lock is released in the finally block even if an error
// is thrown at any point.

exports.dispatchOne = async function dispatchOne(row) {
  const pool = getPool()
  // Self-lease-steal shield (see _inFlightDispatchIds above): register BEFORE
  // queueing on the launch-lock so staleLeaseRecovery cannot reclaim this row
  // while it waits its turn, and skip outright if a duplicate dispatch for the
  // same row is already in flight in this process.
  const inFlightKey = String(row.id)
  if (_inFlightDispatchIds.has(inFlightKey)) {
    process.stderr.write('[scheduler] dispatchOne: row ' + row.id +
      ' already in-flight in this process; skipping duplicate dispatch\n')
    return
  }
  _inFlightDispatchIds.add(inFlightKey)
  const release = await launchLockAcquire()
  let account = null
  let tabId = null
  try {
    // 0. Defense-in-depth austerity gate. leaseDueRows already excludes
    // austerity_paused rows, but a 2026-06-19 incident showed ~8 paused crons
    // (calendar-watch, opportunity-triage, neo4j-maintenance, monthly-financial-close,
    // github-push-ci-watch ...) reaching dispatchOne in a 3-min burst and spawning
    // worker tabs despite the gate (unreproducible race between lease + stale-lease
    // recovery under the signal_bound hold). The harm is a spawned CC session burning
    // tokens while austerity is meant to suppress it. Re-read the marker here, under
    // the launch-lock (fresh read), BEFORE any cred rotation / worktree / tab spawn.
    // If the row is now paused (or gone/cancelled), release the lease and skip - NOT
    // markFailed: this is a suppression, not a failure, so it must not burn a retry or
    // defer the cron off its cadence. next_run_at is left intact so the row rejoins
    // its schedule the moment austerity lifts.
    {
      const guard = await pool.query(
        `SELECT austerity_paused, status, archived_at, last_status, name, type
         FROM os_scheduled_tasks WHERE id = $1`,
        [row.id]
      )
      const g = guard.rows[0]
      // Band-aware suppression: a suppressed cron may carry NO marker (created or
      // re-registered after the last `set`), so ALSO compute the band live from
      // (name -> band, live kv level). Cron-only: one-shot delayed/followup rows
      // have no band and MUST still fire at L4 by design. null level (read failure
      // or band table absent) leaves bandSuppressed false -> proceed on the marker.
      let bandSuppressed = false
      if (_austerityCfg && g && g.type === 'cron') {
        // Posture is the single-source suppression predicate (same one the lever
        // writes markers from). If the posture key is missing/unreadable we leave
        // bandSuppressed false and proceed on the austerity_paused marker alone.
        // See readAusterityPosture for why this must not diverge from the lever.
        const posture = await readAusterityPosture(pool)
        if (posture != null) {
          bandSuppressed = _austerityCfg.decidePosture(g.name, posture).suppressed
        }
      }
      // Fire ONLY on the explicit suppression signals. A missing read (g
      // undefined) is left to proceed - the guard targets suppression state,
      // not row existence, and a genuinely-deleted row fails downstream anyway.
      const suppressed = !!g && (
        g.austerity_paused === true ||
        g.archived_at != null ||
        g.last_status === 'cancelled' ||
        bandSuppressed
      )
      if (suppressed) {
        await pool.query(
          `UPDATE os_scheduled_tasks
           -- PRE-SPAWN-BAIL: the lease is released here BEFORE any worker is spawned,
           -- so leaving the due time past-due cannot duplicate a live worker, and it is
           -- what lets the row retry promptly once the suppression lifts. This marker is
           -- the structural opt-out read by the case 7 class guard in
           -- tools/scheduler.rearm-reentry-oneshot.test.js. Do NOT add it to a statement
           -- that can run after a worker exists.
           SET status = 'active', leased_by = NULL, leased_at = NULL, updated_at = NOW()
           WHERE id = $1
             AND status = 'dispatching'
             AND leased_by IS NOT DISTINCT FROM $2`,
          [row.id, row.leased_by || null]
        )
        // Converge the marker with the live band, or this gate becomes a hot loop.
        // leaseDueRows only excludes a cron whose austerity_paused MARKER is set, so a
        // band-suppressed cron carrying no marker is re-leased on EVERY poll, re-checked
        // here, and released again, forever. Measured 2026-08-26: cowork.nwg-score-on-arrival
        // (next_run_at 30h in the past, band-suppressed, no marker) skipped 125 times in the
        // 100 minutes after the 01:57Z boot, and 28,197 of 169,876 lines in
        // eos-laptop-agent.err.log were that single row. Writing the marker the gate has
        // just computed moves the exclusion back to lease time and the spin stops.
        // Safe to write from here: scheduler-austerity.cjs applyState() recomputes
        // toPause/toResume across the WHOLE fleet from the live band, not from who set the
        // marker, so the next lever run clears this row (with its own stagger) the moment it
        // stops being suppressed. next_run_at is still left intact, so the catch-up run the
        // original comment wanted is preserved.
        let markerWritten = false
        if (bandSuppressed && g.type === 'cron' && g.austerity_paused !== true) {
          const m = await pool.query(
            `UPDATE os_scheduled_tasks SET austerity_paused = true, updated_at = NOW()
             WHERE id = $1 AND type = 'cron' AND austerity_paused IS NOT TRUE`,
            [row.id]
          )
          markerWritten = m.rowCount > 0
        }
        // Distinguish the cause so the log tells us WHICH gate fired. A
        // 'band>level(no-marker)' skip means a marker-less suppressed cron was
        // caught - the exact 2026-07-15 leak class - now harmless and self-reporting.
        const cause =
          g.austerity_paused === true ? 'marker' :
          g.archived_at != null ? 'archived' :
          g.last_status === 'cancelled' ? 'cancelled' :
          bandSuppressed ? 'band>level(no-marker)' : 'unknown'
        process.stderr.write('[scheduler] dispatchOne: SKIP ' + row.id + ' (' +
          (g && g.name || row.name || '?') + ') - austerity gate [' + cause + ']; ' +
          'lease released without retry' + (markerWritten ? '; marker written (spin stopped)' : '') + '\n')
        return
      }
    }

    // 0a. CONDUCTOR CLAIM INTERLOCK (2026-08-28, lane R1 coord rebuild).
    //
    // The one duplication hole every other guard is blind to by construction.
    // Migration 147's INSERT arm, the 148/149/150 revive arm and the 2026-08-28
    // leaseDueRows NOT EXISTS defer all compare one ROW against another ROW. When
    // the conductor arms a row and then does that same work on its own thread,
    // there is no second row to compare against, so all of them stay silent.
    //
    // Measured on lane cowork.ecodiaos-lane-R1: row armed 06:16:01Z, tab opened
    // 07:07:38Z, and inside that 51-minute launch lag the conductor landed five
    // commits doing the dispatched job itself. The tab opened onto finished work.
    //
    // WHY HERE AND NOT IN leaseDueRows. The claim is normally stamped DURING the
    // launch lag, which is to say AFTER the lease. A lease-time predicate cannot
    // see a fact that does not exist yet. This is the fresh read under the
    // launch-lock, the last point before a tab is spawned, so it is the only
    // check that covers the window the incident actually happened in.
    //
    // DISPOSITION IS A TERMINAL SETTLE, NOT A PRE-SPAWN-BAIL. Releasing the lease
    // back to 'active' is right for a SUPPRESSION, which lifts. A claim does not
    // lift: the conductor is doing the work now, so the row would be re-leased and
    // re-checked on every 30s poll forever. That spin is not hypothetical in this
    // file (28,197 of 169,876 stderr lines from one row, 2026-08-26). The row
    // settles terminal, and `result` names the claiming tab so the row reads in
    // every audit as work-that-went-somewhere rather than work-that-vanished.
    //
    // FAILS OPEN, DELIBERATELY, IN ITS OWN QUERY. If the agent is restarted before
    // migration 151 applies, these columns do not exist and this SELECT throws. It
    // is kept OUT of the austerity guard's SELECT above so that failure can never
    // take the austerity gate down with it, and the catch degrades to exactly
    // today's behaviour with a loud line rather than killing every dispatch.
    // Precedent: hasLiveWorkerForTask fails open the same way.
    {
      let claim = null
      try {
        const cr = await pool.query(
          `SELECT claimed_by_tab_id, claimed_at FROM os_scheduled_tasks WHERE id = $1`,
          [row.id]
        )
        claim = cr.rows[0] || null
      } catch (e) {
        process.stderr.write('[scheduler] dispatchOne: conductor-claim check UNAVAILABLE for ' +
          row.id + ' (' + (row.name || '?') + ') - ' + e.message +
          '; proceeding (migration 151 not applied?)\n')
      }
      if (claim && claim.claimed_by_tab_id) {
        const claimedBy = String(claim.claimed_by_tab_id)
        const claimedAtMs = claim.claimed_at ? new Date(claim.claimed_at).getTime() : null
        const claimAgeMs = claimedAtMs ? (Date.now() - claimedAtMs) : null
        const claimedAtIso = claimedAtMs ? new Date(claimedAtMs).toISOString() : 'unknown time'
        const isCron = row.type === 'cron' && !!row.cron_expression

        // 1. ABANDONED CLAIM. A claim is only ever released by hand
        // (scripts/conductor-claim.cjs --release); nothing expires it. A conductor
        // that claims a row and then dies - a crashed tab, a usage cap, a machine
        // reboot - would otherwise suppress that row FOREVER, and for a cron that is
        // a silent permanent death of a recurring job. Bounded by the same 6h the
        // orphan sweep uses for a one-shot worker: past it, no conductor is still
        // plausibly holding the work, so the claim is treated as abandoned, cleared,
        // and the row dispatches normally. Fail-safe direction: the worst case is one
        // duplicate arc after 6h, versus a job that never runs again.
        if (claimAgeMs !== null && claimAgeMs > ORPHAN_TIMEOUT_MS) {
          await pool.query(
            `UPDATE os_scheduled_tasks
             SET claimed_by_tab_id = NULL, claimed_at = NULL, updated_at = NOW()
             WHERE id = $1`, [row.id]
          )
          process.stderr.write('[scheduler] dispatchOne: conductor-claim on ' + row.id + ' (' +
            (row.name || '?') + ') is ABANDONED (claimed by ' + claimedBy + ' at ' + claimedAtIso +
            ', age ' + Math.round(claimAgeMs / 60000) + 'min > ' +
            Math.round(ORPHAN_TIMEOUT_MS / 60000) + 'min); cleared, dispatching normally\n')
          // fall through to normal dispatch
        } else if (isCron) {
          // 2. CRON ROWS NEVER PERMANENTLY FAIL. Terminal-cancelling a cron here was
          // a permanent kill: leaseDueRows excludes last_status IN ('paused','cancelled'),
          // and schedule_resume refuses anything whose last_status is not 'paused', so a
          // claimed cron could not be revived by ANY exposed scheduler tool. Same class as
          // the 2026-06-09 incident that stranded 23 corpus rows, and as the quarantineRow
          // status/last_status bug of 2026-08-26. Per
          // [[scheduler-no-ide-defer-and-cron-rows-never-permanently-fail-2026-06-02]] a
          // cron DEFERS to its next interval; only one-shot rows settle terminal. The claim
          // is deliberately RETAINED so a conductor arc spanning several intervals keeps
          // suppressing the tab (that suppression is the whole point of the interlock), and
          // the abandoned-claim branch above is what stops retention becoming permanent.
          let nextRunAt
          try { nextRunAt = exports.computeNextRunAt(row) }
          catch (e) { nextRunAt = new Date(Date.now() + 60 * 60 * 1000).toISOString() }
          await pool.query(
            `UPDATE os_scheduled_tasks
             SET status = 'active',
                 last_status = NULL,
                 next_run_at = $3,
                 leased_by = NULL, leased_at = NULL,
                 result = $4,
                 updated_at = NOW()
             WHERE id = $1
               AND status = 'dispatching'
               AND leased_by IS NOT DISTINCT FROM $2`,
            [row.id, row.leased_by || null, nextRunAt,
             'claimed-by-conductor: ' + claimedBy + ' took this occurrence onto its own thread at ' +
             claimedAtIso + '; no worker tab opened, cron deferred to ' + nextRunAt]
          )
          process.stderr.write('[scheduler] dispatchOne: SKIP ' + row.id + ' (' +
            (row.name || '?') + ') - conductor-claim interlock [claimed by ' + claimedBy +
            ']; CRON DEFERRED to ' + nextRunAt + ' (claim retained, age ' +
            (claimAgeMs === null ? '?' : Math.round(claimAgeMs / 60000) + 'min') +
            '), no tab spawned\n')
          return
        } else {
          // 3. ONE-SHOT. The conductor is doing this specific job; terminal is correct.
          await pool.query(
            `UPDATE os_scheduled_tasks
             SET status = 'cancelled',
                 last_status = 'cancelled',
                 austerity_paused = false,
                 leased_by = NULL, leased_at = NULL,
                 result = $3,
                 updated_at = NOW()
             WHERE id = $1
               AND status = 'dispatching'
               AND leased_by IS NOT DISTINCT FROM $2`,
            [row.id, row.leased_by || null,
             'claimed-by-conductor: ' + claimedBy + ' took this work onto its own thread at ' +
             claimedAtIso + '; no worker tab opened']
          )
          process.stderr.write('[scheduler] dispatchOne: SKIP ' + row.id + ' (' +
            (row.name || '?') + ') - conductor-claim interlock [claimed by ' + claimedBy +
            ']; settled cancelled, no tab spawned\n')
          return
        }
      }
    }

    // 0b. Runaway circuit breaker (Hunter 4 #7, 2026-08-13). Two brakes CHECKED
    // under the launch-lock, BEFORE any cred pick / worktree / tab spawn, so a
    // runaway is stopped before it spends a single token. Like the austerity gate
    // above, a trip RELEASES the lease without a retry (a freeze/quarantine is
    // suppression, not a row failure) so cadence and retry budget are untouched.
    //
    // CHECKED here, RECORDED after the spawn (split 2026-08-26, task daead163).
    // breakerRecordDispatch used to run at the end of this block, before any tab
    // existed, so it counted ATTEMPTS rather than dispatches. A spawn outage
    // therefore manufactured quarantines out of perfectly healthy rows: the row
    // is re-leased, re-recorded, and 5 failures inside the 10min window trip a
    // guard built for runaway LOOPS. Measured 2026-08-26: a 3h02m IDE-bridge
    // outage (04:18Z to 07:20Z) quarantined 35 rows at roughly one every 7
    // minutes, each at exactly its due minute, and 33 of them had run_count=0 and
    // had never run once. The breaker exists to stop a row that keeps SPAWNING
    // workers, so a spawn that never happened must not count toward it. The
    // spawn-failure class has its own owner (scheduler-spawn-fail-guard.sh plus
    // the noteFailedDispatch outage pager); it was never this breaker's job.
    let breakerRowName = null
    {
      const nowMs = Date.now()
      const rowName = row.name || String(row.id)

      // (i) Same-name re-dispatch guard: if THIS name already looped past the limit
      // in the window, quarantine the row (pause) so it stops re-dispatching, page
      // once, and skip. Stops the one looping row; the fleet keeps flowing.
      const sn = exports.breakerCheckSameName(rowName, nowMs)
      if (!sn.allow) {
        await exports.quarantineRow(pool, row, sn.count)
        exports.breakerNoteQuarantine(rowName, sn.count)
        process.stderr.write('[scheduler] dispatchOne: QUARANTINE ' + row.id + ' (' + rowName +
          ') - same-name breaker [' + sn.count + ' in ' +
          Math.round(BREAKER_SAMENAME_WINDOW_MS / 60000) + 'min]; row paused, lease released\n')
        return
      }

      // (ii) Rolling-window aggregate breaker: freeze ALL new dispatch when the
      // fleet-wide dispatch rate reaches the cap.
      const agg = exports.breakerCheckAggregate(nowMs)
      if (!agg.allow) {
        await pool.query(
          `UPDATE os_scheduled_tasks
           -- PRE-SPAWN-BAIL: the lease is released here BEFORE any worker is spawned,
           -- so leaving the due time past-due cannot duplicate a live worker, and it is
           -- what lets the row retry promptly once the suppression lifts. This marker is
           -- the structural opt-out read by the case 7 class guard in
           -- tools/scheduler.rearm-reentry-oneshot.test.js. Do NOT add it to a statement
           -- that can run after a worker exists.
           SET status = 'active', leased_by = NULL, leased_at = NULL, updated_at = NOW()
           WHERE id = $1 AND status = 'dispatching'
             AND leased_by IS NOT DISTINCT FROM $2`,
          [row.id, row.leased_by || null]
        )
        process.stderr.write('[scheduler] dispatchOne: FROZEN ' + row.id + ' (' + rowName +
          ') - aggregate breaker [' + agg.count + ' in ' +
          Math.round(BREAKER_WINDOW_MS / 60000) + 'min >= cap ' + BREAKER_MAX_DISPATCHES +
          ']; lease released without retry\n')
        return
      }

      // Both brakes clear. The dispatch is NOT recorded here - see the
      // breakerRecordDispatch call after the confirmed tab spawn below.
      breakerRowName = rowName
    }

    // 1. Dispatch on the account that is ALREADY live. No per-dispatch rotation.
    // 2026-06-29 switcher consolidation. The Mac shares ONE Keychain, so a
    // dispatched worker and the interactive conductor are ALWAYS the same account
    // at any instant - rotating for a worker moves EVERYONE, including Tate's live
    // session. Per-dispatch rotation is therefore incoherent on this substrate
    // (it can only ever clobber, never load-balance - 2026-06-25 doctrine), and
    // it was a recurring murder path for the live account. The scheduler no longer
    // rotates at all: it dispatches the worker onto whatever account is live. The
    // ONLY sanctioned account mover is the cap-watch/autoswitch -> account-switch.sh
    // canonical re-login, gated by the operator pin (<COORD_ROOT>/usage/account-pin).
    // Origin: 2026-06-29, status_board 3b604f2e. Prior sticky half-measure: f3e097c.
    //
    // Defer-on-all-capped guard (DETECTION ONLY, no rotation). Consult
    // pick_healthiest_account purely for its side effect: it throws
    // AllAccountsCappedError when every enabled account is capped, which the catch
    // below turns into a row deferral instead of spawning a worker onto a dead
    // account. Its RETURN value is deliberately discarded - dispatch runs on the
    // live account, never a picked one.
    await getCreds().pick_healthiest_account({ required_headroom_minutes: 1 })
    const liveShort = (() => { try { return getCreds().current_account() } catch (_) { return null } })()
    account = (liveShort && liveShort !== 'unknown') ? liveShort : 'current-process'

    // 2026-06-10 branch-thrash guard: allocate isolated worktree off origin/main
    // so the worker tab cannot flip the conductor's shared tree. Allocation
    // failure is non-fatal - we log it and proceed without a worktree path,
    // and the reference-transaction hook on the shared tree is the runtime
    // backstop in that case.
    let worktreePath = null
    try {
      worktreePath = await exports.allocateWorktreeForRow(row)
    } catch (e) {
      process.stderr.write('[scheduler] dispatchOne: worktree allocation failed for ' + row.id + ': ' + (e && e.message || e) + ' (dispatching without isolated worktree; branch-thrash guard now relies on the reference-transaction hook)\n')
    }

    // 2. Build brief with actual_account + worktree_path filled in.
    const rowWithAccount = Object.assign({}, row, { actual_account: account, worktree_path: worktreePath })
    let brief = exports.buildBrief(rowWithAccount)

    // 2b. Report-back default-on (2026-08-13). ensureReportBackDefault normally
    // runs in exports.schedule_delayed, but the conductor's schedule_delayed is
    // an HTTP MCP served REMOTELY (api.admin.ecodia.au), whose scheduler.js does
    // not carry this logic - so a row inserted via the MCP never gets the default
    // there. This is the LOCAL dispatch path every leased row flows through (remote
    // or local insert), so applying it here guarantees the default for interactive
    // dispatches. Gated to non-cron rows: standing crons stay inbox-only (no wake),
    // delayed/one_shot rows default to REPORT-BACK: conductor unless the author
    // opted out with REPORT-BACK: none. Idempotent, so double-application (if the
    // local insert path also ran it) is a no-op. dispatch_worker then parses the
    // directive and composeBrief emits the coord.message_chat({to:"conductor"})
    // wake step. Doctrine: coord-chat-to-chat-push-delivery-2026-08-02.
    if (row && row.type !== 'cron') {
      try { brief = ensureReportBackDefault(brief) } catch (e) {}
    }

    // 2c. Dispatch-start lease refresh + pre-spawn reclaim guard.
    // 2026-06-21 lease-aging-in-launch-lock-queue fix (the live 5-6h cron-fleet
    // stall: gmail-inbox-poll / calendar-watch / infra-health-pulse / status-board-
    // execute-top all sat status=dispatching retry_count=3, last_run_at frozen ~5h,
    // "stale lease recovered"). leased_at is stamped by leaseDueRows at QUEUE-ENTRY
    // time, but dispatchOne holds the SERIAL launch-lock across each bind (up to
    // SIGNAL_BOUND_TIMEOUT_MS). Under a due-row burst a row's leased_at ages past
    // STALE_DISPATCHING_MS (15min) WHILE STILL QUEUED behind the lock - before its
    // worker is ever spawned - so staleLeaseRecovery branch-1's liveness gate
    // (4cab6c3) cannot protect it (hasLiveWorkerForTask returns null: no worker
    // exists in coord yet). branch-1 reclaims it, and the eventual running-flip
    // no-ops (rowCount 0), leaving an ORPHAN tab that inflates active_workers,
    // forces rotation to defer, and collapses dispatch throughput into the spiral.
    // Refresh leased_at to NOW() HERE, at dispatch-start under the lock and as late
    // as possible before spawn (after the slow rotate_to + worktree alloc), so the
    // 15min stale window measures the actual bind duration (<=10min) with the
    // intended 5min headroom rather than the queue wait. Guarded on still owning
    // the lease: if the row was reclaimed/cancelled during account-rotation or
    // worktree prep, bail WITHOUT spawning a worker - preventing the orphan tab
    // entirely instead of discovering the loss after the spawn at the running-flip.
    // 2026-07-17 double-run guard (a): run_count re-check folded into the guard.
    // If run_count advanced past our leased snapshot, a prior spawn of this row
    // already COMPLETED (markComplete is the only writer of run_count) - spawning
    // again would double-run the task. Atomic here rather than SELECT-then-act.
    const startGuard = await pool.query(
      `UPDATE os_scheduled_tasks
          SET leased_at = NOW(), updated_at = NOW()
        WHERE id = $1
          AND status = 'dispatching'
          AND leased_by IS NOT DISTINCT FROM $2
          AND run_count IS NOT DISTINCT FROM $3`,
      [row.id, row.leased_by || null, row.run_count == null ? null : row.run_count]
    )
    if (startGuard.rowCount === 0) {
      // 2026-07-12: this guard was failing for EVERY row, so nothing ever spawned and the whole
      // fleet drained into a stuck 'dispatching' state. "Lease lost" alone does not say WHY, so
      // print what the row actually looks like against what we expected to own.
      let actual = '(unreadable)'
      try {
        const chk = await pool.query(
          'SELECT status, leased_by, archived_at, last_status FROM os_scheduled_tasks WHERE id = $1',
          [row.id]
        )
        actual = chk.rows[0] ? JSON.stringify(chk.rows[0]) : '(row gone)'
      } catch (e) {
        actual = '(check failed: ' + e.message + ')'
      }
      process.stderr.write('[scheduler] dispatchOne: row ' + row.id +
        ' lease lost during dispatch prep (reclaimed/cancelled before worker spawn); skipping spawn' +
        ' | expected leased_by=' + JSON.stringify(row.leased_by || null) +
        ' | actual=' + actual + '\n')
      return
    }

    // 2d. 2026-07-17 double-run guard (b): coord worker liveness. If a live
    // worker already exists for this task (a prior spawn survived a lease steal:
    // its running-flip no-op'd, the row cycled back through active->dispatching,
    // and we are now about to spawn a SECOND worker for the same task), do NOT
    // spawn. ADOPT the live worker instead: flip the row to running against its
    // tab so completionPass can complete it when it signals done. Doctrine:
    // [[scheduler-stale-lease-must-check-coord-worker-liveness-before-redispatch-2026-06-10]].
    {
      const liveWorker = await hasLiveWorkerForTask(String(row.id))
      if (liveWorker) {
        await pool.query(
          `UPDATE os_scheduled_tasks
           SET status = 'running', dispatched_tab_id = $1,
               leased_at = NOW(), updated_at = NOW()
           WHERE id = $2
             AND status = 'dispatching'
             AND leased_by IS NOT DISTINCT FROM $3`,
          [liveWorker.tab_id || row.dispatched_tab_id || null, row.id, row.leased_by || null]
        )
        process.stderr.write('[scheduler] dispatchOne: task ' + row.id +
          ' already has live worker ' + liveWorker.tab_id +
          ' (stale_ms=' + liveWorker.stale_ms + '); adopting it as running instead of double-spawning\n')
        return
      }
    }

    // 3. Dispatch worker.
    // Give the worker chat a PLAIN-LANGUAGE tab title from the task's own name
    // (e.g. "cowork.chambers-fi-arc-33" -> "chambers fi arc 33"), so Claude Code
    // titles the tab readably instead of [EOS-W-<hex>]. That makes the tab useful
    // in the IDE, useful as the iMessage reply prefix, and routable by topic. The
    // dispatcher still stamps the sentinel + fingerprint for the close path, so
    // this only changes the human-facing name, not how the tab is closed.
    // 2026-08-24: PREFIX the readable name with 4 hex of the task id.
    //
    // coord resolves a worker tab through its sentinel_prefix / autotitle
    // fingerprint, both derived from the TAB TITLE. This name comes from the
    // row's own name, and a worker chain names its successors after the same
    // project (coexist-ticket-selfservice, then -verify, then -verify-pass-3),
    // so sibling tabs carried IDENTICAL sentinels and the registry rows
    // collapsed onto whichever tab registered last. Two symptoms, one cause:
    // close_my_tab failed safe and refused (so every chained worker tab leaked
    // into the IDE), and message_chat to a colliding chat returned
    // delivered:false / session_ownership_conflict, which reads as a worker
    // failing to report when it actually reported fine.
    //
    // Found 2026-08-24 by worker d9f75736 while trying to close its own tab: one
    // list_channels call returned your_tab_id = <its own id> with NO channel row
    // carrying that address, and three rows all resolved to a sibling.
    //
    // The prefix goes at the FRONT deliberately. Claude Code truncates the
    // displayed label to ~24 chars, so a suffix would be cut off and the
    // disambiguator would vanish exactly where it is needed.
    //
    // name_chat does NOT fix this: it sets a stable inbox address and leaves the
    // window-title fingerprint untouched (ruled out empirically, same run).
    const workerName = (function () {
      const n = String((row && row.name) || '')
        .replace(/^cowork\./, '')
        .replace(/[._\-]+/g, ' ')
        .replace(/[^A-Za-z0-9 ]/g, '')
        .replace(/\s+/g, ' ')
        .trim()
        .slice(0, 34)
        .trim()
      const shortId = String((row && row.id) || '').replace(/[^a-f0-9]/gi, '').slice(0, 4)
      if (!n) return shortId || undefined
      return (shortId ? shortId + ' ' + n : n).slice(0, 40).trim()
    })()
    const dispatcher = getDispatcher()
    const result = await dispatcher.dispatch_worker({
      brief: brief,
      task_id: String(row.id),
      worker_name: workerName,
      // 2026-08-29. THE SOURCE OF THE LANE MAILBOX. Every other link in the
      // durable-addressing chain was already built - inboxTopicFor resolves a
      // lane key, registerWorkerInternal stores a lane_name, routes/comms.js
      // forwards one - and nothing ever SENT one, so lane mailboxes existed
      // only for a worker registered by hand and the whole layer was reachable
      // only from its own test. The name is passed RAW, not through laneKeyOf:
      // the registry stores what it was given and the resolver normalises, so
      // normalising here would put a second opinion about what a lane is into
      // the system. Unconditional is correct - a name with no lane token yields
      // a null key and falls back to per-tab addressing, which is the behaviour
      // every pre-lane worker already had.
      lane_name: (row && row.name) || null,
      ide: 'stable',
      // Set ack timeout to 0 so dispatch_worker returns immediately.
      // We do our own signal_bound wait below inside the launch-lock.
      worker_acknowledgment_timeout_ms: 0,
    })

    if (!result || !result.ok) {
      // 2026-05-29 ultracode audit H6 fix. When dispatch_worker spawns the
      // tab but fails to paste the brief, it returns {ok:false, orphan:true,
      // tab_id, tab_handle}. The tab is open + un-briefed + uncovered by any
      // sweep because dispatched_tab_id was never persisted. Clean it up
      // here before throwing so the next dispatch tick doesn't compound the
      // leak with a brand-new tab.
      if (result && result.orphan && result.tab_id) {
        try {
          await dispatcher.kill_worker({ tab_id: result.tab_id, tab_handle: result.tab_handle })
        } catch (e) {
          process.stderr.write('[scheduler] dispatchOne: orphan kill_worker error: ' + e.message + '\n')
        }
      }
      throw new Error('dispatch_worker failed: ' + (result && result.error || 'unknown'))
    }

    tabId = result.tab_id

    // Dispatch reached a spawned worker tab: the IDE bridge is alive. Clear any
    // in-progress transient-outage tracking so a later outage pages afresh
    // (2026-08-13 dispatch-outage pager). Placed here, not after the bound wait,
    // because a tab spawn already proves the bridge; a bound timeout on a live
    // bridge is a different (worker-side) failure and must not re-arm the pager.
    exports.noteSuccessfulDispatch()

    // Record this dispatch against both breaker windows, now that a worker tab
    // provably exists. See the CHECKED-here/RECORDED-there note in the breaker
    // block above: recording before the spawn let a spawn outage manufacture
    // quarantines. A fresh Date.now() is deliberate - prep between the check and
    // here can take a while, and the window this feeds should measure when
    // workers actually started.
    if (breakerRowName) exports.breakerRecordDispatch(breakerRowName, Date.now())

    // 3b. CLAIM THE ROW FOR THIS DISPATCH, AND BLANK THE LIFECYCLE SLATE.
    //
    // 2026-08-28 lane R1 item 2. This single guarded statement is what makes the
    // whole handshake race-free, and it has to happen HERE - after the tab
    // provably exists, before the worker can signal anything.
    //
    // It does two inseparable things:
    //   (a) writes dispatched_tab_id NOW rather than at the running-flip below.
    //       That column is the discriminator every worker signal is guarded on
    //       (`WHERE dispatched_tab_id = <caller tab>`), so until it names THIS
    //       tab, this dispatch's own bound would be refused and the PREVIOUS
    //       fire's worker could still write. It has to lead the wait, not follow it.
    //   (b) clears bound_at / done_* / progress_*, so the row carries no signal
    //       from any earlier fire. task_id equals the row id and is STABLE across
    //       cron fires, which is precisely why the old bus needed a leased_at
    //       freshness gate with a 30s back-margin. Clearing at dispatch makes a
    //       stale signal structurally impossible instead of heuristically filtered.
    //
    // GUARDED on still owning the lease, same predicate as the running-flip below
    // and for the same reason: staleLeaseRecovery may have reclaimed this row
    // while dispatch_worker was spawning. If it did, rowCount is 0 and we must
    // NOT proceed - writing our tab id onto a row someone else now owns is how a
    // double-dispatch gets laundered into looking legitimate.
    //
    // Writing dispatched_tab_id during 'dispatching' is safe for every reader of
    // that column: worker-liveness.js only selects status='running', and
    // startupCleanup / sweepStaleWorkers select their own statuses. It is also
    // strictly better on the error path, where the row previously kept a DEAD
    // tab id from an earlier fire while a real orphan tab went unnamed.
    const claimRes = await pool.query(
      `UPDATE os_scheduled_tasks
          SET dispatched_tab_id = $1, ${taskSignals.CLEAR_SQL_FRAGMENT}, updated_at = NOW()
        WHERE id = $2
          AND status = 'dispatching'
          AND leased_by IS NOT DISTINCT FROM $3`,
      [tabId, row.id, row.leased_by || null]
    )
    if (!claimRes || claimRes.rowCount === 0) {
      process.stderr.write('[scheduler] dispatchOne: lease for task ' + row.id +
        ' was reclaimed before the bind wait (tab ' + tabId +
        '); not stamping dispatch ownership\n')
      return
    }

    // 4. Wait for signal_bound (inside launch-lock).
    // Poll THIS ROW for bound_at / done_at. No inbox, no shared seen flag, no
    // oldest-first window: the worker writes the row it was dispatched from, and
    // its write is refused unless dispatched_tab_id still names its tab.
    const start = Date.now()
    let bound = false
    // 2026-07-17 fast-worker completion race, now structural rather than patched.
    // A short task (the dispatch selftest signals done immediately) can finish
    // without ever signalling bound. On the old bus that made the wait run its
    // full SIGNAL_BOUND_TIMEOUT_MS, and the running-flip below then re-stamped
    // leased_at = NOW(), which made completionPass's freshness gate classify the
    // already-landed done as a prior fire's stale signal and skip it FOREVER -
    // the row rotted at status=running until the 6h orphan sweep despite a clean
    // success. Live evidence 2026-07-17: selftest 973d712e done 02:08:22Z,
    // running-flip 02:17:58Z, completionPass skipping.
    //
    // Reading done_at from the row keeps that case correct for a simpler reason:
    // a done written before bound is just a column that is already set when the
    // first poll runs. There is no ordering to get wrong.
    let doneSignal = null
    // 2026-08-28. The three compensations this loop used to need are GONE, and
    // each one is gone because of a property of the row rather than a heuristic:
    //   - the `since` window (peek_inbox served oldest-unseen-first, so 525
    //     messages of backlog blinded every wait): a column has no backlog.
    //   - ack_message by id (read_inbox({limit:1}) marked the OLDEST unread seen,
    //     silently eating a DIFFERENT task's done under back-to-back dispatch):
    //     reading a column consumes nothing.
    //   - the freshness comparison against dispatchStartMs: 3b cleared these
    //     columns for this dispatch, and the worker's write is refused unless
    //     dispatched_tab_id still names its tab.
    while (Date.now() - start < SIGNAL_BOUND_TIMEOUT_MS) {
      let sig = null
      try {
        const sigRes = await pool.query(
          `SELECT bound_at, done_at, done_status, done_summary, done_pointer
             FROM os_scheduled_tasks WHERE id = $1`,
          [row.id]
        )
        sig = (sigRes && sigRes.rows && sigRes.rows[0]) || null
      } catch (e) {
        // A transient pool error must not be read as "not bound". Log and retry
        // inside the same budget; the loop's own timeout is the backstop.
        process.stderr.write('[scheduler] dispatchOne: bind-wait poll error for task ' +
          row.id + ': ' + e.message + '\n')
      }
      if (sig) {
        if (sig.done_at) {
          // Terminal beats bound. Whether or not the worker also bound, it has
          // finished, and holding the launch lock any longer serves nothing.
          bound = true
          doneSignal = {
            status: sig.done_status || 'success',
            result_summary: sig.done_summary,
            result_pointer: sig.done_pointer,
          }
        } else if (sig.bound_at) {
          bound = true
        }
      }
      if (bound) break
      await new Promise(r => setTimeout(r, 1000))
    }

    // 5. Update row to running - GUARDED on still owning the lease.
    //
    // 2026-06-18 half-state race fix. The signal_bound wait above can run up to
    // SIGNAL_BOUND_TIMEOUT_MS (600s). If it overruns STALE_DISPATCHING_MS,
    // staleLeaseRecovery branch-1 fires on this still-'dispatching' row, sets
    // status='active', leased_by=NULL, retry_count++. The OLD unconditional
    // UPDATE then blindly overwrote status back to 'running' WITHOUT reclaiming
    // leased_by, producing the observed zombie half-state (status=running AND
    // leased_by IS NULL - a running row with no lease owner) and risking a
    // double-dispatch of the same task. Guarding on (status='dispatching' AND
    // leased_by unchanged) makes the flip a no-op when recovery already reclaimed
    // the row; we then bail and let the recovered row re-dispatch cleanly. Live
    // evidence 2026-06-18: external-blocker / client-app-health /
    // monthly-architectural-review all sat status=running, leased_by NULL,
    // leased_at SET, last_error='stale lease recovered'.
    const runRes = await pool.query(
      `UPDATE os_scheduled_tasks
       SET status = 'running', actual_account = $1, dispatched_tab_id = $2,
           leased_at = NOW(), updated_at = NOW()
       WHERE id = $3
         AND status = 'dispatching'
         AND leased_by IS NOT DISTINCT FROM $4`,
      [account, tabId, row.id, row.leased_by || null]
    )

    if (runRes && runRes.rowCount === 0) {
      // Lease reclaimed mid-bind (or already advanced). Do NOT resurrect to
      // running - that is exactly the zombie half-state. The just-spawned tab,
      // if live, is reconciled by cleanup_orphan_workers; if the worker signals
      // done, the row is already back in the active->dispatch cycle.
      process.stderr.write('[scheduler] dispatchOne: lease for task ' + row.id +
        ' was reclaimed during signal_bound wait (tab ' + tabId +
        '); skipping running-flip to avoid zombie half-state\n')
      return
    }

    if (!bound) {
      process.stderr.write('[scheduler] dispatchOne: signal_bound timeout for task ' + row.id + ' (tab ' + tabId + ')\n')
    }

    // 2026-07-17 fast-worker completion race fix (part 2). If the bind wait
    // observed this dispatch's done signal, complete the row NOW. Waiting for
    // completionPass would lose it: the running-flip above just re-stamped
    // leased_at = NOW(), so its freshness gate (doneMs < leasedMs - 30s)
    // classifies the in-wait done as a prior fire's stale signal and skips it
    // forever. See the bind-wait comment above for the live evidence.
    if (doneSignal) {
      try {
        await exports.markComplete({ ...row, dispatched_tab_id: tabId }, doneSignal)
        process.stderr.write('[scheduler] dispatchOne: fast-worker done for task ' + row.id +
          ' consumed inline (worker signalled done during bind wait)\n')
      } catch (e) {
        process.stderr.write('[scheduler] dispatchOne: inline markComplete error for ' + row.id + ': ' + e.message + '\n')
      }
    }
  } catch (err) {
    const errMsg = err && err.message || ''
    if (err && err.name === 'AllAccountsCappedError') {
      // Defer to the earliest reset (floored at 60s), release the lease, keep
      // status=active, mark retry_count for the health detector, and page once
      // the outage has persisted. See handleAllAccountsCappedDefer for why all
      // three legs are needed and why none of the three existing watchers saw this.
      await exports.handleAllAccountsCappedDefer(row, err)
    } else if (exports.isTransientBridgeError(errMsg)) {
      // 2026-06-02 P0 fix, broadened 2026-06-20 (audit finding c). The
      // dispatch_worker editor.open path throws a transient error when no IDE has
      // registered the bridge ("no IDE instances registered") OR when the bridge
      // socket blips mid-open ("populate failed (editor.open): socket hang up",
      // ECONNRESET, ECONNREFUSED, ETIMEDOUT, EPIPE). All are TRANSIENT infra
      // unavailability (Tate closes the IDE, machine sleeps, extension reloads,
      // bridge restarts), not a task fault. Treat like AllAccountsCappedError:
      // defer ~5min, do NOT increment retry_count, do NOT mark failed - for cron
      // AND one-shot/delayed rows alike (the latter would otherwise burn retries
      // to a permanent status='failed' on a transient blip and silently lose the
      // scheduled work). The row survives the gap window naturally.
      const deferMs = 5 * 60 * 1000
      const nextRun = new Date(Date.now() + deferMs)
      try {
        await pool.query(
          `UPDATE os_scheduled_tasks
           SET status = 'active', leased_by = NULL, leased_at = NULL,
               next_run_at = $1, last_error = $2, updated_at = NOW()
           WHERE id = $3`,
          [nextRun.toISOString(), 'transient IDE-bridge error - deferred 5min (' + errMsg.slice(0, 80) + ')', row.id]
        )
      } catch (pgErr) {
        process.stderr.write('[scheduler] transient-bridge defer pg error: ' + pgErr.message + '\n')
      }
      // 2026-08-13 dispatch-outage pager. Count this defer; page Tate exactly
      // once when the outage has persisted N consecutive defers (~15min). The
      // page fire is independent of the pg update above so a DB blip cannot
      // suppress the alert. Rate-limited + latched inside noteTransientDefer.
      try { exports.noteTransientDefer(errMsg) } catch (pgErr) {
        process.stderr.write('[scheduler] outage pager note error: ' + pgErr.message + '\n')
      }
    } else {
      try {
        await exports.markFailed(row, err)
      } catch (pgErr) {
        process.stderr.write('[scheduler] markFailed pg error: ' + pgErr.message + '\n')
      }
    }
    // Re-throw so the caller knows this row failed.
    throw err
  } finally {
    // Always deregister from the in-flight registry and release the launch-lock.
    _inFlightDispatchIds.delete(inFlightKey)
    release()
  }
}

// ── markComplete ─────────────────────────────────────────────────────────────
//
// Called when a running task signals done. Handles cron reschedule or
// one_shot completion. Tolerates close_tab failures.

exports.markComplete = async function markComplete(row, signal) {
  const pool = getPool()
  const dispatcher = getDispatcher()

  // 2026-06-10 branch-thrash guard cleanup: drop the per-row isolated worktree
  // allocated at dispatchOne. Idempotent + tolerant so a missing worktree
  // (legacy row from before the guard shipped, or one already pruned by a
  // recovery sweep) does not block completion.
  try { await exports.pruneWorktreeForRow(row) } catch (_e) {}

  // 2026-05-29 ultracode audit H2 fix. Was: kill_worker in try/catch then
  // unconditional dispatched_tab_id = NULL. kill_worker returns ok:true even
  // when it refuses, so the NULL hid the leak and discarded the only handle
  // for a later sweep. Now: only NULL the column when the kill actually
  // closed the tab. Retained handles are reconcilable by
  // cleanup_orphan_workers (which runs every CLEANUP_ORPHAN_INTERVAL_MS).
  let closeOk = false
  if (row.dispatched_tab_id) {
    try {
      if (dispatcher.kill_worker) {
        const killRes = await dispatcher.kill_worker({ tab_id: row.dispatched_tab_id })
        closeOk = !!(killRes && killRes.closed)
      }
    } catch (e) {
      process.stderr.write('[scheduler] kill_worker tolerated error: ' + e.message + '\n')
    }
  }
  const dispatchedTabIdSqlFrag = closeOk ? 'dispatched_tab_id = NULL,' : ''

  // 2026-06-09: missing status === success. Pre-fix, coord.signal_done was
  // dropping params.status on the way to the inbox, so signal.status came
  // through as undefined and every signal_done was misclassified as failure.
  // Defensive default: a worker calling signal_done at all is the affirmative
  // "I finished" signal; only an EXPLICIT signal.status === 'failed' (or any
  // non-success value) should route through markFailed. See
  // [[scheduler-signal-done-status-must-survive-coord-to-inbox-2026-06-09]].
  // 2026-07-18: stand-down is terminal, never a failure. A worker that honours
  // a stand_down reports status='stood_down'; without this branch that value is
  // "not success", so it routed through markFailed, burned a retry, and the
  // dispatch loop RE-DISPATCHED the very worker the conductor just stood down.
  // The stand-down would resurrect itself, which is worse than the gap it fixes.
  // A stood-down run is a run that correctly did not finish: no retry, no chain
  // wake (children must not fire on work that was abandoned), and a one_shot is
  // cancelled rather than completed because its scope was never done. A cron
  // just skips this fire and keeps its schedule.
  // Doctrine: [[worker-interruptibility-soft-poll-hard-kill-2026-07-18]].
  const stoodDown = !!(signal && signal.status === 'stood_down')
  const explicitFailure = signal && signal.status && signal.status !== 'success' && !stoodDown

  if (explicitFailure) {
    // Treat as failure: delegate to markFailed with a synthetic error.
    const syntheticErr = new Error(
      (signal && signal.result_summary) || 'task signaled non-success'
    )
    syntheticErr.message = (signal && signal.result_summary) || 'task signaled non-success'
    return exports.markFailed(row, syntheticErr)
  }

  const rowType = row.type || 'one_shot'

  // 2026-08-29 lane R1 item 2, verification pass. CONSUME the lifecycle signal
  // at the terminal transition, in every branch below and in markFailed.
  //
  // Without this, done_at / done_status survive completion forever: nothing else
  // clears them but dispatchOne's step 3b, and the ADOPT path (the 2026-07-17
  // double-spawn guard, dispatchOne step 2d) flips a row to status='running'
  // and RETURNS before ever reaching 3b. completionPass then matches
  // `status='running' AND done_at IS NOT NULL` against the PREVIOUS fire's
  // signal, kills the live worker it has just adopted, and advances the row and
  // its chain children off work that already completed once.
  //
  // Measured on the real table 2026-08-29, inside a rolled-back transaction:
  // run_count went 1 -> 2 off a consumed signal and kill_worker was called on
  // the adopted tab. The adopt path fired 19 times in the current daemon log,
  // so this is a live recovery route, not a theoretical one.
  //
  // Clearing HERE rather than at adopt is deliberate. staleLeaseRecovery branch 1
  // reclaims a lease WITHOUT touching dispatched_tab_id, so a worker whose lease
  // was reclaimed can still legitimately write its done while the row sits
  // active; clearing at adopt would erase that and rot the row to the 6h orphan
  // sweep. Consuming at the terminal transition instead makes "done_at present"
  // mean UNCONSUMED, which is what completionPass has always assumed.
  if (rowType === 'cron' && row.cron_expression) {
    // Compute next_run_at via cron-parser using the row's tz (default Brisbane).
    // Pre-2026-05-31 this was hardcoded {utc:true}, which mis-fired AEST schedules
    // by 10h. Phase A1 unification: every cron carries its own tz column.
    let nextRunAt = null
    try {
      nextRunAt = exports.computeNextRunAt(row)
    } catch (cronErr) {
      process.stderr.write('[scheduler] cron-parser error for "' + row.cron_expression + '": ' + cronErr.message + '\n')
    }
    const cronRes = await pool.query(
      `UPDATE os_scheduled_tasks
       SET status = 'active', last_run_at = NOW(), next_run_at = $1,
           run_count = run_count + 1, last_result = $2,
           retry_count = 0, leased_by = NULL, leased_at = NULL,
           ${taskSignals.CLEAR_SQL_FRAGMENT},
           ${dispatchedTabIdSqlFrag} updated_at = NOW()
       WHERE id = $3
         AND archived_at IS NULL
         AND (last_status IS NULL OR last_status NOT IN ('paused', 'cancelled'))`,
      [nextRunAt, String((signal && signal.result_summary) || '').slice(0, 2000), row.id]
    )
    if (!cronRes.rowCount) await consumeSuspendedSignal(pool, row.id)
  } else {
    // one_shot or delayed: mark completed - or cancelled when stood down, since
    // the scope was abandoned rather than finished. 'cancelled' is also what
    // keeps the row out of any re-arm path that respects last_status.
    // Inlined as a literal (not a bound param) to match dispatchedTabIdSqlFrag
    // above: both are constants chosen by a local boolean, never caller input,
    // and keeping the literal keeps the SQL greppable for the assertions that
    // read it.
    const oneShotStatusSqlLiteral = stoodDown ? 'cancelled' : 'completed'
    await pool.query(
      `UPDATE os_scheduled_tasks
       SET status = '${oneShotStatusSqlLiteral}', last_run_at = NOW(), last_result = $1,
           run_count = run_count + 1, leased_by = NULL, leased_at = NULL,
           ${taskSignals.CLEAR_SQL_FRAGMENT},
           ${dispatchedTabIdSqlFrag} updated_at = NOW()
       WHERE id = $2`,
      [String((signal && signal.result_summary) || '').slice(0, 2000), row.id]
    )
  }

  // 2026-06-01 Phase A2: chain wake-up. Any child row with chain_after = parent.id
  // gets next_run_at=NOW() so the dispatch loop picks it up on the next 30s poll.
  // Only fires on success - chain children stay parked if the parent failed.
  // 2026-06-10: ReferenceError fix. Was `if (isSuccess)` referencing an undeclared
  // variable, throwing on every completionPass markComplete and caught by the outer
  // try/catch. !explicitFailure is the correct success predicate at this point
  // (explicitFailure path already returned early via markFailed above).
  // 2026-07-18: !stoodDown added. Chain children encode "the parent's work is
  // done, now do the next stage"; a stood-down parent abandoned its work, so
  // waking children would run the next stage on a foundation that was never
  // built - the stand-down would leak downstream into the exact fleet the
  // conductor was trying to quiet.
  if (!explicitFailure && !stoodDown) {
    try {
      await pool.query(
        `UPDATE os_scheduled_tasks
         SET next_run_at = NOW(), updated_at = NOW()
         WHERE chain_after = $1
           AND status = 'active'
           AND archived_at IS NULL
           AND (last_status IS NULL OR last_status NOT IN ('paused', 'cancelled'))`,
        [row.id]
      )
    } catch (chainErr) {
      process.stderr.write('[scheduler] chain wake-up error for parent ' + row.id + ': ' + chainErr.message + '\n')
    }
  }
}

// ── completionPass ───────────────────────────────────────────────────────────
//
// Checks coord inbox for done signals from running tasks and completes them.
// Non-destructive: uses peek_inbox to scan, then targeted read_inbox to consume.

exports.completionPass = async function completionPass() {
  const pool = getPool()

  // 2026-08-28 lane R1 item 2. The completion is now a column on the row, so
  // the SELECT is the whole detection: a running row with done_at set has been
  // signalled by the tab that currently owns it, and nothing else can have
  // written it. Filtering in SQL also means the common case (no worker has
  // finished this tick) costs one indexless-but-tiny scan and zero further work,
  // where the old path read every running row and then walked the entire
  // in-memory inbox index for each of them, every 5 seconds, forever.
  // 2026-09-05 lane C1. Widened from status = 'running' to include 'orphaned'.
  // Measured over 14 days: 3 one-shot rows carry BOTH status='orphaned' and a
  // worker-written done_at. Those are real completions the scheduler threw away,
  // because the reap or the six-hour timer fired between the worker's row write
  // and the next completion tick, and after that no pass would ever look at them
  // again. Honouring them fabricates nothing: done_at can only have been written
  // by the worker itself, so this reads a report that was already there.
  const result = await pool.query(
    `SELECT * FROM os_scheduled_tasks
      WHERE status = 'running' AND done_at IS NOT NULL LIMIT 50`
  )

  // The orphaned arm is DELIBERATELY SEPARATE and deliberately does not go
  // through markComplete. Shipping it through markComplete first, at 07:10Z on
  // 2026-09-05, produced a hot loop within seconds: row 9ddd9320
  // (cowork.coexist-lane-B1-emergency-contact-book-block) had reported
  // done_status='failed' on 2026-08-29 and been orphaned, so markComplete routed
  // it to markFailed, which re-arms status='active' for a retry, which fired the
  // migration 153 lane-revive arm, which correctly REFUSED because a live sibling
  // held lane coexist-lane-b1. The row stayed orphaned and the next tick tried
  // again, forever, nine times before it was caught.
  //
  // The lesson is the distinction the first version missed. Honouring a report the
  // scheduler threw away means RECORDING what the worker said. It does not mean
  // re-running a week-old job, and a terminal row is terminal: it has no lease to
  // complete, no next fire to schedule, and it may sit on a lane something live
  // now owns. So this arm writes the terminal status the report implies and
  // nothing else. One statement, no re-arm, no lane transition, and it can never
  // select the same row twice because the write moves it out of 'orphaned'.
  try {
    const late = await pool.query(
      `UPDATE os_scheduled_tasks
          SET status = CASE WHEN done_status IS NULL OR done_status IN ('success', 'stood_down')
                            THEN 'completed' ELSE 'failed' END,
              last_status = CASE WHEN done_status IS NULL OR done_status IN ('success', 'stood_down')
                            THEN 'completed' ELSE 'failed' END,
              last_result = COALESCE(done_summary, last_result),
              last_error = COALESCE(last_error, 'worker reported after the row was orphaned'),
              updated_at = NOW()
        WHERE status = 'orphaned' AND done_at IS NOT NULL AND archived_at IS NULL
        RETURNING id, name, done_status`)
    for (const r of late.rows) {
      process.stderr.write('[scheduler] completionPass: honoured a late worker report on orphaned row ' +
        r.id + ' (' + (r.name || '?') + ') done_status=' + (r.done_status || 'success') + '\n')
    }
  } catch (e) {
    process.stderr.write('[scheduler] completionPass late-report arm error: ' + e.message + '\n')
  }

  if (!result.rows.length) return

  for (const row of result.rows) {
    // 2026-07-08 phantom-completion-class guard, RETAINED and now the only gate
    // left. A legitimately-running row ALWAYS carries a lease: dispatchOne
    // stamps leased_at = NOW() on the running-flip. A running row with
    // leased_at IS NULL is a recovery half-state, not a completable run, and
    // completing it would advance a row whose lease someone else reclaimed.
    // This is orthogonal to signal identity, which is why it survives the
    // migration: it is about the LEASE, not about the signal.
    if (!row.leased_at) {
      process.stderr.write('[scheduler] completionPass: skip unleased running row ' + row.id + ' (leased_at NULL; leaving for staleLeaseRecovery)\n')
      continue
    }
    // THE FRESHNESS GATE IS DELETED, and its absence is the point.
    //
    // It compared the done's created_at against leased_at with a 30s back-margin,
    // because task_id equals the row id and is STABLE across cron fires, so an
    // append-only inbox accumulated many done signals per row and a days-old one
    // could complete a fresh dispatch. Two heuristics were stacked on that: the
    // margin (to tolerate a worker finishing microseconds before the flip) and
    // the assumption that cron intervals always exceed it.
    //
    // done_at is now cleared by dispatchOne at spawn and writable only by the tab
    // that currently owns the row. A stale signal cannot be present to be
    // filtered, so there is nothing left to compare against a clock. Restoring a
    // time comparison here would re-open the 2026-07-17 case it used to cause,
    // where a fast worker's done landed BEFORE the running-flip re-stamped
    // leased_at and was then rejected forever for being "too old".
    const signal = {
      status: row.done_status || 'success',
      result_summary: row.done_summary,
      result_pointer: row.done_pointer,
    }
    try {
      await exports.markComplete(row, signal)
    } catch (e) {
      process.stderr.write('[scheduler] completionPass markComplete error for ' + row.id + ': ' + e.message + '\n')
    }
  }
}

// ── staleLeaseRecovery ───────────────────────────────────────────────────────
//
// Three SQL updates per the plan:
//   1. dispatching > STALE_DISPATCHING_MS and retry_count < MAX_RETRY_COUNT -> active, retry_count++
//   2. dispatching > STALE_DISPATCHING_MS and retry_count >= MAX_RETRY_COUNT -> failed
//   3. running > ORPHAN_TIMEOUT_MS -> orphaned

// Liveness staleness window: a worker whose last heartbeat was within
// STALE_WORKER_LIVENESS_MS counts as alive for the stale-lease skip path.
// 180s matches the existing scheduler.SIGNAL_BOUND_TIMEOUT_MS cold-start
// p90 floor and the cowork heartbeat cadence. Hardcoding lower than the
// observed cold-start floor is an anti-pattern: a slow bind would look
// dead and let the scheduler thunder-herd the same row.
const STALE_WORKER_LIVENESS_MS = 180_000

// Passed by the call sites that DESTROY work (prune a worktree, kill a tab), so a
// coord-silent worker gets a second chance to be found alive from transcript
// mtime. Frozen: it is shared across call sites and must stay a constant.
// The two sites that deliberately do NOT pass it, and why, are documented on
// hasLiveWorkerForTask.
const PROTECT_LIVE_WORKERS = Object.freeze({ transcriptFallback: true })

// 2026-08-29 (status_board 8d28c32d, P1). A SECOND CHANCE TO FIND THE WORKER
// ALIVE, from a signal the worker cannot forget to send.
//
// The coord heartbeat below was this gate's ONLY liveness signal, and it is wrong
// in the unsafe direction. Measured: 78 of 114 coord worker rows carry
// last_heartbeat_at EXACTLY equal to registered_at. They beat once at boot and
// never again, so 68.4 pct of workers go invisible here 180 seconds after they
// start. The gate then answers "no live worker" for a worker that is running, and
// four of its six call sites act on that by rm -rf'ing the worker's isolated
// worktree (pruneWorktreeForRow) with its uncommitted work inside.
// Proven instance 2026-08-28: cron row f0c904df (status-board-execute-top) took
// 'running orphan-timeout' at 18:41Z while its tab tab_1787940597108_afd2c194 ran
// on until 18:44:56Z. 55 cron rows carry that last_error, 37 of them live.
//
// The right oracle was already in the repo and already used ONE FUNCTION AWAY:
// livenessReapPass reads tools/worker-liveness.js, which derives a verdict from
// transcript mtime, a file the Claude Code harness writes every turn with no
// cooperation from the model, with a worktree-mtime fallback. That function's own
// header argues this about hasLiveWorkerForTask BY NAME ("a signal that wrong in
// the unsafe direction cannot be the thing that decides to kill a worker"). The
// judgement was written and never applied to the gate that destroys.
//
// SEMANTICS. The oracle can only ever find a worker ALIVE. It is never a new
// reason to kill one:
//   coord says live                 -> live, unchanged.
//   coord silent / stale / down     -> ask the oracle.
//       'live'    -> live, refuse the reclaim.
//       'unknown' -> live, refuse the reclaim. Not-reaping is always the safe
//                    direction on a gate whose false negative deletes work, and
//                    the refusal is BOUNDED: a genuinely dead worker ages past
//                    worker-liveness DEFAULT_CONFIRM_WINDOW_MIN and reads 'dead'.
//       'dead'    -> fall through, the reclaim proceeds exactly as today.
//   probe throws                    -> refuse, and say why on stderr.
//
// OPT-IN, and the two call sites deliberately left OUT. Default off, so any
// caller not passing transcriptFallback is byte-identical to before.
//   ON  markFailed, staleLeaseRecovery 2a, 2b and 3: every one of these prunes a
//       worktree or kills a tab, so a false 'dead' destroys work. Skipping a
//       prune only leaks a directory that reapOrphanWorktrees later collects.
//   OFF dispatchOne's double-spawn guard: it ADOPTS the found worker as running
//       instead of spawning. Its row carries a FRESH leased_at and the PREVIOUS
//       fire's dispatched_tab_id, so the oracle reads the boot race and answers
//       'unknown' on every fire. Worse, transcript mtime persists for the whole
//       window after a tab closes, and a cron's row id IS its task id, so a fire
//       that just ended reads 'live' for the next fire of the same row. Either
//       way the scheduler would adopt a dead tab and never spawn, wedging every
//       cron. That contamination is also why no registry ownership check helps:
//       fires of one cron share the task id, so the registry cannot tell them
//       apart.
//   OFF staleLeaseRecovery branch 1: pure recovery, no prune and no kill, so
//       there is no work to protect and refusing only delays a re-dispatch.
//
// COORD UNREACHABLE. The old catch returned null (fail open) so a coord outage
// could not block stale-lease recovery. The oracle now runs in that catch too at
// the enabled callers: it reads local disk and does not depend on coord, and an
// outage is exactly when a blind rm -rf is least defensible. Recovery is still
// not blocked, because 'dead' and the no-tab-id case both fall through.
async function hasLiveWorkerForTask(taskId, opts) {
  opts = opts || {}
  try {
    // include_dead:true is REQUIRED, and dropping the w.dead test with it.
    // coord.list_workers defaults include_dead=false and filters server-side on
    // its OWN constant (DEAD_HEARTBEAT_MS = 90s), so without this the scheduler
    // documents a 180s policy and silently enforces 90s: every guard below is
    // unreachable for exactly the rows it exists to catch. And because coord
    // still sets dead=true past 90s, asking for the dead rows while keeping
    // `if (w.dead) continue` drops them again client-side, so the flag has to go
    // with it. terminated_at is checked separately, which is the only other
    // thing w.dead contributed. STALE_WORKER_LIVENESS_MS is then the single
    // liveness policy, which was the author's intent.
    // Origin 2026-08-26: a worker inside one long Read/Bash/CDP step goes >90s
    // silent, becomes invisible here, and its LIVE lease is reclaimed and
    // re-dispatched (the same-lane double-dispatch), or its worktree is reaped.
    const r = await getCoord().list_workers({ include_dead: true })
    const ws = (r && r.workers) || []
    for (const w of ws) {
      if (w.task_id !== taskId) continue
      if (w.terminated_at) continue
      if (typeof w.stale_ms === 'number' && w.stale_ms >= STALE_WORKER_LIVENESS_MS) continue
      return w
    }
  } catch (e) {
    // Coord unreachable. Do not block stale-lease recovery on it (see COORD
    // UNREACHABLE above); the transcript oracle below still gets its say.
    process.stderr.write('[scheduler] hasLiveWorkerForTask coord error for ' + taskId + ': ' + e.message + '\n')
  }
  return await transcriptLivenessFallback(taskId, opts)
}

// liveTabs walks the transcript tree (measured 0.17s in worker-liveness's own
// header). The orphan sweep can ask about tens of rows inside one pass, so memo
// the map for a few seconds rather than re-walking it per row. The TTL is far
// shorter than every window the oracle reasons about (boot grace 10min, live
// window 30min), so memoising cannot change a verdict.
let _liveTabsMemo = null
const LIVE_TABS_MEMO_MS = 15_000
exports._resetLiveTabsMemo = function () { _liveTabsMemo = null }

async function transcriptLivenessFallback(taskId, opts) {
  if (!opts || !opts.transcriptFallback) return null
  try {
    // Fetch the row HERE rather than trusting the caller's. Branch 3's own SELECT
    // is (id, dispatched_tab_id, type, cron_expression, tz) with NO leased_at, and
    // probeRows reads a null leased_at as the boot race and answers 'unknown'
    // forever. Fetching keeps every caller's SELECT untouched and makes the four
    // enabled sites behave identically.
    const res = await getPool().query(
      `SELECT id, name, leased_at, dispatched_tab_id FROM os_scheduled_tasks WHERE id = $1`,
      [taskId]
    )
    const row = res && res.rows && res.rows[0]
    if (!row) return null

    // NO TAB ID, NO KEY. probeRows answers 'unknown' for a row carrying no
    // dispatched_tab_id, and that verdict can NEVER change: it is an absence, not
    // a margin. Refusing on it would wedge this row's only reclaim path forever,
    // and a verdict that cannot change is not a safety margin. This is the one
    // deliberate deviation from treating every 'unknown' as live. Nothing is lost:
    // coord keys on task_id rather than tab, so it already covers these rows.
    if (!row.dispatched_tab_id) return null

    const liveness = opts.liveness || require('./worker-liveness')
    const windowMin = typeof opts.window_minutes === 'number'
      ? opts.window_minutes : liveness.DEFAULT_WINDOW_MIN
    const now = Date.now()
    if (!_liveTabsMemo || _liveTabsMemo.windowMin !== windowMin ||
        (now - _liveTabsMemo.at) >= LIVE_TABS_MEMO_MS) {
      _liveTabsMemo = { at: now, windowMin, map: liveness.liveTabs(windowMin) }
    }
    const v = liveness.probeRows([row], Object.assign({}, opts, { live_tabs: _liveTabsMemo.map }))[0]
    if (!v || v.verdict === 'dead') return null

    process.stderr.write(
      '[scheduler] task ' + taskId + ' coord-silent but transcript oracle says ' + v.verdict +
      ' (' + v.reason + '); refusing the reclaim so its worktree is not destroyed\n')
    return {
      task_id: taskId,
      tab_id: row.dispatched_tab_id,
      source: 'transcript-mtime',
      verdict: v.verdict,
      reason: v.reason,
      evidence: v.evidence,
    }
  } catch (e) {
    // A probe failure is not evidence of death. Refuse, and say why.
    process.stderr.write('[scheduler] hasLiveWorkerForTask transcript probe error for ' + taskId +
      ' (refusing the reclaim, which is the safe direction): ' + e.message + '\n')
    return { task_id: taskId, tab_id: null, source: 'transcript-probe-error',
      verdict: 'unknown', reason: 'probe error: ' + e.message }
  }
}

exports._hasLiveWorkerForTask = hasLiveWorkerForTask

exports.staleLeaseRecovery = async function staleLeaseRecovery() {
  const pool = getPool()

  // EVERY UPDATE BELOW RE-ASSERTS THE STATUS ITS SELECT MATCHED. (lane D1 pass 3,
  // 2026-08-29.) Each branch is SELECT -> await hasLiveWorkerForTask -> UPDATE, and
  // that await is not free: it is a coord.list_workers network call and, on the
  // coord-unreachable path, a transcript-tree walk. The loop is serial, so the last
  // row in a batch is written many seconds after it was read. In that window the row
  // moves under us: dispatchOne flips 'dispatching' -> 'running' on bind (lines 2019
  // and 2245), a worker signals done, the conductor cancels, migration 147 supersedes.
  //
  // Branch 1 has carried `AND status = 'dispatching'` since the 2026-06-21 reclaim-vs-
  // bind fix. The other three never got it, and branch 2b carried no WHERE clause at
  // all beyond the id, which is the worst place for it to be missing because 2b is the
  // branch that PERMANENTLY FAILS a row.
  //
  // Measured against real Postgres 2026-08-29: the shipped 2b UPDATE, applied to four
  // rows representing what the window can hand it, flipped ALL FOUR to 'failed' -
  // a row that had already bound to 'running', an archived row, a breaker-quarantined
  // row (last_status='paused'), and the one genuinely-stale row it is actually for.
  // With the guards, only the genuinely-stale row failed and the other three were
  // untouched, so the guard does not disable the branch, it aims it.
  //
  // Branch 3 additionally guards `done_at IS NULL`. completionPass selects
  // `status='running' AND done_at IS NOT NULL`, so a row reclaimed out of 'running'
  // after its worker signalled done is invisible to the pass that would record the
  // success, and the fire is lost. The reverse ordering of that same race is live on
  // the fleet right now: row 5db1448b cowork.armed-claim-audit-daily was reclaimed at
  // 22:40:54Z, its worker then signalled done_status='success' at 22:44:32Z, and the
  // row still reads run_count=0 / last_result=null. That ordering is the liveness
  // gate's miss, not this one, and is boarded separately; this guard closes the half
  // an UPDATE predicate can close.
  // [[enumerate-all-trigger-paths-when-fixing-data-flow-bugs]]

  // 0. SUSPENDED MID-LEASE -> settle. (lane D1 pass 4, 2026-08-29.)
  //
  // Every branch below re-asserts `last_status NOT IN ('paused','cancelled')`, and
  // that guard is right for the branches that RESURRECT a row (branch 1, branch 2a
  // and branch 3's cron arm all set status='active'): a paused row must not be
  // flipped back to active. But the SAME guard rides on the two branches that
  // SETTLE a row (2b sets 'failed', branch 3's non-cron arm sets 'orphaned'), and
  // there it does not aim the branch, it disables it. "Do not resurrect" silently
  // became "can never settle".
  //
  // The result is a row nothing can move, and it is reachable through a SHIPPED
  // TOOL: schedule_pause writes last_status='paused' and deliberately does not
  // touch status, refusing only archived rows, so pausing a task inside its
  // dispatch or run window leaves `status IN ('dispatching','running') AND
  // last_status='paused' AND archived_at IS NULL`. From there:
  //   - leaseDueRows will not lease it (its arrival guard excludes last_status
  //     'paused'/'cancelled'), so it never runs again;
  //   - but leaseDueRows' HOLDER predicate tests only `status IN ('running',
  //     'dispatching')` with no last_status test, so the row keeps holding its
  //     LANE and every future row on that lane defers behind it FOREVER. A lane
  //     is how the fleet stops two tabs opening on one job, so a permanently
  //     held lane is silent, permanent work stoppage for that lane;
  //   - branches 1/2b select nothing (their SELECTs carry the guard) and
  //     branch 2a + branch 3 select it, run the liveness probe, fire kill_worker
  //     and pruneWorktreeForRow, then no-op the UPDATE, on EVERY pass, forever.
  //     That churn is the same shape the 2026-08-29 completionPass fix removed
  //     (see consumeSuspendedSignal), at this routine's cadence instead of 5s.
  // The comment on consumeSuspendedSignal used to name branch 3 as the designed
  // recovery for the running half of this state. It is not and never was; branch
  // 3's UPDATEs carry the guard. This branch is that recovery.
  //
  // SETTLE TARGET is `status = last_status`, not 'failed' or 'orphaned'. That is
  // quarantineRow's 2026-08-26 lesson (task daead163): a row suspended in one
  // column while status reads healthy is the most invisible failure in the fleet,
  // because every ordinary status query calls it fine. status='paused' is a valid
  // enum value, it releases the lane, it keeps the operator's pause (leaseDueRows
  // still refuses it on last_status), and schedule_resume clears BOTH columns, so
  // the row stays resumable by the normal path.
  //
  // DISJOINT from the population the pass-3 guards protect. A breaker-quarantined
  // row already sits at status='paused' (quarantineRow sets both columns), so it
  // is not in ('dispatching','running') and this SELECT cannot see it. Nothing
  // here loosens any guard below; it is additive.
  //
  // TWO independent signals separate an abandoned row from a healthy launching
  // one, because either alone is wrong: SUSPENDED_LEASE_SETTLE_MS (30min, past the
  // ~21min p99 cold-bind tail) AND the per-row liveness gate, which is what makes
  // freeing the lane safe. A row with a live worker is left strictly alone, so
  // this can never open a second tab on a job someone is still doing.
  //
  // done_at IS NULL mirrors branch 3: completionPass owns a row that has signalled
  // done, and consumeSuspendedSignal clears that signal for exactly this suspended
  // population every 5s, so the two converge instead of racing.
  const dispatcherForSuspended = getDispatcher()
  const suspendedRows = await pool.query(
    `SELECT id, name, type, dispatched_tab_id FROM os_scheduled_tasks
     WHERE status IN ('dispatching', 'running')
       AND archived_at IS NULL
       AND last_status IN ('paused', 'cancelled')
       AND done_at IS NULL
       AND leased_at IS NOT NULL
       AND leased_at < NOW() - ($1 || ' milliseconds')::interval`,
    [SUSPENDED_LEASE_SETTLE_MS]
  )
  for (const row of suspendedRows.rows) {
    // 2026-07-17 self-lease-steal shield (see _inFlightDispatchIds).
    if (_inFlightDispatchIds.has(String(row.id))) {
      process.stderr.write('[scheduler] task ' + row.id +
        ' is in-flight in this process, skipping suspended-lease settle\n')
      continue
    }
    const liveWorker = await hasLiveWorkerForTask(row.id, PROTECT_LIVE_WORKERS)  // this branch prunes the worktree below
    if (liveWorker) {
      process.stderr.write(
        '[scheduler] task ' + row.id + ' has live worker ' + liveWorker.tab_id +
        ' (stale_ms=' + liveWorker.stale_ms + '), skipping suspended-lease settle\n'
      )
      continue
    }
    let closeOk = false
    if (row.dispatched_tab_id && dispatcherForSuspended && dispatcherForSuspended.kill_worker) {
      try {
        const r = await dispatcherForSuspended.kill_worker({ tab_id: row.dispatched_tab_id })
        closeOk = !!(r && r.closed)
      } catch (e) {
        process.stderr.write('[scheduler] suspended-lease settle kill_worker tolerated error for ' +
          row.id + ': ' + e.message + '\n')
      }
    }
    const tabFrag = closeOk ? 'dispatched_tab_id = NULL,' : ''
    // Re-assert every column the SELECT matched: this loop awaits a coord network
    // call per row, so the row moves under us (lane D1 pass 3 note above).
    await pool.query(
      `UPDATE os_scheduled_tasks
       SET status = last_status,
           last_error = 'suspended mid-lease: paused/cancelled while status was ' ||
                        status || '; settled so the lane releases (resume with schedule_resume)',
           leased_by = NULL, leased_at = NULL, ${tabFrag} updated_at = NOW()
       WHERE id = $1
         AND status IN ('dispatching', 'running')
         AND archived_at IS NULL
         AND last_status IN ('paused', 'cancelled')
         AND done_at IS NULL`,
      [row.id]
    )
    try { await exports.pruneWorktreeForRow(row) } catch (_e) {}
  }

  // 1. Stale dispatching, retryable. 2026-06-21 reclaim-vs-bind race fix
  // (status_board 128b7c82): converted from a bulk UPDATE to SELECT + per-row
  // UPDATE so this branch consults coord worker liveness BEFORE reclaiming, exactly
  // as branches 2a/2b/3 already do. The bulk form reclaimed ANY dispatching row
  // whose leased_at aged past STALE_DISPATCHING_MS (15min) with NO liveness check,
  // but the observed p99 cold-bind tail (~21min, see the SIGNAL_BOUND_TIMEOUT_MS
  // note above) EXCEEDS that window: a slow-but-live worker still inside its
  // legitimate bind / early-work window had its lease freed, next_run_at re-armed,
  // retry_count bumped, and the next 30s poll re-dispatched the SAME task while the
  // first worker was still alive. dispatchOne's running-flip guard only blocks the
  // zombie half-state; it does NOT stop the duplicate re-dispatch, so the fleet
  // thrashed ('stale lease recovered' grew to 48 active rows 2026-06-20 even after
  // a mitigation restart). Per [[enumerate-all-trigger-paths-when-fixing-data-flow-bugs]]
  // a guard wired into N parallel branches must cover ALL branches of the same
  // routine; branch 1 was the one staleLeaseRecovery branch the liveness gate was
  // never wired into (the comment in branch 3 below even mis-stated it as already
  // covering "branches 1 + 2a + 2b"). A genuinely dead worker (heartbeat stale
  // >= STALE_WORKER_LIVENESS_MS, or no worker row at all) still reclaims. next_run_at
  // re-arm is unchanged: BOUNDED retry backoff per RETRY_BACKOFF_MS +
  // [[cron-rearm-must-recompute-next-run-at-or-guard-reentry-per-period-2026-06-19]].
  const staleRetryableRows = await pool.query(
    `SELECT id, name FROM os_scheduled_tasks
     WHERE status = 'dispatching'
       AND leased_at < NOW() - ($1 || ' milliseconds')::interval
       AND ${exports.EFFECTIVE_RETRY_COUNT_SQL} < $2
       AND archived_at IS NULL
       AND (last_status IS NULL OR last_status NOT IN ('paused', 'cancelled'))`,
    [STALE_DISPATCHING_MS, MAX_RETRY_COUNT]
  )
  for (const row of staleRetryableRows.rows) {
    // 2026-07-17 self-lease-steal shield: never reclaim a row THIS process is
    // still dispatching (queued on the launch-lock or mid-prep) - reclaiming it
    // is stealing our own in-flight dispatch. See _inFlightDispatchIds.
    if (_inFlightDispatchIds.has(String(row.id))) {
      process.stderr.write('[scheduler] task ' + row.id +
        ' is in-flight in this process, skipping stale-lease retry reclaim\n')
      continue
    }
    const liveWorker = await hasLiveWorkerForTask(row.id)  // NOT protected: pure recovery, no prune and no kill_worker
    if (liveWorker) {
      process.stderr.write(
        '[scheduler] task ' + row.id + ' has live worker ' + liveWorker.tab_id +
        ' (stale_ms=' + liveWorker.stale_ms + '), skipping stale-lease retry reclaim\n'
      )
      continue
    }
    await pool.query(
      `UPDATE os_scheduled_tasks
       SET status = 'active', retry_count = ${exports.EFFECTIVE_RETRY_COUNT_SQL} + 1,
           last_error = 'stale lease recovered (next_run_at re-armed to bounded retry backoff)',
           next_run_at = NOW() + ($2 || ' milliseconds')::interval,
           leased_by = NULL, leased_at = NULL, updated_at = NOW()
       WHERE id = $1
         AND status = 'dispatching'
         AND archived_at IS NULL
         AND (last_status IS NULL OR last_status NOT IN ('paused', 'cancelled'))`,
      [row.id, RETRY_BACKOFF_MS]
    )
  }

  // 2a. Stale dispatching, max retries exhausted, CRON row: defer to next
  // scheduled interval and reset retry_count per
  // [[scheduler-no-ide-defer-and-cron-rows-never-permanently-fail-2026-06-02]].
  // Marking a cron row permanently failed loses every future interval of
  // recurring work. markFailed already handles this for the dispatchOne path;
  // this branch fixes the parallel path through the stale-lease sweep.
  // 2026-08-29 lane D1 pass 4: this SELECT was the one of the four missing the
  // `archived_at IS NULL` + last_status guards its own UPDATE re-asserts (branches
  // 1 and 2b carry them on both halves). SCOPE, added by pass 5 so the next reader
  // does not conclude the class was swept: "the four" counts only the
  // status='dispatching' sweeps, 0 / 1 / 2a / 2b. Branch 3's status='running'
  // SELECT has the SAME mismatch on three guards and is deliberately left alone;
  // its own note below says why. An archived or suspended cron row was
  // selected, cost a coord liveness call, no-opped the UPDATE, and then still ran
  // pruneWorktreeForRow, which sits OUTSIDE the guarded write. Every pass, forever.
  // Branch 0 above is what actually settles the suspended half.
  // [[enumerate-all-trigger-paths-when-fixing-data-flow-bugs]]
  const staleCronRows = await pool.query(
    `SELECT id, cron_expression, tz FROM os_scheduled_tasks
     WHERE status = 'dispatching'
       AND leased_at < NOW() - ($1 || ' milliseconds')::interval
       AND ${exports.EFFECTIVE_RETRY_COUNT_SQL} >= $2
       AND type = 'cron'
       AND cron_expression IS NOT NULL
       AND archived_at IS NULL
       AND (last_status IS NULL OR last_status NOT IN ('paused', 'cancelled'))`,
    [STALE_DISPATCHING_MS, MAX_RETRY_COUNT]
  )
  for (const row of staleCronRows.rows) {
    // Liveness gate per
    // [[scheduler-stale-lease-must-check-coord-worker-liveness-before-redispatch-2026-06-10]].
    // If a worker tab is still heartbeating against this task, skip the
    // re-dispatch and leave leased_by + leased_at intact so the lease keeps
    // holding. Freeing the lease here is the thundering-herd anti-pattern
    // (origin: 2026-06-10T04:23Z, telemetry-batch fired 4 sibling workers
    // inside 4 min because cold-start binds breached STALE_DISPATCHING_MS).
    // 2026-07-17 self-lease-steal shield (see _inFlightDispatchIds).
    if (_inFlightDispatchIds.has(String(row.id))) {
      process.stderr.write('[scheduler] task ' + row.id +
        ' is in-flight in this process, skipping stale-lease cron-defer\n')
      continue
    }
    const liveWorker = await hasLiveWorkerForTask(row.id, PROTECT_LIVE_WORKERS)  // this branch prunes the worktree below
    if (liveWorker) {
      process.stderr.write(
        '[scheduler] task ' + row.id + ' has live worker ' + liveWorker.tab_id +
        ' (stale_ms=' + liveWorker.stale_ms + '), skipping stale-lease cron-defer\n'
      )
      continue
    }
    let nextRunAt = null
    try {
      nextRunAt = exports.computeNextRunAt(row)
    } catch (_e) {
      nextRunAt = new Date(Date.now() + 60 * 60 * 1000).toISOString()
    }
    await pool.query(
      `UPDATE os_scheduled_tasks
       SET status = 'active', retry_count = 0,
           last_error = 'stale lease - max retries exhausted (cron: deferred to next interval per doctrine)',
           next_run_at = $1, leased_by = NULL, leased_at = NULL, updated_at = NOW()
       WHERE id = $2
         AND status = 'dispatching'
         AND archived_at IS NULL
         AND (last_status IS NULL OR last_status NOT IN ('paused', 'cancelled'))`,
      [nextRunAt, row.id]
    )
    // 2026-06-10 branch-thrash guard cleanup on cron-defer recovery.
    try { await exports.pruneWorktreeForRow(row) } catch (_e) {}
  }

  // 2b. Stale dispatching, max retries exhausted, NON-CRON row: permanently
  // fail. One-shot delayed work is genuinely done after 3 stale leases.
  // Converted from a single bulk UPDATE to SELECT + per-row UPDATE on
  // 2026-06-10 so the coord-liveness gate can consult per task_id. The
  // bulk UPDATE could not consult coord per row and re-dispatched mid-flight
  // workers (incident 2026-06-10T04:23Z, telemetry-batch).
  const staleNonCronRows = await pool.query(
    `SELECT id FROM os_scheduled_tasks
     WHERE status = 'dispatching'
       AND leased_at < NOW() - ($1 || ' milliseconds')::interval
       AND ${exports.EFFECTIVE_RETRY_COUNT_SQL} >= $2
       AND (type != 'cron' OR cron_expression IS NULL)
       AND archived_at IS NULL
       AND (last_status IS NULL OR last_status NOT IN ('paused', 'cancelled'))`,
    [STALE_DISPATCHING_MS, MAX_RETRY_COUNT]
  )
  for (const row of staleNonCronRows.rows) {
    // 2026-07-17 self-lease-steal shield (see _inFlightDispatchIds).
    if (_inFlightDispatchIds.has(String(row.id))) {
      process.stderr.write('[scheduler] task ' + row.id +
        ' is in-flight in this process, skipping stale-lease fail\n')
      continue
    }
    const liveWorker = await hasLiveWorkerForTask(row.id, PROTECT_LIVE_WORKERS)  // this branch prunes the worktree below
    if (liveWorker) {
      process.stderr.write(
        '[scheduler] task ' + row.id + ' has live worker ' + liveWorker.tab_id +
        ' (stale_ms=' + liveWorker.stale_ms + '), skipping stale-lease fail\n'
      )
      continue
    }
    await pool.query(
      `UPDATE os_scheduled_tasks
       SET status = 'failed', retry_count = retry_count + 1,
           last_error = 'stale lease - max retries exhausted',
           leased_by = NULL, leased_at = NULL, updated_at = NOW()
       WHERE id = $1
         AND status = 'dispatching'
         AND archived_at IS NULL
         AND (last_status IS NULL OR last_status NOT IN ('paused', 'cancelled'))`,
      [row.id]
    )
    // 2026-06-10 branch-thrash guard cleanup on permanent fail.
    try { await exports.pruneWorktreeForRow(row) } catch (_e) {}
  }

  // 3. Running too long -> orphaned (non-cron) OR deferred to next interval (cron).
  //
  // 2026-05-29 ultracode audit H3 fix. Was: status='orphaned' only. The
  // orphan row's dispatched_tab_id was never cleaned, and startupCleanup
  // filters on last_run_at > NOW() - 24h - orphan rows have stale/null
  // last_run_at so they were permanently excluded. Tab leaked forever.
  // Now: identify orphans, kill_worker on each (best-effort), gate the
  // tab_id NULL on closed:true (mirrors markComplete), and set last_run_at
  // so a future startupCleanup or cleanup_orphan_workers sweep can still
  // target the stored tab_handle on disk.
  //
  // 2026-06-09 fix: cron rows in this branch were also being permanently
  // terminated as 'orphaned', stranding 23 corpus rows after the 24h
  // signal_bound regression window (08-09 June). Per
  // [[scheduler-no-ide-defer-and-cron-rows-never-permanently-fail-2026-06-02]],
  // cron rows must defer to their next scheduled interval; only one-shot
  // (delayed/chained) rows go to terminal status='orphaned'.
  // 2026-06-22 cadence-aware window: a stuck-running CRON row is eligible after
  // RUNNING_CRON_ORPHAN_MS (30min); a one-shot (non-cron) keeps the 6h
  // ORPHAN_TIMEOUT_MS. Both still pass through the per-row liveness gate below,
  // so this only TIGHTENS eligibility for fast crons, never bypasses the gate.
  // 2026-08-29 lane D1 pass 5: VERIFIED MISMATCHED, DELIBERATELY NOT TIGHTENED.
  // The SELECT below carries NO `archived_at IS NULL`, NO `done_at IS NULL` and no
  // last_status guard, and BOTH of its UPDATEs re-assert all three. So an archived,
  // done or suspended running row past its timeout is selected, costs a coord
  // liveness call, fires kill_worker on its tab, runs pruneWorktreeForRow, then
  // no-ops the write, on every pass. That is branch 2a's fixed defect in a second
  // place. Live population measured 2026-08-29T11:06Z: 0 archived, 0 done_at,
  // 0 suspended, so it is latent rather than leaking.
  // WHY IT IS NOT JUST FIXED: adding the three guards ALSO removes the kill_worker
  // that this branch is today the only path performing on an archived row stuck in
  // 'running'. Deleting a real (if repeating) cleanup to silence churn on a class
  // with zero rows is a behaviour decision, not a tidy-up, so it is boarded on
  // status_board 40fb5c8f rather than shipped inside a verification pass.
  // The suspended slice of this population is already owned by branch 0 above,
  // which pass 5 watched settle a REAL running row at 11:09:44Z.
  const orphans = await pool.query(
    `SELECT id, dispatched_tab_id, type, cron_expression, tz FROM os_scheduled_tasks
     WHERE status = 'running'
       AND (
         (type = 'cron' AND cron_expression IS NOT NULL
            AND leased_at < NOW() - ($1 || ' milliseconds')::interval)
         OR
         ((type <> 'cron' OR cron_expression IS NULL)
            AND leased_at < NOW() - ($2 || ' milliseconds')::interval)
       )`,
    [RUNNING_CRON_ORPHAN_MS, ORPHAN_TIMEOUT_MS]
  )
  const dispatcher = getDispatcher()
  for (const row of orphans.rows) {
    // Liveness gate per
    // [[scheduler-stale-lease-must-check-coord-worker-liveness-before-redispatch-2026-06-10]].
    // 2026-06-18: the 2026-06-10 fix wired this gate into branches 1 + 2a + 2b
    // (the status='dispatching' sweeps) but MISSED this branch 3 (the
    // status='running' orphan-timeout sweep). A cron worker still heartbeating
    // but running past ORPHAN_TIMEOUT_MS (6h) had its row flipped back to
    // 'active' with a fresh next_run_at WITHOUT a liveness check, so the next
    // poll re-leased (re-dispatched) the task while the prior tab was still
    // finishing. That is the exact double-fire path behind the morning dup
    // bursts (climate-pm-chain + orphan-next-action-audit) and the
    // partnership-watering double-dispatch on 2026-06-14. When a worker tab is
    // still alive for this task, SKIP the reclaim and leave status='running' +
    // leased_by + leased_at intact so the lease keeps holding (mirrors the
    // skip in branches 2a + 2b). Once heartbeats stop, hasLiveWorkerForTask
    // returns null within STALE_WORKER_LIVENESS_MS and the next sweep reclaims.
    // Doctrine: [[enumerate-all-trigger-paths-when-fixing-data-flow-bugs]] -
    // a guard wired into N parallel branches must cover ALL branches of the
    // same routine.
    const liveWorker = await hasLiveWorkerForTask(row.id, PROTECT_LIVE_WORKERS)  // THE 30min cron reclaim: kill_worker + prune. status_board 8d28c32d
    if (liveWorker) {
      process.stderr.write(
        '[scheduler] task ' + row.id + ' has live worker ' + liveWorker.tab_id +
        ' (stale_ms=' + liveWorker.stale_ms + '), skipping running orphan-timeout reclaim\n'
      )
      continue
    }
    let closeOk = false
    if (row.dispatched_tab_id && dispatcher.kill_worker) {
      try {
        const r = await dispatcher.kill_worker({ tab_id: row.dispatched_tab_id })
        closeOk = !!(r && r.closed)
      } catch (e) {
        process.stderr.write('[scheduler] orphan kill_worker tolerated error for ' + row.id + ': ' + e.message + '\n')
      }
    }
    const tabFrag = closeOk ? 'dispatched_tab_id = NULL,' : ''
    if (row.type === 'cron' && row.cron_expression) {
      let nextRunAt = null
      try {
        nextRunAt = exports.computeNextRunAt(row)
      } catch (_e) {
        nextRunAt = new Date(Date.now() + 60 * 60 * 1000).toISOString()
      }
      await pool.query(
        `UPDATE os_scheduled_tasks
         SET status = 'active', retry_count = 0, last_run_at = NOW(),
             next_run_at = $1,
             last_error = 'running orphan-timeout (cron: deferred to next interval per doctrine)',
             leased_by = NULL, leased_at = NULL, ${tabFrag} updated_at = NOW()
         WHERE id = $2
           AND status = 'running'
           AND done_at IS NULL
           AND archived_at IS NULL
           AND (last_status IS NULL OR last_status NOT IN ('paused', 'cancelled'))`,
        [nextRunAt, row.id]
      )
    } else {
      await pool.query(
        `UPDATE os_scheduled_tasks
         SET status = 'orphaned', last_run_at = NOW(), ${tabFrag} updated_at = NOW()
         WHERE id = $1
           AND status = 'running'
           AND done_at IS NULL
           AND archived_at IS NULL
           AND (last_status IS NULL OR last_status NOT IN ('paused', 'cancelled'))`,
        [row.id]
      )
    }
    // 2026-06-10 branch-thrash guard cleanup on orphan + cron-defer paths.
    try { await exports.pruneWorktreeForRow(row) } catch (_e) {}
  }
}

// ── livenessReapPass ─────────────────────────────────────────────────────────
//
// 2026-08-28, coord rebuild lane R1. A row in status='running' held its lane shut
// against the new lane-defer predicate in leaseDueRows, so a dead holder would
// pin a lane forever. This is the counterweight: settle rows that are PROVABLY
// dead, from an observed signal, on a 60s cadence instead of the 6h orphan timer.
//
// It exists because the pre-existing reclaim path could not be trusted to do it.
// That path gates on hasLiveWorkerForTask, which reads the coord heartbeat, and
// the heartbeat declares a worker dead after 90 SECONDS of not calling coord.
// Measured 2026-08-28T06:26Z: coord.list_workers returned 1 while 24 sessions had
// written a transcript turn in the previous three minutes. A signal that wrong in
// the unsafe direction cannot be the thing that decides to kill a worker, so its
// only remaining gate was the 6h timeout, which is far too slow to free a lane.
//
// tools/worker-liveness.js supplies the verdict from transcript mtime (a file the
// harness writes every turn with no cooperation from the model) with a worktree
// mtime fallback. Only verdict 'dead' is actionable: 'unknown' covers the boot
// race, a missing tab id and any probe error, and every one of those means leave
// the row alone. Reaping nothing is always safe.
//
// Settle semantics deliberately MIRROR the orphan-timeout path above: a cron row
// returns to 'active' at its next boundary (it will fire again on schedule), a
// one-shot goes terminal as 'orphaned'. Either way the lane reopens on the next
// lease tick, which is what makes the defer predicate safe to ship.
exports.livenessReapPass = async function livenessReapPass(opts) {
  opts = opts || {}
  const pool = opts.pool || getPool()
  const liveness = opts.liveness || require('./worker-liveness')
  const dispatcher = opts.dispatcher || getDispatcher()
  const res = await pool.query(
    `SELECT id, name, type, cron_expression, status, leased_at, dispatched_tab_id,
            bound_at, launch_retry_count
       FROM os_scheduled_tasks WHERE status = 'running' AND archived_at IS NULL`
  )
  if (!res.rows.length) return { scanned: 0, reaped: 0, live: 0, unknown: 0, verdicts: [] }

  let verdicts
  try {
    verdicts = liveness.probeRows(res.rows, opts)
  } catch (e) {
    // Probe failure is not evidence of death. Change nothing.
    process.stderr.write('[scheduler] livenessReapPass probe error (reaping nothing): ' + e.message + '\n')
    return { scanned: res.rows.length, reaped: 0, live: 0, unknown: res.rows.length, error: e.message, verdicts: [] }
  }

  const byId = new Map(res.rows.map(r => [String(r.id), r]))
  let reaped = 0, live = 0, unknown = 0
  for (const v of verdicts) {
    if (v.verdict === 'live') { live++; continue }
    if (v.verdict !== 'dead') { unknown++; continue }
    const row = byId.get(String(v.id))
    if (!row) { unknown++; continue }

    let closeOk = false
    if (row.dispatched_tab_id && dispatcher && dispatcher.kill_worker) {
      try {
        const r = await dispatcher.kill_worker({ tab_id: row.dispatched_tab_id })
        closeOk = !!(r && r.closed)
      } catch (e) {
        process.stderr.write('[scheduler] livenessReapPass kill_worker tolerated error for ' + row.id + ': ' + e.message + '\n')
      }
    }
    const tabFrag = closeOk ? 'dispatched_tab_id = NULL,' : ''
    const why = 'liveness-reap: ' + v.reason + ' (' + JSON.stringify(v.evidence) + ')'

    if (row.type === 'cron' && row.cron_expression) {
      let nextRunAt = null
      try { nextRunAt = exports.computeNextRunAt(row) }
      catch (_e) { nextRunAt = new Date(Date.now() + 60 * 60 * 1000).toISOString() }
      await pool.query(
        `UPDATE os_scheduled_tasks
         SET status = 'active', retry_count = 0, last_run_at = NOW(), next_run_at = $1,
             last_error = $3, leased_by = NULL, leased_at = NULL, ${tabFrag} updated_at = NOW()
         WHERE id = $2 AND status = 'running' AND archived_at IS NULL
           AND (last_status IS NULL OR last_status NOT IN ('paused', 'cancelled'))`,
        [nextRunAt, row.id, why.slice(0, 900)]
      )
    } else if (!row.bound_at) {
      // C-i, 2026-09-05 lane C1. A one-shot that leased and never bound never
      // STARTED. Measured over 14 days: 89 of 194 leased rows died here, every one
      // of them carrying this same reap reason at retry_count = 0, and every one
      // of them terminal, because 0 orphaned rows carry a future next_run_at. The
      // Mac failing to open a tab (usage cap, memory pressure, the IDE tab
      // ceiling) was being recorded as the job failing, and the job was then
      // silently lost. It is the largest single class in the whole funnel.
      //
      // So relaunch it, under a ceiling, and refuse the relaunch in the three
      // cases where retrying is worse than stopping. The decision is in
      // dispatch-settle.launchFailureDecision so it is testable without a tick.
      const settleMod = opts.settle || require('./dispatch-settle')
      let laneHeld = false, laneKey = null
      try {
        const lh = await pool.query(
          `SELECT os_sched_lane_key($2) AS lane,
                  EXISTS (SELECT 1 FROM os_scheduled_tasks h
                           WHERE h.id <> $1 AND h.archived_at IS NULL
                             AND h.status IN ('active', 'paused', 'running', 'dispatching')
                             AND os_sched_lane_key(h.name) IS NOT NULL
                             AND os_sched_lane_key(h.name) = os_sched_lane_key($2)) AS held`,
          [row.id, row.name]
        )
        laneKey = lh.rows[0] && lh.rows[0].lane
        laneHeld = !!(laneKey && lh.rows[0].held)
      } catch (e) {
        // Unresolvable lane state fails CLOSED: treat it as held, which defers.
        laneHeld = true
        process.stderr.write('[scheduler] launch-failure lane probe failed for ' + row.id + ': ' + e.message + '\n')
      }
      const capped = exports._getCappedOutageState().firstDeferAt !== null
      const d = settleMod.launchFailureDecision(row, { capped, laneHeld, laneKey })

      if (d.action === 'relaunch') {
        await pool.query(
          `UPDATE os_scheduled_tasks
           SET status = 'active', next_run_at = $2,
               launch_retry_count = COALESCE(launch_retry_count, 0) + 1,
               leased_by = NULL, leased_at = NULL, last_run_at = NOW(),
               last_error = $3, ${tabFrag} updated_at = NOW()
           WHERE id = $1 AND status = 'running' AND archived_at IS NULL
             AND bound_at IS NULL AND done_at IS NULL
             AND (last_status IS NULL OR last_status NOT IN ('paused', 'cancelled'))`,
          [row.id, d.next_run_at, ('launch-failure relaunch: ' + d.reason).slice(0, 900)]
        )
        process.stderr.write('[scheduler] launch-failure RELAUNCH ' + row.id + ' (' + (row.name || '?') +
          ') attempt ' + d.attempts + '/' + settleMod.MAX_LAUNCH_RETRIES + ' at ' + d.next_run_at + '\n')
      } else if (d.action === 'escalate') {
        // Loudly. Repeated launch failure is not this row's problem, it is the
        // whole fleet degraded, and a quiet retry ceiling would hide exactly that.
        await pool.query(
          `UPDATE os_scheduled_tasks
           SET status = 'settled-no-trace', settled_at = NOW(),
               settle_verdict = 'launch-failure',
               settle_evidence = $2::jsonb,
               last_run_at = NOW(), last_error = $3, ${tabFrag} updated_at = NOW()
           WHERE id = $1 AND status = 'running' AND archived_at IS NULL AND done_at IS NULL`,
          [row.id,
           JSON.stringify({ launch_failure: true, attempts: d.attempts, ceiling: settleMod.MAX_LAUNCH_RETRIES,
                            liveness_reason: v.reason, transcript: v.evidence,
                            settled_by: 'livenessReapPass.launch-failure-ceiling',
                            note: 'the Mac could not open a tab for this row after ' + d.attempts +
                                  ' attempts; the fleet is out of launch capacity' }),
           ('launch-failure ceiling: ' + d.reason).slice(0, 900)]
        )
        process.stderr.write('[scheduler] LAUNCH-FAILURE CEILING ' + row.id + ' (' + (row.name || '?') +
          '): ' + d.reason + ' -- the Mac is out of launch capacity\n')
      } else {
        // Deferred (capped, or the lane is held). Leave the row exactly where the
        // pre-existing behaviour left it so nothing regresses, and say why.
        await pool.query(
          `UPDATE os_scheduled_tasks
           SET status = 'orphaned', last_run_at = NOW(), last_error = $2, ${tabFrag} updated_at = NOW()
           WHERE id = $1 AND status = 'running' AND archived_at IS NULL`,
          [row.id, ('launch-failure not relaunched: ' + d.reason + ' | ' + why).slice(0, 900)]
        )
        process.stderr.write('[scheduler] launch-failure DEFER ' + row.id + ': ' + d.reason + '\n')
      }
    } else {
      await pool.query(
        `UPDATE os_scheduled_tasks
         SET status = 'orphaned', last_run_at = NOW(), last_error = $2, ${tabFrag} updated_at = NOW()
         WHERE id = $1 AND status = 'running' AND archived_at IS NULL`,
        [row.id, why.slice(0, 900)]
      )
    }
    try { await exports.pruneWorktreeForRow(row) } catch (_e) {}
    reaped++
    process.stderr.write('[scheduler] livenessReapPass settled ' + row.id + ' (' + (row.name || '?') + '): ' + v.reason + '\n')
  }
  return { scanned: res.rows.length, reaped, live, unknown, verdicts }
}

// ── runCountAnomalyAudit ─────────────────────────────────────────────────────
//
// Dispatcher self-audit canary. Scans non-archived cron rows and flags any whose
// lifetime run_count is implausibly high for the row's age given its schedule
// (e.g. an annual cron created two weeks ago with run_count > 1). This is the
// observability that turns the off-cadence re-fire bug from "found at the monthly
// review" into "alarmed on day one". Read-only; returns the anomaly list and logs
// each to stderr so the scheduler-health canary / log drain surfaces it. Per
// [[cron-rearm-must-recompute-next-run-at-or-guard-reentry-per-period-2026-06-19]]
// fix #3 and [[scheduler-health-canary-and-cron-dupe-guard-2026-06-10]].
exports.runCountAnomalyAudit = async function runCountAnomalyAudit() {
  const pool = getPool()
  const ref = new Date()
  let rows
  try {
    const r = await pool.query(
      `SELECT id, name, type, cron_expression, tz, run_count, created_at
         FROM os_scheduled_tasks
        WHERE type = 'cron'
          AND cron_expression IS NOT NULL
          AND archived_at IS NULL
          AND (last_status IS NULL OR last_status NOT IN ('cancelled'))`
    )
    rows = r.rows
  } catch (e) {
    process.stderr.write('[scheduler] runCountAnomalyAudit query error: ' + e.message + '\n')
    return []
  }
  const anomalies = []
  for (const row of rows) {
    const a = exports.runCountAnomalyForRow(row, ref)
    if (a && a.anomalous) {
      anomalies.push(a)
      process.stderr.write(
        '[scheduler] RUN_COUNT ANOMALY ' + a.id + ' (' + (a.name || '?') + '): run_count=' +
        a.runCount + ' expectedMax=' + a.expectedMax + ' periodMs=' + a.periodMs +
        ' ageDays=' + (a.ageMs / 86400000).toFixed(1) + ' - off-cadence re-fire fingerprint\n'
      )
    }
  }
  return anomalies
}

// ── startupCleanup ───────────────────────────────────────────────────────────
//
// On boot, find recently completed/failed/orphaned rows that still have a
// dispatched_tab_id and try to close those tabs + null out the column.
// Tolerate all errors - cleanup is best-effort.

exports.startupCleanup = async function startupCleanup() {
  let rows = []
  try {
    const pool = getPool()
    const result = await pool.query(
      `SELECT id, dispatched_tab_id FROM os_scheduled_tasks
       WHERE status IN ('completed', 'failed', 'orphaned')
         AND last_run_at > NOW() - INTERVAL '24 hours'
         AND dispatched_tab_id IS NOT NULL
       LIMIT 20`
    )
    rows = result.rows
  } catch (e) {
    process.stderr.write('[scheduler] startupCleanup query error: ' + e.message + '\n')
    return
  }

  const dispatcher = getDispatcher()
  for (const row of rows) {
    let closeOk = false
    try {
      if (dispatcher.kill_worker) {
        const r = await dispatcher.kill_worker({ tab_id: row.dispatched_tab_id })
        closeOk = !!(r && r.closed)
      }
    } catch (e) {
      // Tolerate - tab may already be closed.
    }
    if (!closeOk) continue  // 2026-05-29 H2: retain handle so the cleanup sweep can target it
    try {
      const pool = getPool()
      await pool.query(
        `UPDATE os_scheduled_tasks SET dispatched_tab_id = NULL, updated_at = NOW() WHERE id = $1`,
        [row.id]
      )
    } catch (e) {
      process.stderr.write('[scheduler] startupCleanup update error for ' + row.id + ': ' + e.message + '\n')
    }
  }
}

// ── usage-cap observer (Phase 7) ─────────────────────────────────────────────
//
// Every 5 minutes, checks the current account's headroom. If it is below
// HEADROOM_WARN_THRESHOLD_MIN, writes an observer_signals row so any active CC
// chat sees the warning in its <observer_signals> continuity block on the next
// turn. Anti-spam: 1-hour cooldown per current-account fingerprint.

const HEADROOM_WARN_THRESHOLD_MIN = 15
const CAP_OBSERVER_INTERVAL_MS = 5 * 60 * 1000
const CAP_OBSERVER_COOLDOWN_MS = 60 * 60 * 1000

let _usageModule = null
function getUsageModule() {
  if (!_usageModule) _usageModule = require('./usage')
  return _usageModule
}
exports._setUsageModule = function (mod) { _usageModule = mod }

const _capWarningLast = { account: null, at: 0 }

exports.checkCapWarning = async function checkCapWarning() {
  const current = getCreds().current_account()
  if (current === 'unknown') return { skipped: 'no_current_account' }

  // FIXED 2026-08-02. This has run on a 5-minute interval since it was written and has
  // silently no-oped on every single fire, for three compounding reasons: it indexed
  // stateResult.state[current] when the shape is {state:{accounts:{...}}}; `current` is a
  // short name ('code') while the keys are full emails; and it then wanted a
  // headroom_minutes field that no account row has ever carried. observer_signals has
  // zero rows to this day. Nothing failed loudly because every path returns a `skipped`
  // object, which reads exactly like a healthy no-op.
  const usage = getUsageModule()
  const stateResult = await usage.get_usage_state({})
  const canonical = (usage._normalizeAccount && usage._normalizeAccount(current)) || current
  const accounts = (stateResult && stateResult.state && stateResult.state.accounts) || {}
  const accountState = accounts[canonical] || accounts[current]
  if (!accountState) return { skipped: 'no_state_for_current', looked_for: canonical }

  // Minutes of headroom come from the measured utilization plus the observed burn rate,
  // via the single implementation in usage-real. Its degraded values are deliberate:
  // Infinity when there is no reading, so a blind moment never raises a warning, and 0
  // when an account is measured capped.
  let headroomMin = null
  try {
    headroomMin = require('./usage-real').headroomMinutesFor(accountState)
  } catch (e) {
    headroomMin = typeof accountState.headroom_minutes === 'number' ? accountState.headroom_minutes : null
  }
  if (headroomMin === Infinity) return { skipped: 'no_usage_reading' }

  if (headroomMin === null || headroomMin > HEADROOM_WARN_THRESHOLD_MIN) {
    return { skipped: 'headroom_ample', headroom_min: headroomMin }
  }

  const now = Date.now()
  if (_capWarningLast.account === current && (now - _capWarningLast.at) < CAP_OBSERVER_COOLDOWN_MS) {
    return { skipped: 'cooldown', cooldown_remaining_ms: CAP_OBSERVER_COOLDOWN_MS - (now - _capWarningLast.at) }
  }

  let nextAccount = null
  let nextHeadroom = null
  try {
    nextAccount = await getCreds().pick_healthiest_account({ required_headroom_minutes: 30 })
    if (nextAccount && nextAccount !== current) {
      const nextState = stateResult.state[nextAccount]
      if (nextState) {
        nextHeadroom = typeof nextState.headroom_minutes === 'number'
          ? nextState.headroom_minutes
          : nextState.remaining_minutes
      }
    } else {
      nextAccount = null
    }
  } catch (_e) {
    nextAccount = null
  }

  const message = nextAccount
    ? `Current account (${current}) is capping in ${Math.floor(headroomMin)} minutes. Next-healthiest account (${nextAccount}) has ${nextHeadroom ? Math.floor(nextHeadroom) : '?'} minutes of headroom. When convenient, finish your turn and open a new chat - it will land on ${nextAccount} automatically.`
    : `Current account (${current}) is capping in ${Math.floor(headroomMin)} minutes. All other accounts are also low. Reduce non-urgent work until reset.`

  const fingerprint = `usage_cap:${current}:${Math.floor(headroomMin / 5) * 5}m`

  await getPool().query(
    `INSERT INTO observer_signals (observer_name, signal_kind, message, fingerprint, priority, created_at)
     VALUES ($1, $2, $3, $4, $5, now())`,
    // priority 3, NOT 2. observer_signals carries CHECK (priority IN (1,3,5)), so this
    // insert would have thrown on its very first fire - which is also why the bug above
    // hid so long: the function never reached this line. Probed against the live
    // constraint 2026-08-02 (3 accepted, 2 rejected).
    ['autonomy-substrate-usage-cap-observer', 'usage_cap_warning', message, fingerprint, 3]
  )

  _capWarningLast.account = current
  _capWarningLast.at = now
  return { fired: true, current, next: nextAccount, headroom_min: headroomMin }
}

exports._resetCapWarningLast = function () { _capWarningLast.account = null; _capWarningLast.at = 0 }

// ── CRUD MCP handlers (Phase A1: scheduler unification) ─────────────────────
//
// Three tools exposed as scheduler.schedule_delayed / scheduler.schedule_cron /
// scheduler.schedule_list. They write to os_scheduled_tasks and the existing
// dispatch engine (leaseDueRows -> dispatchOne) picks them up untouched.
//
// All times returned as both _utc (ISO) and _aest (AEST ISO) per the
// UTC-for-machines, AEST-for-Tate doctrine.

const AEST_OFFSET_HOURS = 10  // UTC+10, no DST in Brisbane

function toAestIso(dateLike) {
  if (!dateLike) return null
  const d = (dateLike instanceof Date) ? dateLike : new Date(dateLike)
  if (isNaN(d.getTime())) return null
  const shifted = new Date(d.getTime() + AEST_OFFSET_HOURS * 60 * 60 * 1000)
  return shifted.toISOString().replace('Z', '+10:00')
}

// Parse a delay string into an absolute Date.
//   'in 30m' / 'in 2h' / 'in 3d' / 'in 45s'
//   raw ISO 8601: '2026-06-01T15:00:00Z'
//   numeric ms-from-now: number (seconds-as-number rejected to force unit)
function parseDelay(delay) {
  if (delay == null) throw new Error('delay required')
  if (delay instanceof Date) return delay
  if (typeof delay === 'string') {
    const m = delay.match(/^\s*in\s+(\d+)\s*([smhd])\s*$/i)
    if (m) {
      const n = parseInt(m[1], 10)
      const unit = m[2].toLowerCase()
      const mult = unit === 's' ? 1000
        : unit === 'm' ? 60 * 1000
        : unit === 'h' ? 60 * 60 * 1000
        : 24 * 60 * 60 * 1000
      return new Date(Date.now() + n * mult)
    }
    // Try as ISO 8601.
    const iso = new Date(delay)
    if (!isNaN(iso.getTime())) return iso
    throw new Error('delay must be "in <N>[smhd]" or ISO 8601, got: ' + delay)
  }
  throw new Error('delay must be string, got: ' + typeof delay)
}

// Translate friendly schedule strings to raw cron-expressions.
//   'every 1h' / 'every 30m' / 'every 10s' (every <N>[smh])
//   'daily 09:00' / 'daily 23:30'
//   already-raw 5- or 6-field cron: passes through
// Back-compat shim. The real schedule semantics live in scheduleCore now; this
// only survives for external callers/tests that expect the cron STRING form of a
// cron-kind schedule. Intervals ("every 72h") are NOT cron, so the shim returns
// them unchanged rather than faking a (broken) "0 */N * * *". Internal next-run
// computation no longer goes through here; it uses scheduleCore directly.
function parseSchedule(schedule) {
  if (!schedule || typeof schedule !== 'string') {
    throw new Error('schedule required (string)')
  }
  const c = scheduleCore.classify(schedule)
  if (c && c.kind === 'cron') return c.cron
  return schedule.trim()
}
exports.parseSchedule = parseSchedule

const VALID_PRIORITY_CLASSES = new Set(['normal', 'high', 'low'])
function normalisePriorityClass(pc) {
  if (pc == null || pc === '') return 'normal'
  const v = String(pc).toLowerCase()
  if (v === 'high_fork' || v === 'low_fork') {
    throw new Error('priority_class ' + pc + ' references dead fork substrate; use "normal", "high", or "low"')
  }
  if (!VALID_PRIORITY_CLASSES.has(v)) {
    throw new Error('priority_class must be one of normal|high|low, got: ' + pc)
  }
  return v
}

// Map priority_class -> priority int (1=highest .. 5=lowest).
function priorityForClass(pc) {
  if (pc === 'high') return 1
  if (pc === 'low') return 5
  return 3
}

// 2026-08-13 (report-back v2). Report-back to the conductor is now the DEFAULT for
// interactive schedule_delayed rows. A dispatched worker's signal_done is a MACHINE
// signal that lands in chat.conductor.inbox and NEVER injects a turn, so a chat that
// scheduled the worker and is idle-waiting is never woken - it sits waiting while the
// worker acks, finishes, and closes its own tab. The fix is a PUSH: the worker's
// CLOSING calls coord.message_chat to inject a completion turn back into the conductor
// chat. The row carries only `prompt` from schedule-time to dispatch-time, so the
// directive has to live in the prompt text; mac-dispatcher.js dispatch_worker parses
// `REPORT-BACK: <addr>` and wires composeBrief's CLOSING accordingly.
//
// The prior freezeReportBackOrigin FROZE `origin` to a `chat.label:<slug>.inbox`
// captured from the conductor's title_match. That is REMOVED: title_match is a stale
// window title, and freezing to any guessed label risks the exact misroute this is
// meant to end (2026-08-13 incident). The ONE safe+reliable target is the `conductor`
// keyword, which the sibling coord fix resolves by stored-label/fingerprint and FAILS
// SAFE to the conductor inbox (never guessing a foreground chat). So every default
// lands on `conductor`.
//
// Rules (applied once, at schedule_delayed time; idempotent on the directive-producing
// outputs):
//   - `REPORT-BACK: none` (case-insensitive) -> strip the directive line: explicit
//       opt-out (belt-and-suspenders: dispatch_worker also drops a `none` it sees).
//   - explicit `REPORT-BACK: <addr>` where addr is not origin|parent|self|none
//       -> leave as-is (the author chose a target, e.g. a chat.<...>.inbox).
//   - `REPORT-BACK: origin|parent|self` OR no directive at all
//       -> ensure the prompt carries exactly `REPORT-BACK: conductor`.
// Only schedule_delayed calls this. schedule_cron never does: standing background work
// is inbox-only, consumed by execute-top / turn-hooks, and must NOT inject a turn into
// Tate's active chat on every fire. Doctrine: coord-chat-to-chat-push-delivery-2026-08-02.
function ensureReportBackDefault(prompt) {
  try {
    if (typeof prompt !== 'string') return prompt
    const directiveRe = /^[ \t]*REPORT-BACK:[ \t]*(.+?)[ \t]*$/mi
    const m = prompt.match(directiveRe)
    if (m) {
      const addr = m[1].trim()
      if (/^none$/i.test(addr)) {
        // Opt-out: strip the directive line (and the newline it sits on).
        return prompt.replace(/^[ \t]*REPORT-BACK:[ \t]*none[ \t]*\n?/mi, '')
      }
      if (/^(origin|parent|self)$/i.test(addr)) {
        // Rewrite the keyword to the one safe target. Re-running is a no-op
        // (the output is `conductor`, which falls through untouched below).
        return prompt.replace(directiveRe, 'REPORT-BACK: conductor')
      }
      // Already `conductor` or an author-chosen explicit target -> untouched.
      return prompt
    }
    // No directive at all -> default ON: append a report-back-to-conductor line.
    // Re-running matches the appended directive above and returns unchanged.
    const sep = prompt.endsWith('\n') ? '' : '\n'
    return prompt + sep + '\nREPORT-BACK: conductor\n'
  } catch (e) { return prompt }
}
exports._ensureReportBackDefault = ensureReportBackDefault  // test seam

exports.schedule_delayed = async function schedule_delayed(params) {
  const p = params || {}
  if (!p.name || typeof p.name !== 'string') throw new Error('name required (string)')
  if (!p.prompt || typeof p.prompt !== 'string') throw new Error('prompt required (string)')
  const promptFrozen = ensureReportBackDefault(p.prompt)
  const runAt = parseDelay(p.delay)
  const priorityClass = normalisePriorityClass(p.priority_class)
  const priority = priorityForClass(priorityClass)
  const tz = p.tz || 'Australia/Brisbane'

  const pool = getPool()
  const sql = `
    INSERT INTO os_scheduled_tasks
      (type, name, prompt, run_at, next_run_at, preferred_account, priority, tz, status)
    VALUES ('delayed', $1, $2, $3, $3, $4, $5, $6, 'active')
    RETURNING id, run_at, next_run_at, tz
  `
  const result = await pool.query(sql, [
    p.name,
    promptFrozen,
    runAt.toISOString(),
    p.preferred_account || null,
    priority,
    tz,
  ])
  const row = result.rows[0]
  return {
    ok: true,
    id: row.id,
    type: 'delayed',
    priority_class: priorityClass,
    next_fire_at_utc: new Date(row.next_run_at).toISOString(),
    next_fire_at_aest: toAestIso(row.next_run_at),
  }
}

exports.schedule_cron = async function schedule_cron(params) {
  const p = params || {}
  if (!p.name || typeof p.name !== 'string') throw new Error('name required (string)')
  if (!p.prompt || typeof p.prompt !== 'string') throw new Error('prompt required (string)')
  if (!scheduleCore.isValid(p.schedule)) {
    throw new Error('invalid schedule: "' + p.schedule + '" (use "every Xm|h", "daily HH:MM", "weekly <mon-sun> HH:MM", or a raw cron expression)')
  }
  const tz = p.tz || 'Australia/Brisbane'
  const priorityClass = normalisePriorityClass(p.priority_class)
  const priority = priorityForClass(priorityClass)

  // Store the schedule AS GIVEN. The shared engine reads aliases and raw cron
  // alike; intervals ("every 72h") cannot be expressed as cron so must not be
  // normalised. Compute the first next_run_at via the same engine the rearm uses.
  const cronExpr = String(p.schedule).trim()
  const firstRun = scheduleCore.nextRun(p.schedule, new Date(), tz)
  if (!firstRun) throw new Error('invalid schedule (could not compute next run): "' + p.schedule + '"')

  const pool = getPool()
  const sql = `
    INSERT INTO os_scheduled_tasks
      (type, name, prompt, cron_expression, next_run_at, preferred_account, priority, tz, status)
    VALUES ('cron', $1, $2, $3, $4, $5, $6, $7, 'active')
    RETURNING id, cron_expression, next_run_at, tz
  `
  const result = await pool.query(sql, [
    p.name,
    p.prompt,
    cronExpr,
    firstRun.toISOString(),
    p.preferred_account || null,
    priority,
    tz,
  ])
  const row = result.rows[0]
  return {
    ok: true,
    id: row.id,
    type: 'cron',
    schedule_expression: row.cron_expression,
    tz: row.tz,
    priority_class: priorityClass,
    next_fire_at_utc: new Date(row.next_run_at).toISOString(),
    next_fire_at_aest: toAestIso(row.next_run_at),
  }
}

const ACTIVE_STATUSES = new Set(['active', 'dispatching', 'running'])
const ARCHIVED_STATUSES = new Set(['completed', 'failed', 'orphaned'])

exports.schedule_list = async function schedule_list(params) {
  const p = params || {}
  const limit = Math.min(Math.max(parseInt(p.limit, 10) || 100, 1), 100)
  const archived = p.archived === true

  const clauses = []
  const vals = []
  if (archived) {
    vals.push(Array.from(ARCHIVED_STATUSES))
    clauses.push('status = ANY($' + vals.length + ')')
  } else {
    vals.push(Array.from(ACTIVE_STATUSES))
    clauses.push('status = ANY($' + vals.length + ')')
  }
  if (p.type) {
    vals.push(p.type)
    clauses.push('type = $' + vals.length)
  }
  if (p.name_like) {
    vals.push('%' + p.name_like + '%')
    clauses.push('name ILIKE $' + vals.length)
  }
  vals.push(limit)

  const sql = `
    SELECT id, type, name, prompt, cron_expression, run_at, next_run_at, last_run_at,
           status, run_count, priority, preferred_account, actual_account, tz,
           last_error, last_result, created_at, updated_at
    FROM os_scheduled_tasks
    WHERE ${clauses.join(' AND ')}
    ORDER BY next_run_at ASC NULLS LAST, created_at DESC
    LIMIT $${vals.length}
  `
  const pool = getPool()
  const result = await pool.query(sql, vals)
  const rows = result.rows.map(r => ({
    id: r.id,
    type: r.type,
    name: r.name,
    prompt: (r.prompt || '').slice(0, 200),
    cron_expression: r.cron_expression,
    run_at_utc: r.run_at ? new Date(r.run_at).toISOString() : null,
    run_at_aest: toAestIso(r.run_at),
    next_run_at_utc: r.next_run_at ? new Date(r.next_run_at).toISOString() : null,
    next_run_at_aest: toAestIso(r.next_run_at),
    last_run_at_utc: r.last_run_at ? new Date(r.last_run_at).toISOString() : null,
    status: r.status,
    run_count: r.run_count,
    priority: r.priority,
    preferred_account: r.preferred_account,
    actual_account: r.actual_account,
    tz: r.tz,
    last_error: r.last_error,
    last_result: r.last_result ? r.last_result.slice(0, 200) : null,
  }))
  return { ok: true, count: rows.length, limit, archived, rows }
}

// ── A2 CRUD: cancel / pause / resume / run_now / chain ──────────────────────

// Resolve a row by id (uuid) or name. Returns the row or null.
async function _resolveRow(p) {
  const pool = getPool()
  if (p.id) {
    const r = await pool.query(`SELECT * FROM os_scheduled_tasks WHERE id = $1`, [p.id])
    return r.rows[0] || null
  }
  if (p.name) {
    // Prefer active, fall back to most-recent if multiple matches exist.
    const r = await pool.query(
      `SELECT * FROM os_scheduled_tasks
       WHERE name = $1
       ORDER BY (archived_at IS NULL) DESC, created_at DESC
       LIMIT 1`,
      [p.name]
    )
    return r.rows[0] || null
  }
  throw new Error('id or name required')
}

exports.schedule_cancel = async function schedule_cancel(params) {
  const p = params || {}
  const row = await _resolveRow(p)
  if (!row) return { ok: false, error: 'task not found', id: p.id || null, name: p.name || null }
  const pool = getPool()
  const result = await pool.query(
    // 2026-06-19 cancellation-durability fix: also set status='cancelled', not
    // just last_status + archived_at. Pre-fix, a cancelled cron kept whatever
    // status it had (often 'active'), and any re-arm UPDATE that matched BY ID
    // without an archived_at/last_status guard would resurrect it. The guard is
    // now applied at every status='active' re-arm site (markFailed, markComplete,
    // staleLeaseRecovery), and status itself is made terminal here so the row is
    // internally consistent (status agrees with last_status/archived_at).
    `UPDATE os_scheduled_tasks
     SET status = 'cancelled', archived_at = NOW(), last_status = 'cancelled', updated_at = NOW()
     WHERE id = $1
     RETURNING id, name, archived_at`,
    [row.id]
  )
  const r = result.rows[0]
  return {
    ok: true,
    id: r.id,
    name: r.name,
    cancelled_at_utc: new Date(r.archived_at).toISOString(),
    cancelled_at_aest: toAestIso(r.archived_at),
  }
}

exports.schedule_pause = async function schedule_pause(params) {
  const p = params || {}
  const row = await _resolveRow(p)
  if (!row) return { ok: false, error: 'task not found', id: p.id || null, name: p.name || null }
  if (row.archived_at) {
    return { ok: false, error: 'task is cancelled (archived); cannot pause', id: row.id, name: row.name }
  }
  const pool = getPool()
  const result = await pool.query(
    `UPDATE os_scheduled_tasks
     SET last_status = 'paused', updated_at = NOW()
     WHERE id = $1
     RETURNING id, name, updated_at`,
    [row.id]
  )
  const r = result.rows[0]
  return {
    ok: true,
    id: r.id,
    name: r.name,
    paused_at_utc: new Date(r.updated_at).toISOString(),
    paused_at_aest: toAestIso(r.updated_at),
  }
}

exports.schedule_resume = async function schedule_resume(params) {
  const p = params || {}
  const row = await _resolveRow(p)
  if (!row) return { ok: false, error: 'task not found', id: p.id || null, name: p.name || null }
  if (row.archived_at) {
    return { ok: false, error: 'task is cancelled (archived); cannot resume', id: row.id, name: row.name }
  }
  if (row.last_status !== 'paused') {
    return { ok: false, error: 'task is not paused (last_status=' + (row.last_status || 'null') + ')', id: row.id, name: row.name }
  }

  // Recompute next_run_at:
  //  - cron rows: next interval from now using row.tz
  //  - delayed rows: NOW() (the original delay window has passed during pause)
  //  - other types: NOW()
  let nextRunAt = new Date()
  if (row.type === 'cron' && row.cron_expression) {
    try {
      nextRunAt = new Date(exports.computeNextRunAt(row))
    } catch (cronErr) {
      process.stderr.write('[scheduler] schedule_resume cron-parse error for "' + row.cron_expression + '": ' + cronErr.message + '\n')
    }
  }

  const pool = getPool()
  // Clears the conductor claim too (2026-08-28, lane R1). schedule_resume is the
  // documented "un-stick this row" path, and a claim it left behind would have the
  // dispatcher suppress the row again on the very next fire - a resume that reports
  // ok:true and changes nothing observable. Same reasoning as the lease columns:
  // whatever was holding the row is released here, or the resume is a lie.
  const result = await pool.query(
    `UPDATE os_scheduled_tasks
     SET last_status = NULL, next_run_at = $2,
         claimed_by_tab_id = NULL, claimed_at = NULL,
         updated_at = NOW()
     WHERE id = $1
     RETURNING id, name, next_run_at, updated_at`,
    [row.id, nextRunAt.toISOString()]
  )
  const r = result.rows[0]
  return {
    ok: true,
    id: r.id,
    name: r.name,
    resumed_at_utc: new Date(r.updated_at).toISOString(),
    resumed_at_aest: toAestIso(r.updated_at),
    next_fire_at_utc: new Date(r.next_run_at).toISOString(),
    next_fire_at_aest: toAestIso(r.next_run_at),
  }
}

exports.schedule_run_now = async function schedule_run_now(params) {
  const p = params || {}
  const row = await _resolveRow(p)
  if (!row) return { ok: false, error: 'task not found', id: p.id || null, name: p.name || null }
  if (row.archived_at) {
    return { ok: false, error: 'task is cancelled (archived); cannot run', id: row.id, name: row.name }
  }
  if (row.last_status === 'paused') {
    return { ok: false, error: 'task is paused; resume first', id: row.id, name: row.name }
  }
  const pool = getPool()
  const result = await pool.query(
    `UPDATE os_scheduled_tasks
     SET next_run_at = NOW(), updated_at = NOW()
     WHERE id = $1
     RETURNING id, name, next_run_at`,
    [row.id]
  )
  const r = result.rows[0]
  return {
    ok: true,
    id: r.id,
    name: r.name,
    next_fire_at_utc: new Date(r.next_run_at).toISOString(),
    next_fire_at_aest: toAestIso(r.next_run_at),
  }
}

exports.schedule_chain = async function schedule_chain(params) {
  const p = params || {}
  if (!p.after_task_id || typeof p.after_task_id !== 'string') {
    throw new Error('after_task_id required (string uuid)')
  }
  if (!p.name || typeof p.name !== 'string') throw new Error('name required (string)')
  if (!p.prompt || typeof p.prompt !== 'string') throw new Error('prompt required (string)')

  const pool = getPool()
  // Verify parent exists - chain target must be resolvable.
  const parent = await pool.query(
    `SELECT id, name, status, archived_at FROM os_scheduled_tasks WHERE id = $1`,
    [p.after_task_id]
  )
  if (!parent.rows.length) {
    return { ok: false, error: 'after_task_id not found', after_task_id: p.after_task_id }
  }

  const priorityClass = normalisePriorityClass(p.priority_class)
  const priority = priorityForClass(priorityClass)

  // Chain rows insert with status='active', next_run_at=NULL. markComplete on
  // the parent wakes them via the chain wake-up hook (sets next_run_at=NOW()).
  // type='chained' is one of the allowed values in os_scheduled_tasks_type_check
  // (cron|delayed|chained). leaseDueRows skips parked chain rows by requiring
  // next_run_at IS NOT NULL when chain_after is set.
  const sql = `
    INSERT INTO os_scheduled_tasks
      (type, name, prompt, chain_after, next_run_at, preferred_account, priority, status)
    VALUES ('chained', $1, $2, $3, NULL, $4, $5, 'active')
    RETURNING id, name, chain_after, status, created_at
  `
  const result = await pool.query(sql, [
    p.name,
    p.prompt,
    p.after_task_id,
    p.preferred_account || null,
    priority,
  ])
  const r = result.rows[0]
  return {
    ok: true,
    id: r.id,
    name: r.name,
    type: 'chain',
    chain_after: r.chain_after,
    parent_name: parent.rows[0].name,
    parent_status: parent.rows[0].status,
    priority_class: priorityClass,
    created_at_utc: new Date(r.created_at).toISOString(),
    created_at_aest: toAestIso(r.created_at),
  }
}

// Internals exposed for inline unit testing.
exports._parseDelay = parseDelay
exports._parseSchedule = parseSchedule
exports._normalisePriorityClass = normalisePriorityClass
exports._priorityForClass = priorityForClass
exports._toAestIso = toAestIso
exports._resolveRow = _resolveRow

// ── reapOrphanWorktrees ──────────────────────────────────────────────────────
//
// 2026-06-20 autonomy audit finding (b). Standing GC for leaked dispatched-worker
// worktrees. pruneWorktreeForRow only fires on a row's OWN completion / failure /
// recovery, so a crashed worker or a dispatcher restart mid-dispatch leaks its
// dir forever (5.0 GB / 70 dirs observed 2026-06-20). This sweeps WORKTREE_ROOT
// on boot and removes ONLY dirs that provably cannot lose work and are not live:
//   - the row is not running and not freshly leased (< REAP_LIVE_GRACE_MS), AND
//   - it is a REGISTERED worktree whose worker/<id> branch is fully merged into
//     origin/main AND whose working tree is clean (git status --porcelain empty).
// Unregistered leftover dirs and any dir with unpushed commits / uncommitted
// changes / a live lease are PRESERVED (worktree work must be pushed before
// removal, never orphaned). Best-effort and fully tolerant; returns a tally.
const REAP_LIVE_GRACE_MS = 30 * 60 * 1000
exports.reapOrphanWorktrees = async function reapOrphanWorktrees(opts) {
  const dryRun = !!(opts && opts.dryRun)
  const tally = { scanned: 0, reaped: 0, preserved: 0, skippedLive: 0, reapedIds: [] }
  let entries = []
  try {
    entries = fs.readdirSync(WORKTREE_ROOT, { withFileTypes: true })
      .filter(d => d.isDirectory()).map(d => d.name)
  } catch (_e) { return tally }
  tally.scanned = entries.length
  if (!entries.length) return tally

  // Coord live-worker set, fetched ONCE. FAIL SAFE: if coord is unreachable we
  // cannot tell which worktrees belong to running workers, so we reap NOTHING
  // this pass (the opposite of hasLiveWorkerForTask's fail-open - here a missed
  // live worker means destroying its tree, which is worse than skipping a GC).
  // 2026-06-20: the DB status/lease check ALONE missed the live coord worker
  // 3222c393 (its os_scheduled_tasks row was not running/fresh-leased but its
  // heartbeat was alive); the dry-run caught it before any deletion.
  const liveIds = new Set()
  try {
    // include_dead:true is REQUIRED, and dropping the w.dead test with it.
    // coord.list_workers defaults include_dead=false and filters server-side on
    // its OWN constant (DEAD_HEARTBEAT_MS = 90s), so without this the scheduler
    // documents a 180s policy and silently enforces 90s: every guard below is
    // unreachable for exactly the rows it exists to catch. And because coord
    // still sets dead=true past 90s, asking for the dead rows while keeping
    // `if (w.dead) continue` drops them again client-side, so the flag has to go
    // with it. terminated_at is checked separately, which is the only other
    // thing w.dead contributed. STALE_WORKER_LIVENESS_MS is then the single
    // liveness policy, which was the author's intent.
    // Origin 2026-08-26: a worker inside one long Read/Bash/CDP step goes >90s
    // silent, becomes invisible here, and its LIVE lease is reclaimed and
    // re-dispatched (the same-lane double-dispatch), or its worktree is reaped.
    const r = await getCoord().list_workers({ include_dead: true })
    for (const w of ((r && r.workers) || [])) {
      if (w.terminated_at) continue
      if (typeof w.stale_ms === 'number' && w.stale_ms >= STALE_WORKER_LIVENESS_MS) continue
      if (w.task_id) liveIds.add(String(w.task_id))
    }
  } catch (e) {
    process.stderr.write('[scheduler] reapOrphanWorktrees: coord unavailable, reaping nothing (fail-safe): ' + (e && e.message) + '\n')
    return tally
  }

  await runGit(['fetch', 'origin', 'main', '--quiet']).catch(() => {})
  const registered = new Set()
  try {
    const out = await runGit(['worktree', 'list', '--porcelain'])
    for (const line of String((out && out.stdout) || out || '').split('\n')) {
      if (line.startsWith('worktree ')) registered.add(line.slice('worktree '.length).trim())
    }
  } catch (_e) {}

  const pool = getPool()
  for (const id of entries) {
    const wtPath = path.join(WORKTREE_ROOT, id)
    if (!wtPath.startsWith(WORKTREE_ROOT + path.sep)) continue // safety bound

    // Liveness gate 1: a live coord worker (fresh heartbeat) owns this dir.
    if (liveIds.has(id)) { tally.skippedLive++; continue }

    // Liveness gate 2: running row or fresh lease -> never touch.
    let live = false
    try {
      const r = await pool.query(
        `SELECT status, leased_at FROM os_scheduled_tasks WHERE id = $1`, [id])
      const row = r.rows[0]
      if (row) {
        if (row.status === 'running') live = true
        if (row.leased_at && (Date.now() - new Date(row.leased_at).getTime()) < REAP_LIVE_GRACE_MS) live = true
      }
    } catch (_e) { live = true } // fail safe: cannot confirm dead -> keep
    if (live) { tally.skippedLive++; continue }

    // Safe-to-drop: registered + branch merged + working tree clean. Anything
    // else (unregistered leftover, unpushed branch, dirty tree) is preserved.
    if (!registered.has(wtPath)) { tally.preserved++; continue }
    const merged = await runGit(['merge-base', '--is-ancestor', 'worker/' + id, 'origin/main'])
      .then(() => true).catch(() => false)
    if (!merged) { tally.preserved++; continue }
    let clean = false
    try {
      const st = await execFileP('git', ['-C', wtPath, 'status', '--porcelain'], { timeout: WORKTREE_GIT_TIMEOUT_MS })
      clean = !String((st && st.stdout) || '').trim()
    } catch (_e) { clean = false }
    if (!clean) { tally.preserved++; continue }

    if (dryRun) { tally.reaped++; tally.reapedIds.push(id); continue }
    await runGit(['worktree', 'remove', '--force', wtPath]).catch(() => {})
    await runGit(['worktree', 'prune']).catch(() => {})
    if (fs.existsSync(wtPath)) {
      try { fs.rmSync(wtPath, { recursive: true, force: true }) } catch (_e) {}
      await runGit(['worktree', 'prune']).catch(() => {})
    }
    await runGit(['branch', '-D', 'worker/' + id]).catch(() => {})
    tally.reaped++; tally.reapedIds.push(id)
  }
  return tally
}

// ── wake-stall detection (sleep-resilience) ──────────────────────────────────
//
// Pure + deterministic so the wake path is unit-testable with an injected clock
// instead of fake timers. Returns whether the Node event loop was frozen between
// two consecutive stale-lease ticks (the host slept). `frozenMs` is the observed
// inter-tick gap; `stalled` is true when that gap exceeds intervalMs * factor.
// Doctrine: [[scheduler-must-survive-mac-sleep-2026-06-20]].
exports.detectWakeStall = function detectWakeStall(prevTickMs, nowMs, intervalMs, factor) {
  const f = (typeof factor === 'number' && factor > 1) ? factor : WAKE_STALL_FACTOR
  const frozenMs = nowMs - prevTickMs
  return { stalled: frozenMs > intervalMs * f, frozenMs }
}

// Surface a wake-stall to the conductor / Tate via observer_signals so a silent
// nightly sleep-stall becomes a visible signal on the next CC turn (mirrors the
// usage-cap observer). Best-effort, app-level dedup to one signal per host-hour so
// a single multi-hour freeze does not spam. Doctrine:
// [[health-canary-must-alert-not-silently-accumulate]].
const _wakeStallLast = { hourKey: null }
exports._resetWakeStallLast = function () { _wakeStallLast.hourKey = null }
exports.recordWakeStall = async function recordWakeStall(frozenMs) {
  const mins = Math.round(frozenMs / 60000)
  const hourKey = new Date().toISOString().slice(0, 13)   // dedup per host-hour
  if (_wakeStallLast.hourKey === hourKey) return { skipped: 'cooldown', minutes: mins }
  await getPool().query(
    `INSERT INTO observer_signals (observer_name, signal_kind, message, fingerprint, priority, created_at)
     VALUES ($1, $2, $3, $4, $5, now())`,
    ['scheduler-wake-stall-observer', 'scheduler_wake_stall',
     'Scheduler event loop was frozen ~' + mins + ' min (host sleep/hibernate). Crons leased before the freeze may have stalled in running/dispatching; staleLeaseRecovery ran a catch-up sweep on wake. If this recurs nightly, keep the Mac on AC power or wrap the laptop-agent in `caffeinate -s` (note: critical-battery hibernate ignores caffeinate).',
     'scheduler_wake_stall:' + hourKey, 2]
  )
  _wakeStallLast.hourKey = hourKey
  return { fired: true, minutes: mins }
}

// One stale-lease tick: detect a wake-from-sleep gap, surface it + run an
// immediate catch-up recovery, then run the normal recovery sweep. Extracted from
// the setInterval callback so the wake path is testable with an injected clock.
// `_lastStaleTickMs` is module state seeded by start(); pass nowMs explicitly so
// tests stay deterministic.
let _lastStaleTickMs = null
exports._resetStaleTickClock = function (ms) { _lastStaleTickMs = (typeof ms === 'number') ? ms : null }
exports.staleTick = async function staleTick(nowMs) {
  const now = (typeof nowMs === 'number') ? nowMs : Date.now()
  if (_lastStaleTickMs !== null) {
    const wake = exports.detectWakeStall(_lastStaleTickMs, now, STALE_LEASE_INTERVAL_MS)
    if (wake.stalled) {
      exports._wakeStallCount = (exports._wakeStallCount || 0) + 1
      process.stderr.write(
        '[scheduler] WAKE-STALL: event loop frozen ~' + Math.round(wake.frozenMs / 1000) +
        's (likely host sleep/hibernate) - running immediate catch-up staleLeaseRecovery\n'
      )
      try { await exports.recordWakeStall(wake.frozenMs) } catch (e) {
        process.stderr.write('[scheduler] recordWakeStall error: ' + e.message + '\n')
      }
    }
  }
  _lastStaleTickMs = now
  await exports.staleLeaseRecovery()
}
exports._wakeStallCount = 0

// ── dispatch-loop watchdog (2026-07-17 silent-death self-heal) ───────────────
//
// THE DEATH MODE this guards against (root cause of the 2026-07-16/17 fleet
// stall): the dispatch loop is a non-reentrant setInterval guarded by
// _dispatchPassRunning, reset ONLY in the pass's own finally. If any await
// inside a pass never settles (an unbounded query hang - now bounded by the
// pool query_timeout added the same day - or any other stuck sub-await), the
// finally never runs, the guard latches TRUE, and every future 30s tick no-ops
// at `if (running) return`. The interval keeps firing and the process stays
// alive, so launchd KeepAlive never restarts it: the loop is dead while the
// process is green. The pool query_timeout closes the common (DB) vector; this
// watchdog is defense-in-depth for ANY other stuck await, and the self-healing
// backstop the incident proved was missing (nothing detected the stuck pass or
// re-armed leasing; the external scheduler-health canary only ALARMS into the
// next session, it does not heal).
//
// State is module-scope (not a start() local) so the watchdog tick and the
// /api/health snapshot can read + reset it, and so it is unit-testable.
let _dispatchPassRunning = false
let _dispatchPassStartedAt = 0     // ms; when the in-flight pass began (0 = idle)
let _lastLeasePassAt = 0           // ms; when a lease pass last COMPLETED (heartbeat)
let _dispatchWedgeResets = 0       // count of watchdog force-resets (health signal)
let _wedgeStallLast = { hourKey: null }

// Ceiling for a LEGIT dispatch pass: DISPATCH_LIMIT rows, each up to a full
// SIGNAL_BOUND_TIMEOUT_MS bind wait + ~60s prep, plus slack. A pass older than
// this is not slow, it is wedged. Computed from the live constants so it tracks
// any future retune of the limit or bind timeout.
const DISPATCH_PASS_MAX_MS = DISPATCH_LIMIT * (SIGNAL_BOUND_TIMEOUT_MS + 60_000) + 120_000
const DISPATCH_WATCHDOG_INTERVAL_MS = 60_000

// Testable core. Returns {wedged, ageMs}. On a wedge it force-releases the guard
// so the NEXT dispatch tick runs a fresh pass (the leaked hung promise, if any,
// stays pending but harmless - query_timeout will reject it), logs loudly, and
// records an observer_signal so the wedge is externally visible within a minute
// instead of only via the 2h-latency overdue-actives canary.
exports.dispatchWatchdogTick = async function dispatchWatchdogTick(nowMs) {
  const now = (typeof nowMs === 'number') ? nowMs : Date.now()
  if (!_dispatchPassRunning || !_dispatchPassStartedAt) return { wedged: false }
  const ageMs = now - _dispatchPassStartedAt
  if (ageMs <= DISPATCH_PASS_MAX_MS) return { wedged: false, ageMs }
  _dispatchWedgeResets += 1
  _dispatchPassRunning = false
  _dispatchPassStartedAt = 0
  process.stderr.write('[scheduler] WATCHDOG: dispatch pass wedged ~' +
    Math.round(ageMs / 1000) + 's (> ceiling ' + Math.round(DISPATCH_PASS_MAX_MS / 1000) +
    's) - force-released the non-reentrant guard so the next tick re-leases. ' +
    'A stuck await never settled; the loop was leasing NOTHING until this reset. reset#' +
    _dispatchWedgeResets + '\n')
  try { await exports.recordDispatchWedge(ageMs) } catch (e) {
    process.stderr.write('[scheduler] recordDispatchWedge error: ' + e.message + '\n')
  }
  return { wedged: true, ageMs }
}

// Surface a dispatch-loop wedge to the conductor / Tate via observer_signals.
exports.recordDispatchWedge = async function recordDispatchWedge(ageMs) {
  const secs = Math.round(ageMs / 1000)
  const hourKey = new Date().toISOString().slice(0, 13)   // dedup per host-hour
  if (_wedgeStallLast.hourKey === hourKey) return { skipped: 'cooldown' }
  await getPool().query(
    `INSERT INTO observer_signals (observer_name, signal_kind, message, fingerprint, priority, created_at)
     VALUES ($1, $2, $3, $4, $5, now())`,
    ['scheduler-dispatch-watchdog', 'scheduler_dispatch_wedge',
     'Scheduler dispatch loop was WEDGED ~' + secs + 's (a pass await never settled; the non-reentrant guard latched and leasing had STOPPED). The watchdog force-released it and leasing resumed. If this recurs, find the stuck await (most hangs are now bounded by the pool query_timeout added 2026-07-17); check for a coord/tab-open path with no timeout.',
     'scheduler_dispatch_wedge:' + hourKey, 1]
  )
  _wedgeStallLast.hourKey = hourKey
  return { fired: true, seconds: secs }
}

// Health snapshot for /api/health + on-demand probes (the external canary reads
// os_scheduled_tasks, but this gives a direct in-process liveness read).
exports.schedulerHealthSnapshot = function schedulerHealthSnapshot() {
  const now = Date.now()
  return {
    dispatch_pass_running: _dispatchPassRunning,
    dispatch_pass_age_ms: _dispatchPassStartedAt ? (now - _dispatchPassStartedAt) : 0,
    dispatch_pass_max_ms: DISPATCH_PASS_MAX_MS,
    // last_lease_pass_at is stamped on the THROW path too - it proves the interval is
    // ticking, NOT that leasing works. Read last_lease_ok_at for that.
    last_lease_pass_at: _lastLeasePassAt ? new Date(_lastLeasePassAt).toISOString() : null,
    ms_since_last_lease_pass: _lastLeasePassAt ? (now - _lastLeasePassAt) : null,
    last_lease_ok_at: _lastLeaseOkAt ? new Date(_lastLeaseOkAt).toISOString() : null,
    ms_since_last_lease_ok: _lastLeaseOkAt ? (now - _lastLeaseOkAt) : null,
    consecutive_lease_errors: _consecutiveLeaseErrors,
    lease_outage_paged: _leaseErrorPageSent,
    dispatch_wedge_resets: _dispatchWedgeResets,
    armed: !!(exports._intervals && exports._intervals.dispatchInterval),
  }
}

// Test seams.
exports._resetDispatchWatchdogState = function (o) {
  o = o || {}
  _dispatchPassRunning = !!o.running
  _dispatchPassStartedAt = o.startedAt || 0
  _lastLeasePassAt = o.lastLeasePassAt || 0
  _dispatchWedgeResets = o.wedgeResets || 0
  _wedgeStallLast = { hourKey: null }
}
exports._dispatchWatchdogState = function () {
  return { running: _dispatchPassRunning, startedAt: _dispatchPassStartedAt, lastLeasePassAt: _lastLeasePassAt, wedgeResets: _dispatchWedgeResets }
}
exports._intervals = null

// ── start ────────────────────────────────────────────────────────────────────
//
// Starts all three intervals plus the usage-cap observer + the dispatch
// watchdog. Wraps each tick in try/catch so one bad pass does not tank the loop.
// Returns an API-SAFE plain object (never the raw Timeout handles, which are
// circular and made `scheduler.start` over the HTTP API throw "Converting
// circular structure to JSON"); the handles live on exports._intervals for the
// watchdog and tests.

exports.start = function start() {
  process.stderr.write('[scheduler] starting dispatch loop + completion poller + stale-lease recovery + cap observer\n')

  // Non-blocking startup cleanup.
  exports.startupCleanup().catch(e => {
    process.stderr.write('[scheduler] startupCleanup error: ' + e.message + '\n')
  })

  // Non-blocking orphan-worktree GC (2026-06-20 audit finding b). Conservative:
  // only reaps registered worktrees whose branch is merged + tree is clean + not
  // live; preserves everything else. Boot cadence (restarts are the natural GC
  // point); idempotent.
  exports.reapOrphanWorktrees().then(t => {
    process.stderr.write('[scheduler] reapOrphanWorktrees: scanned=' + t.scanned +
      ' reaped=' + t.reaped + ' preserved=' + t.preserved + ' skippedLive=' + t.skippedLive + '\n')
  }).catch(e => {
    process.stderr.write('[scheduler] reapOrphanWorktrees error: ' + e.message + '\n')
  })
  // 2026-06-20 sleep-resilience: reconcile stuck dispatching/running leases
  // IMMEDIATELY on start (boot / KeepAlive restart / first runtime after a host
  // sleep), not only on the first 60s stale-lease tick. startupCleanup above only
  // closes leaked tabs - it does NOT recover leases. Seeds the wake-stall clock so
  // the first scheduled tick measures the gap from now.
  exports._resetStaleTickClock(Date.now())
  exports.staleLeaseRecovery().catch(e => {
    process.stderr.write('[scheduler] startup staleLeaseRecovery error: ' + e.message + '\n')
  })

  // Dispatch loop. NON-REENTRANT (2026-07-17 self-lease-steal race fix, see
  // _inFlightDispatchIds): a dispatch pass can hold the launch-lock for up to
  // SIGNAL_BOUND_TIMEOUT_MS per row, far longer than POLL_INTERVAL_MS, and a
  // re-entrant setInterval stacked a fresh 5-row batch onto the lock queue
  // every 30s under a due backlog - the queue pileup whose lease-aging fed the
  // steal cycle. One pass at a time; a tick that finds the previous pass still
  // draining just skips (the next tick re-leases whatever is still due).
  const dispatchInterval = setInterval(async () => {
    if (_dispatchPassRunning) return
    _dispatchPassRunning = true
    _dispatchPassStartedAt = Date.now()
    try {
      let rows = []
      try {
        rows = await exports.leaseDueRows(DISPATCH_LIMIT)
      } catch (e) {
        // 2026-09-07 lane D1. This catch used to log and return, and that was the
        // whole of the response: 7h55m of total fleet death on 2026-09-06 produced
        // 24,273 of these lines and not one alert. noteLeaseError counts consecutive
        // throws and pages Tate over iMessage (a path that does not need the DB that
        // is usually the cause). The log line normalises an EMPTY e.message, which is
        // the shape the live log actually carried.
        process.stderr.write('[scheduler] leaseDueRows error: ' +
          (((e && e.message) || String(e || '')).trim() || '(empty message)') + '\n')
        try { exports.noteLeaseError(e) } catch (_pageErr) {}
        return
      }
      // Leasing RETURNED. This is the only proof the lease path works, and it is the
      // only place the recovery flush can honestly run.
      // Logged, not swallowed. A silently-throwing recovery hook would leave the
      // lease-outage counter latched forever and the next real outage unreported,
      // which is the same invisible-failure class this whole block exists to end.
      try { exports.noteLeaseOk() } catch (okErr) {
        process.stderr.write('[scheduler] noteLeaseOk error: ' + ((okErr && okErr.message) || okErr) + '\n')
      }
      // 2026-07-17 batch-aging fix. This loop awaits dispatchOne SERIALLY, so
      // rows 3..N of a leased batch are not yet inside dispatchOne (and thus
      // not in _inFlightDispatchIds) while earlier rows burn bind waits of up
      // to SIGNAL_BOUND_TIMEOUT_MS each. Their leases age past
      // STALE_DISPATCHING_MS while merely WAITING THEIR TURN, and
      // staleLeaseRecovery branch-1 reclaims them (registry miss, no live
      // worker) - the residual "lease lost during dispatch prep" fingerprint
      // with the CURRENT pid observed 3x on 2026-07-17 (rows f1016859,
      // 524a87e4, ccecd418, leased 11:47:20, reclaimed ~12:02:20 mid-queue).
      // Fix: register the WHOLE leased batch in the in-flight Set at lease
      // time. Deregister each row just before its dispatchOne (dispatchOne
      // re-registers on entry and would otherwise dup-skip); the finally
      // sweep clears any rows a thrown error left behind.
      for (const row of rows) exports._inFlightDispatchIds.add(String(row.id))
      try {
        for (const row of rows) {
          exports._inFlightDispatchIds.delete(String(row.id))
          try {
            await exports.dispatchOne(row)
          } catch (e) {
            // dispatchOne already called markFailed; just log.
            process.stderr.write('[scheduler] dispatchOne error for ' + row.id + ': ' + e.message + '\n')
          }
        }
      } finally {
        for (const row of rows) exports._inFlightDispatchIds.delete(String(row.id))
      }
    } catch (e) {
      process.stderr.write('[scheduler] dispatch loop error: ' + e.message + '\n')
    } finally {
      _dispatchPassRunning = false
      _dispatchPassStartedAt = 0
      // NOT a leasing-alive heartbeat, whatever it was named. This finally runs on the
      // THROW path too, so through the whole 2026-09-06 lease outage this stamp stayed
      // fresh every 30s while zero rows were leased. It measures "a pass reached its
      // finally", nothing more. The honest liveness signal is _lastLeaseOkAt, stamped
      // by noteLeaseOk only when leaseDueRows actually returned.
      _lastLeasePassAt = Date.now()
    }
  }, POLL_INTERVAL_MS)

  // Completion pass.
  const completionInterval = setInterval(async () => {
    try {
      await exports.completionPass()
    } catch (e) {
      process.stderr.write('[scheduler] completionPass error: ' + e.message + '\n')
    }
  }, COMPLETION_POLL_INTERVAL_MS)

  // Stale-lease recovery. Routed through staleTick so a wake-from-sleep gap is
  // detected, surfaced (observer_signals), and followed by an immediate catch-up
  // recovery on the first resumed tick. Doctrine:
  // [[scheduler-must-survive-mac-sleep-2026-06-20]].
  const staleInterval = setInterval(async () => {
    try {
      await exports.staleTick(Date.now())
    } catch (e) {
      process.stderr.write('[scheduler] staleLeaseRecovery error: ' + e.message + '\n')
    }
    // 2026-08-28 R1. Runs AFTER staleTick in the same tick, never in parallel, so
    // a reclaim and a liveness reap can never both settle one row in one pass.
    try {
      const t = await exports.livenessReapPass()
      if (t.reaped) {
        process.stderr.write('[scheduler] livenessReapPass: scanned=' + t.scanned +
          ' reaped=' + t.reaped + ' live=' + t.live + ' unknown=' + t.unknown + '\n')
      }
    } catch (e) {
      process.stderr.write('[scheduler] livenessReapPass error: ' + e.message + '\n')
    }
    // 2026-09-05 lane C1. The evidence-settle reconciler, THIRD and last in the
    // same serial tick, so a reclaim, a reap and a settle can never touch one row
    // in one pass.
    //
    // Deliberately in-process and NOT a scheduled cron row. A cron row costs a
    // dispatched tab and a model turn, which means the mechanism that fixes
    // orphaned dispatches would itself be orphanable at the fleet's measured 84
    // pct, and it would sit in the austerity band that currently has 37 cron rows
    // paused. A reconciler you cannot count on running is not a reconciler.
    try {
      const settle = require('./dispatch-settle')
      const r = await settle.settleReconcilerPass({ scheduler: exports })
      if (r.settled || r.skipped_live) {
        process.stderr.write('[scheduler] settleReconcilerPass: scanned=' + r.scanned +
          ' settled=' + r.settled + ' refused_live=' + r.skipped_live + '\n')
      }
    } catch (e) {
      process.stderr.write('[scheduler] settleReconcilerPass error: ' + e.message + '\n')
    }
  }, STALE_LEASE_INTERVAL_MS)

  // 2026-08-28 R1. The inbox retirement sweep, in-process on an hourly timer.
  // It is deliberately NOT a scheduled cron row: a cron row costs a dispatched
  // tab and a model turn to run a pure bookkeeping pass, and the last drain
  // (2026-07-21) was a hand-run one-off precisely because nothing owned it. The
  // whole point of this fix is that retirement is a mechanism, not an errand.
  // Attention is digested weekly-equivalent (anything past 7 days) so a stale
  // escalation lands in a file and a pointer rather than sitting unread forever.
  const retireInterval = setInterval(async () => {
    try {
      const retire = require('./coord-retire')
      const r = await retire.sweep({})
      if (r.retired) {
        process.stderr.write('[scheduler] coord-retire.sweep: retired=' + r.retired +
          ' remaining=' + r.unseen_after + '\n')
      }
      const d = await retire.digest({ older_than_days: 7 })
      if (d.digested) {
        process.stderr.write('[scheduler] coord-retire.digest: digested=' + d.digested +
          ' -> ' + d.digest_file + '\n')
      }
    } catch (e) {
      process.stderr.write('[scheduler] coord-retire error: ' + e.message + '\n')
    }
  }, 60 * 60 * 1000)

  // Usage-cap observer.
  const capObserverInterval = setInterval(async () => {
    try {
      await exports.checkCapWarning()
    } catch (e) {
      process.stderr.write('[scheduler] checkCapWarning error: ' + e.message + '\n')
    }
  }, CAP_OBSERVER_INTERVAL_MS)

// Summarise cleanup_orphan_workers results into an attributable one-liner.
// Families only (the part before the first ':'), so a per-tab id never lands in
// the log. Empty string when there is nothing to say, so a quiet sweep keeps
// printing exactly the line it always printed.
function _orphanSweepBreakdown(r) {
  try {
    const rows = (r && r.results) || []
    // A strategy reads 'stable_tab_id:ttab_x' and a refusal reads
    // 'stable_id_not_live:ttab_x', so a fixed segment count keeps the id in one
    // and drops the reason in the other. Take segments until one looks like a
    // bridge id: that both aggregates (10 closes collapse to one entry instead
    // of ten) and keeps a per-tab handle out of the log.
    const fam = (s) => {
      const parts = String(s || 'unknown').split(':')
      const out = []
      for (const p of parts) {
        if (/^ttab_/.test(p)) break
        out.push(p)
        if (out.length >= 2) break
      }
      return out.length ? out.join(':') : 'unknown'
    }
    const tally = (pred, key) => {
      const c = {}
      for (const x of rows) if (pred(x)) { const k = fam(x[key]); c[k] = (c[k] || 0) + 1 }
      return Object.keys(c).sort((a, b) => c[b] - c[a]).map((k) => k + '=' + c[k])
    }
    const closed = tally((x) => x.action === 'closed', 'strategy')
    const leaked = tally((x) => x.action === 'leak', 'reason').slice(0, 4)
    // 2026-08-29 lane W1 registry GC. A row proven dead by a successful listing
    // is garbage, not a leak, and it now leaves the registry instead of being
    // re-counted every pass. Reported separately so a real ghost tab (a leak we
    // could still close) stays visible instead of drowning in corpses.
    const gone = tally((x) => x.action === 'gone' || x.action === 'gone_pending', 'action')
    let out = ''
    if (closed.length) out += ' [closed: ' + closed.join(' ') + ']'
    if (leaked.length) out += ' [leak: ' + leaked.join(' ') + ']'
    if (gone.length) out += ' [gc: ' + gone.join(' ') + ']'
    return out
  } catch (e) { return '' }
}

  // 2026-05-29 ultracode audit C3 fix. cleanup_orphan_workers reconciles
  // the worker registry against live ide.tabs and closes orphans the strict
  // close path could not match. The audit caught this wired to zero cron -
  // the entire 'better leak than wrong-close' posture assumed this ran on a
  // schedule. Now does, every CLEANUP_ORPHAN_INTERVAL_MS.
  const cleanupOrphanInterval = setInterval(async () => {
    try {
      const dispatcher = getDispatcher()
      if (dispatcher.cleanup_orphan_workers) {
        const r = await dispatcher.cleanup_orphan_workers({ max_age_days: 7, force_untitled: false })
        if (r && (r.closed > 0 || r.candidates > 0)) {
          // 2026-08-29 verification-pass fix. This line printed only the counts,
          // so a non-zero close could not be attributed to a strategy from the
          // log at all: the sweep writes stderr ONLY on the refusal path, and
          // its success path is silent. The lane W1 brief asked its verifier to
          // grep this log for 'stable_tab_id' to prove the stable-id close had
          // fired; that check could never fire by construction, and proving the
          // gate took a forensic walk of ~107 registry .json files instead.
          // The per-row strategy was already in r.results and was simply not
          // being summarised. Attribution now lands in the line itself.
          process.stderr.write('[scheduler] cleanup_orphan_workers: closed=' + r.closed + ' of ' + r.candidates + ' candidates (leaked=' + (r.leaked || 0) + ' gone=' + (r.gone || 0) + ' gone_pending=' + (r.gone_pending || 0) + ')' + _orphanSweepBreakdown(r) + '\n')
        }
      }
    } catch (e) {
      process.stderr.write('[scheduler] cleanup_orphan_workers error: ' + e.message + '\n')
    }
  }, CLEANUP_ORPHAN_INTERVAL_MS)

  // Dispatch-loop watchdog (2026-07-17 silent-death self-heal). Detects a pass
  // whose await never settled (guard latched TRUE, leasing stopped) and force-
  // releases it so the next tick re-leases. Its own tick is try/catch wrapped so
  // a watchdog error can never itself wedge anything.
  const watchdogInterval = setInterval(async () => {
    try {
      await exports.dispatchWatchdogTick(Date.now())
    } catch (e) {
      process.stderr.write('[scheduler] dispatchWatchdogTick error: ' + e.message + '\n')
    }
  }, DISPATCH_WATCHDOG_INTERVAL_MS)

  // Unref so the intervals don't prevent process exit in test environments.
  if (dispatchInterval.unref) dispatchInterval.unref()
  if (completionInterval.unref) completionInterval.unref()
  if (staleInterval.unref) staleInterval.unref()
  if (capObserverInterval.unref) capObserverInterval.unref()
  if (cleanupOrphanInterval.unref) cleanupOrphanInterval.unref()
  if (watchdogInterval.unref) watchdogInterval.unref()
  // 2026-08-28 (lane R1): retireInterval was armed by 04dd65d and then left out of
  // all three of these. Un-unref'd it holds the event loop open so the process
  // cannot exit; absent from exports._intervals neither the watchdog nor a test can
  // see or clear it; absent from the returned list, introspection under-reports what
  // is running. A seventh timer that no surface admits to is the shape of a leak
  // nobody can find later. The count regression this caused (start() calls
  // setInterval 7 times against an assertion of 6) sat undetected because the
  // commit gate was reporting 34 fabricated failures at the time - see the trap
  // note in scripts/run-tests.sh.
  if (retireInterval.unref) retireInterval.unref()

  // Handles live on exports._intervals for the watchdog + tests. The API caller
  // gets a plain, serialisable object - returning the raw Timeout handles made
  // `scheduler.start` over HTTP throw "Converting circular structure to JSON".
  exports._intervals = { dispatchInterval, completionInterval, staleInterval, capObserverInterval, cleanupOrphanInterval, watchdogInterval, retireInterval }
  return { ok: true, armed: true, intervals: ['dispatch', 'completion', 'stale', 'capObserver', 'cleanupOrphan', 'watchdog', 'retire'] }
}
