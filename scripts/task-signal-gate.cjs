// task-signal-gate.cjs - proves the worker handshake BOTH ways, on the real table.
//
// THE QUESTION THIS GATE ASKS is not "does dispatch still work". A handshake has
// two failure directions and a one-directional gate cannot tell them apart:
//
//   RELEASES WHEN IT SHOULD NOT  -> the launch lock opens without a worker, tabs
//                                   flood, and two arcs run the same job.
//   NEVER RELEASES               -> every dispatch in the fleet serialises behind
//                                   a full SIGNAL_BOUND_TIMEOUT_MS of dead wait.
//
// A gate that only proves "a bound releases the lock" scores a lock that was
// never engaged exactly as green as a working one. So section D drives the real
// dispatchOne end to end and measures BOTH: it releases on a legitimate bind, and
// it does NOT release on a bind from a tab that does not own the row.
//
// EVERY REFUSAL LEG ASSERTS THE ABSENCE OF THE WRITE, not the presence of an
// error. On this same lane, 2026-08-28, a deliberate breakage that made a refusal
// quietly file the message anyway left EVERY "REFUSED" assertion passing; only an
// on-disk count caught it. `ok === false` is not evidence that nothing happened.
//
// SECTION A IS POSITIVE AND RUNS FIRST. Without it, an implementation that
// refused everything (a crashed require, a typo'd predicate, an unset pool) would
// sail through every refusal leg below. A crash and a refusal are both a falsy
// ok with something in .error.
//
// The claim statement under test is EXTRACTED FROM dispatchOne's shipped source
// at runtime rather than retyped here, so this cannot pass against a statement
// that has since been edited away (same technique as conductor-claim-gate.cjs and
// lane-defer-gate.cjs).
//
// EVERYTHING runs inside ONE transaction that always ROLLS BACK, and the probe
// row is pinned to next_run_at 2030 besides, so the live 30s dispatch loop can
// never see it and no tab can open. Rows are addressed by id, never by name.
//
// Run: node scripts/task-signal-gate.cjs
// Exit 0 = all legs pass. Exit 1 = a leg failed. Exit 2 = the gate CRASHED,
// which is deliberately distinct: a crash must never be readable as a refusal.

// BOTH of these must be set BEFORE dotenv and before any require below.
//
// The bind timeout: section D deliberately runs waits that must NOT release, and
// the deployed value is 180s, so three of them would take nine minutes. dotenv
// does not override an already-set variable, which is exactly why this goes
// first. Set GATE_BOUND_TIMEOUT_MS to run the legs at the deployed budget.
//
// 2026-08-29 verification pass: raised 3000 -> 6000. Section D's RELEASE legs
// (D1, D9) assert wall-clock elapsed < budget, and wall clock cannot tell a
// released lock from a stalled query. Steady-state round-trip to this database
// is p50 36ms / max 54ms over 40 samples, so the budget was never the systematic
// problem; the observed failures were single-query STALLS of ~2s and ~9s
// (D9 3223ms of 3000ms, D1 10449ms of 3000ms) against a pooler the live daemon
// also uses. A legitimate release costs about one 1000ms poll, so 6000 buys real
// headroom over a 2s stall. It does NOT eliminate the floor: a stall longer than
// the budget still reads as a held lock, so an isolated D1/D9 failure with every
// other leg green is a stall, not a regression - re-run before believing it.
process.env.SCHEDULER_SIGNAL_BOUND_TIMEOUT_MS = process.env.GATE_BOUND_TIMEOUT_MS || '6000'

// COORD_ROOT: section D calls the real coord.signal_done, which still posts a
// worker_report notice to the file substrate. Without this the gate would write
// notices into the LIVE ~/.ecodiaos/coordination inbox that the running
// laptop-agent owns, which is the substrate this whole lane is trying to stop
// filling with machine sediment.
const os = require('os')
const fsx = require('fs')
const OWNED_COORD_ROOT = process.env.COORD_ROOT
  ? null
  : fsx.mkdtempSync(require('path').join(os.tmpdir(), 'task-signal-gate-'))
process.env.COORD_ROOT = process.env.COORD_ROOT || OWNED_COORD_ROOT
// Close what you opened. Only the directory THIS run created, never one handed in.
process.on('exit', () => {
  if (OWNED_COORD_ROOT) { try { fsx.rmSync(OWNED_COORD_ROOT, { recursive: true, force: true }) } catch (e) {} }
})
process.env.COORD_DISABLE_SWEEP = '1'

require('dotenv').config({ path: require('path').join(__dirname, '..', '.env') })

const { Pool } = require('pg')
const scheduler = require('../tools/scheduler')
const taskSignals = require('../tools/task-signals')
const coord = require('../tools/coord')
const creds = require('../tools/creds')

const OWNER_TAB = 'tab_signalgate_owner'
const OTHER_TAB = 'tab_signalgate_other'
const FAR_FUTURE = '2030-01-01T00:00:00Z'
const LANE = 'cowork.signalgate-lane-Z9'

let pass = 0, fail = 0
const ok = (c, l) => { c ? (pass++, console.log('  ok   ' + l)) : (fail++, console.log('  FAIL ' + l)) }

// -- extract the SHIPPED claim statement out of dispatchOne -------------------
// Bounded to the block it belongs to. An extractor that can wander past its own
// block matches an unrelated later statement with a different parameter count and
// dies on a Postgres bind error instead of reporting a failed leg (measured on
// this lane, 2026-08-28).
const SRC = scheduler.dispatchOne.toString()
const MARK = '3b. CLAIM THE ROW FOR THIS DISPATCH'
const iMark = SRC.indexOf(MARK)
const iEnd = (() => {
  const e = SRC.indexOf('4. Wait for signal_bound', iMark < 0 ? 0 : iMark)
  return e > -1 ? e : SRC.length
})()
const BLOCK = iMark < 0 ? '' : SRC.slice(iMark, iEnd)
function shippedStatement(pred) {
  if (!BLOCK) return null
  let i = 0
  for (;;) {
    const a = BLOCK.indexOf('`', i); if (a < 0) return null
    const b = BLOCK.indexOf('`', a + 1); if (b < 0) return null
    const body = BLOCK.slice(a + 1, b)
    if (pred(body)) return body
    i = b + 1
  }
}
// The ADOPT statement (dispatchOne step 2d, the 2026-07-17 double-spawn guard)
// lives BEFORE the 3b block, so it needs its own extractor over the whole source.
// Extracted rather than retyped for the same reason as the claim: a gate that
// replays a hand-copied statement measures the copy, and section G's whole point
// is that this transition reaches running WITHOUT the claim's clear. Retyping it
// here would have scored a clear-at-adopt fix exactly as green as a correct one.
function shippedStatementIn(src, pred) {
  let i = 0
  for (;;) {
    const a = src.indexOf('`', i); if (a < 0) return null
    const b = src.indexOf('`', a + 1); if (b < 0) return null
    const body = src.slice(a + 1, b)
    if (pred(body)) return body
    i = b + 1
  }
}
const UPD_ADOPT_RAW = shippedStatementIn(SRC, b =>
  /UPDATE\s+os_scheduled_tasks/.test(b) && /SET status = 'running', dispatched_tab_id = \$1/.test(b))
const UPD_ADOPT = UPD_ADOPT_RAW
  ? UPD_ADOPT_RAW.replace('${taskSignals.CLEAR_SQL_FRAGMENT}', taskSignals.CLEAR_SQL_FRAGMENT)
  : null

const UPD_CLAIM_RAW = shippedStatement(b => /UPDATE\s+os_scheduled_tasks/.test(b) && /dispatched_tab_id\s*=\s*\$1/.test(b))
// The shipped statement interpolates the clear-list from the module that owns it,
// so the raw source carries the template token rather than the column names.
// Assert the token IS there (deleting the interpolation must fail this gate),
// then expand it exactly as the running code does, and check the EXPANSION for
// all eight columns. Checking the raw source alone would report all eight
// missing on a perfectly correct statement; checking only the expansion would
// pass even if dispatchOne had stopped clearing anything at all.
const CLEAR_TOKEN = '${taskSignals.CLEAR_SQL_FRAGMENT}'
const CLAIM_INTERPOLATES = !!UPD_CLAIM_RAW && UPD_CLAIM_RAW.indexOf(CLEAR_TOKEN) !== -1
const UPD_CLAIM = UPD_CLAIM_RAW
  ? UPD_CLAIM_RAW.replace(CLEAR_TOKEN, require('../tools/task-signals').CLEAR_SQL_FRAGMENT)
  : null

// The eight columns the claim must blank. Read from the module that owns them so
// a column added there without being cleared fails here rather than silently
// carrying a stale signal into the next dispatch.
const LIFECYCLE = taskSignals.LIFECYCLE_COLUMNS

async function main() {
  const pool = new Pool({ connectionString: process.env.DATABASE_URL, connectionTimeoutMillis: 30_000 })
  const client = await pool.connect()
  let taskId = null
  try {
    await client.query('BEGIN')

    // Both the scheduler and the signal sink write through THIS transaction, so
    // the gate exercises the real end-to-end path (coord -> task-signals -> the
    // same rows dispatchOne polls) and every write disappears on rollback.
    const txPool = { query: (sql, params) => client.query(sql, params) }
    scheduler._setPool(txPool)
    taskSignals._setPool(txPool)

    // ── PRE: the shipped statement is present and has the shape claimed ──────
    console.log('\nPRE. the claim statement exists in the shipped dispatchOne')
    ok(!!UPD_CLAIM, 'PRE1. dispatchOne still contains the dispatch-claim UPDATE')
    if (!UPD_CLAIM) {
      console.log('\n  the claim statement is gone; every leg below would be measuring nothing')
      console.log('\n' + pass + ' passed, ' + fail + ' failed')
      await client.query('ROLLBACK'); client.release(); await pool.end()
      process.exit(1)
    }
    ok(/AND status = 'dispatching'/.test(UPD_CLAIM),
      'PRE2. the claim is guarded on status=dispatching')
    ok(/leased_by IS NOT DISTINCT FROM/.test(UPD_CLAIM),
      'PRE3. the claim is guarded on still owning the lease')
    ok(CLAIM_INTERPOLATES,
      'PRE4. the claim still interpolates task-signals CLEAR_SQL_FRAGMENT (deleting it must fail here)')
    const missing = LIFECYCLE.filter(c => !new RegExp(c + '\\s*=\\s*NULL').test(UPD_CLAIM))
    ok(missing.length === 0,
      'PRE5. the expanded claim blanks ALL ' + LIFECYCLE.length + ' lifecycle columns' +
      (missing.length ? ' (missing: ' + missing.join(',') + ')' : ''))

    // ── probe row, pinned far future, addressed by id from here on ──────────
    const ins = await client.query(
      `INSERT INTO os_scheduled_tasks (name, type, prompt, status, next_run_at, run_count)
       VALUES ($1, 'delayed', 'task-signal-gate probe', 'active', $2, 0) RETURNING id`,
      [LANE + '-probe', FAR_FUTURE]
    )
    taskId = ins.rows[0].id
    const row = async () => (await client.query('SELECT * FROM os_scheduled_tasks WHERE id = $1', [taskId])).rows[0]
    const setOwner = async (tab) => client.query('UPDATE os_scheduled_tasks SET dispatched_tab_id = $2 WHERE id = $1', [taskId, tab])

    // ── A. POSITIVE. Runs FIRST, on purpose. ────────────────────────────────
    console.log('\nA. POSITIVE: the owning tab can write every signal')
    await setOwner(OWNER_TAB)
    const a1 = await taskSignals.recordBound({ task_id: taskId, tab_id: OWNER_TAB })
    ok(!!(a1 && a1.ok), 'A1. bound from the owning tab returns ok')
    let r = await row()
    ok(!!r.bound_at, 'A2. bound_at is stamped on the row')
    ok(r.bound_tab_id === OWNER_TAB, 'A3. bound_tab_id records which tab bound')

    const a4 = await taskSignals.recordProgress({ task_id: taskId, tab_id: OWNER_TAB, summary: 'halfway' })
    ok(!!(a4 && a4.ok), 'A4. progress from the owning tab returns ok')
    r = await row()
    ok(r.progress_summary === 'halfway' && !!r.progress_at, 'A5. progress lands on the row')

    const a6 = await taskSignals.recordDone({
      task_id: taskId, tab_id: OWNER_TAB, status: 'success',
      result_summary: 'the probe returned 200', result_pointer: 'file:///proof',
    })
    ok(!!(a6 && a6.ok), 'A6. done from the owning tab returns ok')
    r = await row()
    ok(!!r.done_at, 'A7. done_at is stamped')
    ok(r.done_status === 'success', 'A8. done_status persists (dropping it made 48 of 48 rows look failed in June)')
    ok(r.done_summary === 'the probe returned 200', 'A9. done_summary persists')
    ok(r.done_pointer === 'file:///proof', 'A10. done_pointer persists')

    // ── B. THE GUARD, and every leg asserts the ABSENCE of the write ─────────
    console.log('\nB. NEGATIVE: a tab that does not own the row cannot write, and nothing lands')
    await client.query(
      `UPDATE os_scheduled_tasks SET bound_at = NULL, bound_tab_id = NULL, done_at = NULL,
        done_status = NULL, done_summary = NULL, done_pointer = NULL,
        progress_at = NULL, progress_summary = NULL WHERE id = $1`, [taskId])

    const b1 = await taskSignals.recordBound({ task_id: taskId, tab_id: OTHER_TAB })
    ok(!(b1 && b1.ok), 'B1. bound from a non-owning tab is REFUSED')
    ok(!!(b1 && b1.error), 'B2. the refusal carries a named error')
    r = await row()
    ok(r.bound_at === null, 'B3. ABSENCE: bound_at is still NULL (a refusal that writes anyway fails HERE, not above)')
    ok(r.bound_tab_id === null, 'B4. ABSENCE: bound_tab_id is still NULL')

    const b5 = await taskSignals.recordDone({ task_id: taskId, tab_id: OTHER_TAB, status: 'success', result_summary: 'not mine' })
    ok(!(b5 && b5.ok), 'B5. done from a non-owning tab is REFUSED')
    r = await row()
    ok(r.done_at === null && r.done_status === null && r.done_summary === null,
      'B6. ABSENCE: no part of the done landed')

    const b7 = await taskSignals.recordProgress({ task_id: taskId, tab_id: OTHER_TAB, summary: 'not mine either' })
    ok(!(b7 && b7.ok), 'B7. progress from a non-owning tab is REFUSED')
    r = await row()
    ok(r.progress_at === null && r.progress_summary === null, 'B8. ABSENCE: no progress landed')

    const b9 = await taskSignals.recordBound({ task_id: taskId, tab_id: null })
    ok(!(b9 && b9.ok), 'B9. a caller with no tab identity is REFUSED (cannot prove ownership)')
    const b10 = await taskSignals.recordBound({ task_id: 'not-a-uuid', tab_id: OWNER_TAB })
    ok(!(b10 && b10.ok) && /uuid/i.test(String(b10 && b10.error)),
      'B10. a non-uuid task_id is refused by name, not by a Postgres 22P02 out of the MCP surface')

    // POSITIVE CONTROL for section B. Without it a sink that refused everything
    // passes B1 through B10 perfectly.
    const b11 = await taskSignals.recordBound({ task_id: taskId, tab_id: OWNER_TAB })
    ok(!!(b11 && b11.ok), 'B11. POSITIVE CONTROL: the owning tab still succeeds after all those refusals')

    // ── C. THE CLEAR. A stale signal must not survive into the next dispatch ──
    console.log('\nC. the dispatch claim blanks the slate and stamps this dispatch')
    await client.query(
      `UPDATE os_scheduled_tasks SET status='dispatching', leased_by='lease-c', run_count=0,
        bound_at=NOW(), bound_tab_id='tab_previous_fire', done_at=NOW(), done_status='success',
        done_summary='a PRIOR fire finished', done_pointer='file:///old',
        progress_at=NOW(), progress_summary='old progress', dispatched_tab_id='tab_previous_fire'
       WHERE id=$1`, [taskId])
    const before = await row()
    ok(!!before.done_at && !!before.bound_at, 'C1. PRECONDITION: the row carries a full set of prior-fire signals')

    const cRes = await client.query(UPD_CLAIM, ['tab_this_fire', taskId, 'lease-c'])
    ok(cRes.rowCount === 1, 'C2. the claim matches the row it owns the lease on')
    r = await row()
    ok(r.dispatched_tab_id === 'tab_this_fire', 'C3. dispatched_tab_id now names THIS dispatch')
    const notCleared = LIFECYCLE.filter(c => r[c] !== null)
    ok(notCleared.length === 0, 'C4. every lifecycle column is NULL' + (notCleared.length ? ' (left set: ' + notCleared.join(',') + ')' : ''))

    // C5 is the leg that separates a working guard from one that fires on
    // everything: the claim must NOT match when the lease has been reclaimed.
    const cBad = await client.query(UPD_CLAIM, ['tab_thief', taskId, 'a-different-lease'])
    ok(cBad.rowCount === 0, 'C5. NEGATIVE: the claim does not match when the lease was reclaimed')
    r = await row()
    ok(r.dispatched_tab_id === 'tab_this_fire', 'C6. ABSENCE: the thief did not overwrite the tab id')

    // C7: the prior fire's worker is now locked out, which is the point of C.
    const c7 = await taskSignals.recordDone({ task_id: taskId, tab_id: 'tab_previous_fire', status: 'success', result_summary: 'late' })
    ok(!(c7 && c7.ok), 'C7. the PREVIOUS fire worker can no longer complete this row')
    r = await row()
    ok(r.done_at === null, 'C8. ABSENCE: the late done did not land')

    // ── D. THE LAUNCH LOCK, BOTH DIRECTIONS, through the real dispatchOne ────
    console.log('\nD. dispatchOne: the lock releases on a real bind, and does NOT on a foreign one')
    const origPick = creds.pick_healthiest_account
    const origRotate = creds.rotate_to
    const origCurrent = creds.current_account
    creds.pick_healthiest_account = async () => 'code'
    creds.rotate_to = async () => ({ deferred: true })
    creds.current_account = () => 'code'
    scheduler._setWorktreeFns({ allocate: async () => null, prune: async () => {} })

    async function runDispatch(tabId, binder) {
      await client.query(
        `UPDATE os_scheduled_tasks SET status='dispatching', leased_by='lease-d', run_count=0,
          dispatched_tab_id=NULL, bound_at=NULL, bound_tab_id=NULL, done_at=NULL, done_status=NULL,
          done_summary=NULL, done_pointer=NULL, progress_at=NULL, progress_summary=NULL,
          leased_at=NOW() WHERE id=$1`, [taskId])
      const fresh = await row()
      scheduler._setDispatcher({
        dispatch_worker: async () => {
          // The worker "comes up" here. binder runs on a timer so it lands DURING
          // the wait, which is the only way to observe a release rather than a
          // column that was already set before the loop started.
          if (binder) setTimeout(() => { binder().catch(() => {}) }, 300)
          return { ok: true, tab_id: tabId }
        },
        kill_worker: async () => ({ closed: false }),
      })
      // MEASURE THE WAIT, NOT THE CALL. dispatchOne completes a fast worker
      // INLINE, so on D9 markComplete runs inside the same call and its cost
      // lands in any end-to-end timing. Measured 2026-08-29: that inflated D9 to
      // 16645ms against a 6000ms budget on a contended pooler and reported a
      // working handshake as broken - the invented-failure shape this lane
      // already has doctrine for. Stamping the moment markComplete is entered
      // separates the two: waitMs is the lock, the remainder is the completion.
      const origMarkComplete = scheduler.markComplete
      let tMark = null
      scheduler.markComplete = async function (...a) { if (tMark === null) tMark = Date.now(); return origMarkComplete.apply(this, a) }
      const t0 = Date.now()
      try {
        await scheduler.dispatchOne(Object.assign({}, fresh, { leased_by: 'lease-d' }))
      } finally {
        scheduler.markComplete = origMarkComplete
      }
      const total = Date.now() - t0
      return { total, waitMs: tMark === null ? total : tMark - t0 }
    }

    const BUDGET = parseInt(process.env.SCHEDULER_SIGNAL_BOUND_TIMEOUT_MS, 10)

    // ── THE RELEASE LEGS ARE MEASURED AGAINST THIS RUN'S OWN HOLD, NOT THE CLOCK ─
    //
    // 2026-08-29, eighth worker on lane R1. D1 and D9 used to assert
    // `elapsed < BUDGET` against a fixed number, and wall clock cannot tell a
    // released lock from a stalled query. The budget was raised 3000 -> 6000 one
    // arc earlier to buy headroom, which helped and did not fix the shape: a
    // stall longer than the budget still reads as a held lock, and the previous
    // arc could not close it because both of its runs were clean and there was no
    // failing signal to build against.
    //
    // The fix is to stop measuring an absolute at all. A pooler stall is ADDITIVE
    // and it lands on every case in the run alike: with a stall of S, the release
    // costs R + S and the hold costs BUDGET + S. The RATIO of the two degrades
    // toward 1 as S grows, so a ratio test is still fragile. The DIFFERENCE is
    // stall-invariant by construction:
    //
    //     hold - release = (BUDGET + S) - (R + S) = BUDGET - R
    //
    // and that identity holds for any S. So D5's no-bind wait is measured FIRST
    // as this run's own full-budget baseline, on the same pooler, seconds apart,
    // and D1 and D9 then assert that they finished a HALF BUDGET sooner than it.
    // A stall that inflates everything by nine seconds moves neither side of that
    // comparison. A release path that has genuinely stopped releasing collapses
    // the difference to zero and goes red, which is the leg's whole purpose.
    //
    // All four cases are measured before any leg is asserted, purely so the
    // baseline exists when D1 is judged; the printed order is unchanged and the
    // leg ids still mean what the breakage tables in the handovers say they mean.
    const HOLD_MARGIN = Math.round(BUDGET * 0.5)
    const dCase = {}
    async function measureD(key, tabId, binder) {
      const res = await runDispatch(tabId, binder)
      dCase[key] = { ms: res.waitMs, row: await row() }
    }
    // D5 first: it is the baseline the release legs are judged against.
    await measureD('d5', 'tab_d5', null)
    await measureD('d1', 'tab_d1', () => coord.signal_bound({ task_id: taskId }, { tab_id: 'tab_d1' }))
    await measureD('d7', 'tab_d7', () => coord.signal_bound({ task_id: taskId }, { tab_id: 'tab_an_impostor' }))
    await measureD('d9', 'tab_d9', () => coord.signal_done(
      { task_id: taskId, status: 'success', result_summary: 'finished before binding' },
      { tab_id: 'tab_d9' }))
    const d1ms = dCase.d1.ms, d5ms = dCase.d5.ms, d7ms = dCase.d7.ms, d9ms = dCase.d9.ms
    const showD = (ms) => ms + 'ms vs hold ' + d5ms + 'ms, delta ' + (d5ms - ms) + 'ms, need ' + HOLD_MARGIN + 'ms'

    // D1: a legitimate bind. The lock must RELEASE a half budget sooner than the
    // no-bind hold measured moments ago on this same pooler.
    r = dCase.d1.row
    ok(d5ms - d1ms >= HOLD_MARGIN, 'D1. RELEASES: a bind from the dispatched tab ends the wait early (' + showD(d1ms) + ')')
    ok(!!r.bound_at, 'D2. the bind actually landed on the row')
    ok(r.status === 'running', 'D3. the row reached status=running')
    ok(r.dispatched_tab_id === 'tab_d1', 'D4. the row names the tab that was spawned')

    // D5: NO bind at all. The lock must NOT release: the wait runs its budget.
    // This is the direction a one-sided gate cannot see. Without it, a lock that
    // never engages at all scores identically to a working handshake. This one
    // stays absolute on purpose: a stall can only ever INFLATE a hold, never cut
    // it short, so `>= BUDGET` has no false-red mode to fix.
    r = dCase.d5.row
    ok(d5ms >= BUDGET, 'D5. DOES NOT RELEASE: with no bind the wait runs its full budget (' + d5ms + 'ms of ' + BUDGET + 'ms)')
    ok(r.bound_at === null, 'D6. ABSENCE: bound_at stayed NULL through the whole wait')

    // D7: a bind from the WRONG tab, arriving during the wait. This separates
    // "the handshake works" from "the guard works". An unguarded sink releases
    // the lock here, and every other leg in this gate would still be green.
    r = dCase.d7.row
    ok(d7ms >= BUDGET, 'D7. DOES NOT RELEASE on a bind from a tab that does not own the row (' + d7ms + 'ms of ' + BUDGET + 'ms)')
    ok(r.bound_at === null, 'D8. ABSENCE: the impostor bind did not stamp the row')

    // D9: the fast worker. A done that arrives without a bound must still end the
    // wait AND complete the row inline. Losing this case rotted rows at running
    // until the 6h orphan sweep through all of July.
    r = dCase.d9.row
    ok(d5ms - d9ms >= HOLD_MARGIN, 'D9. a done without a bound ends the wait early (' + showD(d9ms) + ')')
    ok(r.status === 'completed', 'D10. and the row is completed inline, not left running for the orphan sweep')

    creds.pick_healthiest_account = origPick
    creds.rotate_to = origRotate
    creds.current_account = origCurrent
    scheduler._resetWorktreeFns()

    // ── E. completionPass detects on the column, and only on the column ──────
    console.log('\nE. completionPass completes a row carrying done_at, and leaves one without')
    await client.query(
      `UPDATE os_scheduled_tasks SET status='running', leased_at=NOW(), leased_by='lease-e',
        dispatched_tab_id='tab_e', done_at=NULL, done_status=NULL, done_summary=NULL WHERE id=$1`, [taskId])
    scheduler._setDispatcher({ kill_worker: async () => ({ closed: false }) })
    scheduler._setWorktreeFns({ allocate: async () => null, prune: async () => {} })
    await scheduler.completionPass()
    r = await row()
    ok(r.status === 'running', 'E1. NEGATIVE: a running row with no done_at is left alone')

    await taskSignals.recordDone({ task_id: taskId, tab_id: 'tab_e', status: 'success', result_summary: 'e done' })
    await scheduler.completionPass()
    r = await row()
    ok(r.status === 'completed', 'E2. POSITIVE: once done_at is set the same row completes')
    ok(String(r.last_result || '').indexOf('e done') !== -1, 'E3. the summary reaches last_result')
    scheduler._resetWorktreeFns()

    // ── G. the signal is CONSUMED at the terminal transition ────────────────
    //
    // completionPass selects `status='running' AND done_at IS NOT NULL` and its
    // comment asserts nothing else can have written it. That is true of the WRITE
    // path and was false of the ROW: the only thing that ever cleared these
    // columns was dispatchOne's step 3b, and the ADOPT path (step 2d, the
    // 2026-07-17 double-spawn guard) flips a row to running and RETURNS before
    // reaching it. A consumed done therefore survived to complete a second time.
    //
    // BOTH DIRECTIONS ARE GATED HERE, because a fix that simply blanked the
    // columns at adopt would pass the phantom legs and silently LOSE a legitimate
    // completion (staleLeaseRecovery branch 1 reclaims a lease without touching
    // dispatched_tab_id, so a reclaimed worker's done still lands while the row
    // sits active). G7 is that control: it must still complete.
    console.log('\nG. a completion is consumed at the terminal transition, and only then')
    const gIns = await client.query(
      `INSERT INTO os_scheduled_tasks (name, type, prompt, status, next_run_at, run_count, cron_expression, tz)
       VALUES ($1, 'cron', 'task-signal-gate adopt probe', 'active', $2, 0, '0 3 * * *', 'Australia/Brisbane')
       RETURNING id`,
      [LANE + '-adopt-probe', FAR_FUTURE]
    )
    const gId = gIns.rows[0].id
    const gRow = async () => (await client.query('SELECT * FROM os_scheduled_tasks WHERE id = $1', [gId])).rows[0]
    const gKilled = []
    scheduler._setDispatcher({ kill_worker: async (a) => { gKilled.push(a && a.tab_id); return { closed: false } } })
    scheduler._setWorktreeFns({ allocate: async () => null, prune: async () => {} })

    // first fire: a normal, legitimate completion
    await client.query(
      `UPDATE os_scheduled_tasks SET status='running', leased_at=NOW(),
         leased_by='lease-g', dispatched_tab_id='tab_g1' WHERE id=$1`, [gId])
    await taskSignals.recordDone({ task_id: gId, tab_id: 'tab_g1', status: 'success', result_summary: 'g first fire' })
    await scheduler.completionPass()
    let g = await gRow()
    ok(g.status === 'active' && g.run_count === 1,
      'G1. POSITIVE: the first fire completes normally (status=' + g.status + ' run_count=' + g.run_count + ')')
    ok(g.done_at === null,
      'G2. ABSENCE: the terminal transition consumed done_at')
    ok(g.done_status === null && g.done_summary === null && g.done_pointer === null,
      'G3. ABSENCE: it consumed done_status/summary/pointer with it')

    // the ADOPT path, driven through dispatchOne's OWN shipped statement rather
    // than a hand-copy. Its guard is (status='dispatching' AND leased_by match),
    // so put the row where the real 2d branch finds it first.
    ok(!!UPD_ADOPT, 'G3b. the adopt statement was found in dispatchOne\'s shipped source')
    await client.query(
      `UPDATE os_scheduled_tasks SET status='dispatching', leased_by='lease-g-adopt' WHERE id=$1`, [gId])
    await client.query(UPD_ADOPT, ['tab_g2_live', gId, 'lease-g-adopt'])
    await client.query(
      `UPDATE os_scheduled_tasks SET leased_at=NOW() WHERE id=$1 AND leased_at IS NULL`, [gId])
    const gBefore = (await gRow()).run_count
    gKilled.length = 0
    await scheduler.completionPass()
    g = await gRow()
    ok(g.run_count === gBefore,
      'G4. NEGATIVE: an adopted row does not re-complete off the prior fire (run_count ' + gBefore + ' -> ' + g.run_count + ')')
    ok(gKilled.indexOf('tab_g2_live') === -1,
      'G5. ABSENCE: the live worker the row just adopted was not killed (killed=' + JSON.stringify(gKilled) + ')')
    ok(g.status === 'running',
      'G6. the adopted row is still running, not falsely advanced (status=' + g.status + ')')

    // THE CONTROL. An overcorrected fix that cleared at adopt would pass G4-G6
    // and lose this. The adopted worker's OWN done must still complete the row.
    await taskSignals.recordDone({ task_id: gId, tab_id: 'tab_g2_live', status: 'success', result_summary: 'g adopted fire' })
    await scheduler.completionPass()
    g = await gRow()
    ok(g.run_count === gBefore + 1,
      'G7. CONTROL: the adopted worker\'s OWN done still completes the row (run_count ' + gBefore + ' -> ' + g.run_count + ')')
    ok(String(g.last_result || '').indexOf('g adopted fire') !== -1,
      'G8. CONTROL: and it is THAT done that landed, not the consumed one')

    // THE REAL LOSING CASE for a clear-at-adopt fix, which G7/G8 do NOT model:
    // they write the adopted worker's done AFTER the adopt, so a clear at adopt
    // cannot touch it. The window that a clear-at-adopt actually destroys is
    // BEFORE it. staleLeaseRecovery branch 1 reclaims a lease and nulls
    // leased_by/leased_at while leaving dispatched_tab_id intact, so the worker
    // it reclaimed can still write a legitimate done onto a row sitting at
    // status='active'. That done is unconsumed. If the row is then adopted and
    // the adopt blanks the columns, the completion is gone and the row rots to
    // the 6h orphan sweep. It must survive the adopt and complete.
    await client.query(
      `UPDATE os_scheduled_tasks SET status='active', leased_by=NULL, leased_at=NULL,
         dispatched_tab_id='tab_g4_reclaimed' WHERE id=$1`, [gId])
    const gPre = (await gRow()).run_count
    const dR = await taskSignals.recordDone({ task_id: gId, tab_id: 'tab_g4_reclaimed',
      status: 'success', result_summary: 'g done after lease reclaim' })
    ok(dR && dR.ok === true,
      'G10. a worker whose lease was reclaimed can still write its done (dispatched_tab_id survives branch 1)')
    await client.query(
      `UPDATE os_scheduled_tasks SET status='dispatching', leased_by='lease-g-adopt2' WHERE id=$1`, [gId])
    await client.query(UPD_ADOPT, ['tab_g4_reclaimed', gId, 'lease-g-adopt2'])
    await client.query(
      `UPDATE os_scheduled_tasks SET leased_at=NOW() WHERE id=$1 AND leased_at IS NULL`, [gId])
    ok((await gRow()).done_at !== null,
      'G11. CONTROL: the adopt did NOT erase that unconsumed done (a clear-at-adopt fix fails HERE)')
    await scheduler.completionPass()
    g = await gRow()
    ok(g.run_count === gPre + 1,
      'G12. CONTROL: and it still completes the row (run_count ' + gPre + ' -> ' + g.run_count + ')')

    // markFailed is the other terminal transition and needs the same consume, or
    // a failed row carries its signal into the next adopt exactly the same way.
    await client.query(
      `UPDATE os_scheduled_tasks SET status='running', leased_at=NOW(),
         leased_by='lease-g', dispatched_tab_id='tab_g3', retry_count=0 WHERE id=$1`, [gId])
    await taskSignals.recordDone({ task_id: gId, tab_id: 'tab_g3', status: 'failed', result_summary: 'g failed fire' })
    await scheduler.completionPass()
    g = await gRow()
    ok(g.done_at === null && g.done_status === null,
      'G9. ABSENCE: markFailed consumes the signal too (done_at=' + g.done_at + ' done_status=' + g.done_status + ')')
    scheduler._resetWorktreeFns()

    // ── H. a row SUSPENDED while running does not loop forever ──────────────
    //
    // 2026-08-29, seventh worker on this lane. The G legs above all run on rows
    // with a clean last_status, so 63 of 63 was green over this.
    //
    // markComplete's cron arm and markFailed's cron-defer / retryable arms are
    // guarded on `archived_at IS NULL AND last_status NOT IN ('paused','cancelled')`
    // and nothing inspected rowCount, so on a suspended row the statement is a
    // no-op and the CONSUME riding inside it is skipped with the status advance.
    // completionPass carries no such guard, so it re-selected the row every 5s
    // forever: kill_worker on the tab and a chain wake, every pass, run_count
    // frozen.
    //
    // The state is REACHABLE THROUGH THE SHIPPED TOOL, which is why this is a
    // gate leg and not a curiosity: schedule_pause sets last_status and
    // deliberately leaves status alone, so pausing a task whose worker is
    // mid-flight produces it. These legs drive the real exported schedule_pause,
    // never a hand-typed UPDATE, so a change to its semantics fails here.
    console.log('\nH. a row paused WHILE RUNNING consumes its signal instead of looping')
    const hIns = await client.query(
      `INSERT INTO os_scheduled_tasks (name, type, prompt, status, next_run_at, run_count, cron_expression, tz)
       VALUES ($1, 'cron', 'task-signal-gate suspended probe', 'active', $2, 0, '0 3 * * *', 'Australia/Brisbane')
       RETURNING id`,
      [LANE + '-suspended-probe', FAR_FUTURE]
    )
    const hId = hIns.rows[0].id
    const hChildIns = await client.query(
      `INSERT INTO os_scheduled_tasks (name, type, prompt, status, next_run_at, run_count, cron_expression, tz)
       VALUES ($1, 'cron', 'task-signal-gate suspended child', 'active', $2, 0, '0 4 * * *', 'Australia/Brisbane')
       RETURNING id`,
      [LANE + '-suspended-child', FAR_FUTURE]
    )
    const hChildId = hChildIns.rows[0].id
    await client.query('UPDATE os_scheduled_tasks SET chain_after = $1 WHERE id = $2', [hId, hChildId])
    const hRow = async (id) => (await client.query('SELECT * FROM os_scheduled_tasks WHERE id = $1', [id])).rows[0]
    const hKilled = []
    scheduler._setDispatcher({ kill_worker: async (a) => { hKilled.push(a && a.tab_id); return { closed: false } } })
    scheduler._setWorktreeFns({ allocate: async () => null, prune: async () => {} })

    await client.query(
      `UPDATE os_scheduled_tasks SET status='running', leased_at=NOW(),
         leased_by='lease-h', dispatched_tab_id='tab_h_live' WHERE id=$1`, [hId])
    const hPause = await scheduler.schedule_pause({ id: hId })
    ok(!!(hPause && hPause.ok), 'H1. REACHABILITY: the shipped schedule_pause succeeds on a row that is running')
    let h = await hRow(hId)
    ok(h.status === 'running' && h.last_status === 'paused',
      'H2. REACHABILITY: it leaves status=running and sets last_status=paused (status=' + h.status + ' last_status=' + h.last_status + ')')
    const hDone = await taskSignals.recordDone({ task_id: hId, tab_id: 'tab_h_live', status: 'success', result_summary: 'h suspended fire' })
    ok(!!(hDone && hDone.ok), 'H3. REACHABILITY: the worker\'s done still lands on the suspended row')

    const hBefore = await hRow(hId)
    hKilled.length = 0
    // Pass 1 is the legitimate one: the row IS completing, so one kill and one
    // chain wake are correct here and are NOT what this section is measuring.
    await scheduler.completionPass()
    const hKillsAfter1 = hKilled.filter(t => t === 'tab_h_live').length
    // Re-park the child so passes 2 and 3 have to move it again to be seen. An
    // OR'd assertion here would be a leg that cannot fail; this one can.
    await client.query('UPDATE os_scheduled_tasks SET next_run_at = $2 WHERE id = $1', [hChildId, FAR_FUTURE])
    await scheduler.completionPass()
    await scheduler.completionPass()
    h = await hRow(hId)
    ok(h.done_at === null && h.done_status === null,
      'H4. THE FIX: the signal is consumed even though the guarded status advance did not apply (done_at=' + h.done_at + ')')
    ok(hKilled.filter(t => t === 'tab_h_live').length === hKillsAfter1,
      'H5. ABSENCE: passes 2 and 3 killed the tab again ZERO times (total ' + hKilled.filter(t => t === 'tab_h_live').length + ', pre-fix this was 3)')
    const hChild = await hRow(hChildId)
    ok(new Date(hChild.next_run_at).getTime() === new Date(FAR_FUTURE).getTime(),
      'H6. ABSENCE: the chain child was not re-woken by passes 2 and 3 (next_run_at=' + hChild.next_run_at + ')')

    // THE CONTROL FOR THE OTHER DIRECTION. The consume must not have become a
    // resurrection: a suspended row must NOT be advanced to active, and its
    // suspension must survive. A fix that simply dropped the guard passes H4-H6
    // and fails here.
    ok(h.status !== 'active',
      'H7. CONTROL: the suspended row was NOT resurrected to active (status=' + h.status + ')')
    ok(h.last_status === 'paused',
      'H8. CONTROL: and its suspension survived the consume (last_status=' + h.last_status + ')')
    ok(h.run_count === hBefore.run_count,
      'H9. CONTROL: the guarded bookkeeping still did not apply (run_count ' + hBefore.run_count + ' -> ' + h.run_count + ')')

    // POSITIVE CONTROL: without this, a completionPass that had crashed or a
    // markComplete that no-opped for every row would score H4-H6 green.
    const hcIns = await client.query(
      `INSERT INTO os_scheduled_tasks (name, type, prompt, status, next_run_at, run_count, cron_expression, tz)
       VALUES ($1, 'cron', 'task-signal-gate suspended control', 'active', $2, 0, '0 3 * * *', 'Australia/Brisbane')
       RETURNING id`,
      [LANE + '-suspended-control', FAR_FUTURE]
    )
    const hcId = hcIns.rows[0].id
    await client.query(
      `UPDATE os_scheduled_tasks SET status='running', leased_at=NOW(),
         leased_by='lease-hc', dispatched_tab_id='tab_hc' WHERE id=$1`, [hcId])
    await taskSignals.recordDone({ task_id: hcId, tab_id: 'tab_hc', status: 'success', result_summary: 'h control fire' })
    await scheduler.completionPass()
    const hc = await hRow(hcId)
    ok(hc.status === 'active' && hc.run_count === 1 && hc.done_at === null,
      'H10. POSITIVE CONTROL: an unsuspended row in the same run still completes and consumes (status=' + hc.status + ' run_count=' + hc.run_count + ')')

    // markFailed's guarded arms need the same consume, or a FAILED run on a
    // suspended row loops the same way through the explicitFailure route.
    await client.query(
      `UPDATE os_scheduled_tasks SET status='running', leased_at=NOW(), leased_by='lease-h',
         dispatched_tab_id='tab_h_fail', retry_count=0 WHERE id=$1`, [hId])
    await taskSignals.recordDone({ task_id: hId, tab_id: 'tab_h_fail', status: 'failed', result_summary: 'h suspended failure' })
    await scheduler.completionPass()
    h = await hRow(hId)
    ok(h.done_at === null && h.done_status === null,
      'H11. markFailed consumes on a suspended row too (done_at=' + h.done_at + ' done_status=' + h.done_status + ')')
    ok(h.last_status === 'paused',
      'H12. CONTROL: and that path did not resurrect it either (last_status=' + h.last_status + ')')

    // markFailed has TWO guarded arms and H11 above reaches only ONE of them.
    // H11 presets retry_count=0, so newRetryCount=1 < MAX_RETRY_COUNT and control
    // falls to the RETRYABLE arm. The other arm - the cron-defer taken when
    // newRetryCount >= MAX_RETRY_COUNT on a cron row - carries its OWN guarded
    // UPDATE and its OWN consume, and nothing above ever executed it.
    //
    // MEASURED 2026-08-29, eighth worker on lane R1: deleting the consume from
    // that one arm left this gate at 75 of 75, exit 0, on an isolated archive
    // copy of the fixed tree. A hole a whole arm wide, in the same routine the
    // section was written for. The legs below are what turn that green red.
    //
    // Reachable through the same shipped tool as the rest of section H: a cron
    // row that has already burned its retries, paused mid-flight via
    // schedule_pause, whose worker then signals failed. Its guarded UPDATE
    // matches nothing, so without the consume, completionPass re-selects it
    // every 5s forever - kill_worker on the tab on every pass, run_count frozen.
    //
    // retry_count is set far above MAX_RETRY_COUNT rather than to its exact
    // value (the constant is not exported), and H13 is the POSITIVE CONTROL that
    // the arm was actually taken: without it, a raised MAX_RETRY_COUNT would
    // route these rows back to the retryable arm and H14 would pass vacuously as
    // a second copy of H11.
    const H_MAXED_RETRY = 99
    const hdIns = await client.query(
      `INSERT INTO os_scheduled_tasks (name, type, prompt, status, next_run_at, run_count, retry_count, cron_expression, tz)
       VALUES ($1, 'cron', 'task-signal-gate cron-defer arm control', 'active', $2, 0, $3, '0 3 * * *', 'Australia/Brisbane')
       RETURNING id`,
      [LANE + '-crondefer-control', FAR_FUTURE, H_MAXED_RETRY]
    )
    const hdId = hdIns.rows[0].id
    await client.query(
      `UPDATE os_scheduled_tasks SET status='running', leased_at=NOW(),
         leased_by='lease-hd', dispatched_tab_id='tab_hd' WHERE id=$1`, [hdId])
    await taskSignals.recordDone({ task_id: hdId, tab_id: 'tab_hd', status: 'failed', result_summary: 'hd max-retry failure' })
    await scheduler.completionPass()
    const hd = await hRow(hdId)
    ok(hd.status === 'active' && hd.retry_count === 0 && /cron: deferred to next interval/.test(String(hd.last_error || '')),
      'H13. POSITIVE CONTROL: retry_count=' + H_MAXED_RETRY + ' on a cron row really does take the cron-defer arm (status=' +
        hd.status + ' retry_count=' + hd.retry_count + ')')

    hKilled.length = 0
    await client.query(
      `UPDATE os_scheduled_tasks SET status='running', leased_at=NOW(), leased_by='lease-h',
         dispatched_tab_id='tab_h_maxfail', retry_count=$2 WHERE id=$1`, [hId, H_MAXED_RETRY])
    await taskSignals.recordDone({ task_id: hId, tab_id: 'tab_h_maxfail', status: 'failed', result_summary: 'h suspended max-retry failure' })
    await scheduler.completionPass()
    const hMaxKillsAfter1 = hKilled.filter(t => t === 'tab_h_maxfail').length
    // Re-park the child so passes 2 and 3 have to move it again to be seen, for
    // the same reason H6 does: an assertion that cannot go red proves nothing.
    await client.query('UPDATE os_scheduled_tasks SET next_run_at = $2 WHERE id = $1', [hChildId, FAR_FUTURE])
    await scheduler.completionPass()
    await scheduler.completionPass()
    h = await hRow(hId)
    ok(h.done_at === null && h.done_status === null,
      'H14. THE FIX, cron-defer arm: a retry-exhausted suspended cron row consumes too (done_at=' + h.done_at + ' done_status=' + h.done_status + ')')
    ok(hKilled.filter(t => t === 'tab_h_maxfail').length === hMaxKillsAfter1,
      'H15. ABSENCE: passes 2 and 3 did not re-kill that tab (total ' +
        hKilled.filter(t => t === 'tab_h_maxfail').length + ', unfixed this climbs every pass)')
    // No chain-child leg here, deliberately. markComplete returns into markFailed
    // BEFORE its chain wake, which is guarded on !explicitFailure, so the failure
    // path never wakes children and such a leg could not go red. Measured: it
    // stayed green against the very breakage H14/H15 catch. A leg that cannot
    // fail is the H6 mistake one arc earlier, and it is not repeated here.
    ok(h.last_status === 'paused' && h.status !== 'active',
      'H16. CONTROL: and the cron-defer consume did not resurrect it (status=' + h.status + ' last_status=' + h.last_status + ')')
    scheduler._resetWorktreeFns()

    // ── F. the retired bus types are genuinely gone, not renamed ─────────────
    console.log('\nF. the retired message types are not produced anywhere')
    ok(typeof coord.scanTopicByType !== 'function',
      'F1. coord.scanTopicByType is no longer exported (the scheduler is not a bus reader)')
    ok(!/coord\.(peek_inbox|read_inbox|ack_message|scanTopicByType)/.test(SRC),
      'F2. dispatchOne contains no coord inbox call at all')
    ok(!/scanTopicByType/.test(scheduler.completionPass.toString()),
      'F3. completionPass contains no scanTopicByType call')

    console.log('\n' + pass + ' passed, ' + fail + ' failed')
    await client.query('ROLLBACK')
    client.release()
    await pool.end()
    process.exit(fail > 0 ? 1 : 0)
  } catch (e) {
    // Exit 2, never 1. A crash and a failed leg are both a non-zero exit with
    // something on stderr, and that confusion scored a broken helper green on
    // this same lane on 2026-08-28.
    console.error('\nGATE CRASHED: ' + (e && e.stack || e))
    try { await client.query('ROLLBACK') } catch (_e) {}
    try { client.release() } catch (_e) {}
    try { await pool.end() } catch (_e) {}
    process.exit(2)
  }
}

main()
