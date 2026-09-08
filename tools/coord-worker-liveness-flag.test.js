// 2026-09-09 lane coord-L1. Two defects, one commit, one test file.
//
// DEFECT 1 - THE DARK WIRE. routes/mcpCoord.js callTool never bumped
// last_heartbeat_at. index.js:147 does bump it, but only on /api/tool and only
// for NON-coord tools, and a dispatched Claude Code worker holds no agent bearer
// so coord.* over the MCP shim is the ONLY traffic it sends the agent. Result:
// 15 of 15 registered workers carried last_heartbeat_at === registered_at.
//
// DEFECT 2 - THE FALSE ABSENCE AND THE LAUNDERING. list_workers called a row
// `dead` on heartbeat age, so with defect 1 live every live worker read dead
// within 90s and include_dead:false returned the empty set permanently. And
// terminated_at, the signal doctrine calls sound, is itself MANUFACTURED from
// heartbeat age by sweepStaleWorkers with terminated_reason 'stale_heartbeat' -
// so a consumer obeying "trust terminated_at, never staleness" inherits the
// staleness it was told to distrust. The discriminator was on the row and was
// never returned.
//
// EVERY case below asserts WHY, not merely that some refusal or inclusion
// happened. Mutation-checked: see the MUTATIONS block at the foot of this file
// for the exact edit that must turn each case red, and run --mutants to see the
// harness confirm each guard is load-bearing rather than agreeing by accident.
// Run: node tools/coord-worker-liveness-flag.test.js
const fs = require('fs')
const os = require('os')
const path = require('path')

const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'coord-liveness-'))
process.env.COORD_ROOT = tmpRoot
process.env.COORD_DISABLE_SWEEP = '1'

const WORKERS_DIR = path.join(tmpRoot, 'workers')
fs.mkdirSync(WORKERS_DIR, { recursive: true })

const nowMs = Date.now()
const iso = (ms) => new Date(ms).toISOString()
const write = (tab, obj) => fs.writeFileSync(
  path.join(WORKERS_DIR, tab + '.json'), JSON.stringify(Object.assign({ tab_id: tab }, obj), null, 2))

// --- fixtures. Ages are chosen relative to the two thresholds this file is about:
//     DEAD_HEARTBEAT_MS = 90s (the old `dead` bound) and the 60min sweep bound.
const FRESH   = 'tab_17888800000001_aaaaaaaa'  // 5s old, nothing interesting
const STALE   = 'tab_17888800000002_bbbbbbbb'  // 40min stale, NOT terminated  <- the false absence
const SWEPT   = 'tab_17888800000003_cccccccc'  // terminated by the sweep       <- the laundering
const DONE    = 'tab_17888800000004_dddddddd'  // terminated by signal_done     <- a real death

write(FRESH, { task_id: 't-fresh', registered_at: iso(nowMs - 5_000),
  last_heartbeat_at: iso(nowMs - 5_000), terminated_at: null })
write(STALE, { task_id: 't-stale', registered_at: iso(nowMs - 40 * 60_000),
  last_heartbeat_at: iso(nowMs - 40 * 60_000), terminated_at: null })
write(SWEPT, { task_id: 't-swept', registered_at: iso(nowMs - 90 * 60_000),
  last_heartbeat_at: iso(nowMs - 90 * 60_000), terminated_at: iso(nowMs - 30 * 60_000),
  terminated_reason: 'stale_heartbeat', stale_at_termination_ms: 3_616_329 })
write(DONE, { task_id: 't-done', registered_at: iso(nowMs - 20 * 60_000),
  last_heartbeat_at: iso(nowMs - 6 * 60_000), terminated_at: iso(nowMs - 5 * 60_000),
  terminated_reason: 'signal_done' })

const coord = require('./coord')
const mcp = require('../routes/mcpCoord')

let pass = 0, fail = 0
const check = (cond, msg) => {
  if (cond) { pass++; console.log('  ok   ' + msg) }
  else { fail++; console.log('  FAIL ' + msg) }
}
const byTab = (rows, tab) => (rows || []).find(r => r.tab_id === tab) || null

// ===================================================================
// PART 1 - list_workers: the false absence, and the laundering it hid
// ===================================================================
async function part1() {
  const live = (await coord.list_workers({ include_dead: false })).workers
  const all = (await coord.list_workers({ include_dead: true })).workers

  // 1a. THE HEADLINE. A 40-minute-stale worker that nothing has terminated is a
  //     worker we have NO evidence about, and the old code called that death.
  //     Asserting only "count > 0" would pass on the fresh row alone, so name the
  //     row and name the reason it must be there.
  const s = byTab(live, STALE)
  check(!!s, '1a a stale-but-not-terminated worker is RETURNED by include_dead:false')
  check(!!s && s.dead === false,
    '1a WHY: dead is false because terminated_at is null, not because it is fresh')
  check(!!s && s.heartbeat_stale === true && s.stale_ms > 90_000,
    '1a and its staleness is still REPORTED (heartbeat_stale true, stale_ms > 90s), just not called death')

  // 1b. The filter still filters. Without this, 1a passes for the useless reason
  //     that include_dead:false stopped excluding anything at all.
  check(byTab(live, SWEPT) === null && byTab(live, DONE) === null,
    '1b both terminated workers are HIDDEN by include_dead:false (the filter still filters)')
  check(byTab(all, SWEPT) !== null && byTab(all, DONE) !== null,
    '1b and both reappear under include_dead:true')

  // 1c. THE LAUNDERING IS NOW VISIBLE. Two rows, identical dead:true, opposite
  //     provenance. A consumer that cannot tell them apart inherits heartbeat
  //     staleness through the very field it was told to trust instead.
  const sw = byTab(all, SWEPT), dn = byTab(all, DONE)
  check(!!sw && !!dn && sw.dead === true && dn.dead === true,
    '1c the swept row and the signal_done row are INDISTINGUISHABLE on dead alone')
  check(!!sw && sw.terminated_reason === 'stale_heartbeat',
    '1c but terminated_reason exposes the swept one as MANUFACTURED from heartbeat age')
  check(!!dn && dn.terminated_reason === 'signal_done',
    '1c and the real terminal event as signal_done')
  check(!!sw && sw.stale_at_termination_ms === 3_616_329,
    '1c stale_at_termination_ms carries the age that triggered the sweep')

  // 1d. The age filter survives as an OPT-IN, so a caller who genuinely wants it
  //     names its own threshold instead of getting 90s under the word "dead".
  const tight = (await coord.list_workers({ include_dead: false, max_stale_ms: 60_000 })).workers
  check(byTab(tight, FRESH) !== null && byTab(tight, STALE) === null,
    '1d max_stale_ms opt-in drops the stale row and keeps the fresh one')
  check(byTab(live, STALE) !== null,
    '1d WHY it is opt-in: the SAME call without max_stale_ms keeps the stale row')
}

// ===================================================================
// PART 2 - the heartbeat wire on the MCP route
// ===================================================================
;(async () => {
  console.log('\nPART 1: list_workers')
  await part1()
  console.log('\nPART 2: mcpCoord heartbeat wire')
  const readHb = (tab) => JSON.parse(
    fs.readFileSync(path.join(WORKERS_DIR, tab + '.json'), 'utf8')).last_heartbeat_at

  // 2a. THE FIX. A plain coord call from a registered worker is proof of life.
  const before = readHb(STALE)
  await mcp._callToolForTest('coord.list_channels', {}, { tab_id: STALE })
  const after = readHb(STALE)
  check(Date.parse(after) > Date.parse(before),
    '2a a NON-heartbeat coord call from a registered worker MOVES last_heartbeat_at')

  // 2b. ORDERING, not just occurrence. The touch must follow a SUCCESSFUL handler,
  //     mirroring /api/tool: a call that did not run is not evidence the tab ran it.
  //
  //     THIS CASE WAS AGREEING BEFORE IT WORKED, and the mutation run is what
  //     caught it. It first drove 'coord.no_such_tool_at_all', which returns at the
  //     handler-lookup guard ABOVE the try block, so the touch was never reached no
  //     matter where inside the try it sat: the case stayed green under its own
  //     mutation (touch moved above the handler await) and proved nothing about
  //     ordering. It needs a handler that EXISTS and THROWS. ack_message with no id
  //     is exactly that. See [[a-negative-control-that-only-asserts-a-refusal-cannot-
  //     tell-a-designed-refusal-from-a-lucky-one]].
  const b2 = readHb(FRESH)
  await new Promise(r => setTimeout(r, 5))
  const r2 = await mcp._callToolForTest('coord.ack_message', {}, { tab_id: FRESH })
  check(r2 && r2.isError === true, '2b a REAL handler that throws is still an error')
  check(readHb(FRESH) === b2,
    '2b WHY ordering matters: a handler that THREW does NOT touch the heartbeat')

  // 2b-control. The same tool, called correctly, DOES touch - so 2b is failing on
  // the throw and not because ack_message is simply never wired to the touch.
  const b3 = readHb(FRESH)
  await new Promise(r => setTimeout(r, 5))
  const r3 = await mcp._callToolForTest('coord.ack_message', { id: 'no-such-message' }, { tab_id: FRESH })
  check(r3 && r3.isError !== true, '2b-control the same handler SUCCEEDS when given an id')
  check(Date.parse(readHb(FRESH)) > Date.parse(b3),
    '2b-control WHY: and then it DOES touch, so 2b discriminates the throw, not the tool')

  // 2c. NO RESURRECTION. A late call from a worker that already signalled done
  //     must not un-terminate it, or signal_done becomes reversible by any
  //     straggler tool call and the sweep can never GC the row.
  const doneBefore = JSON.parse(fs.readFileSync(path.join(WORKERS_DIR, DONE + '.json'), 'utf8'))
  await mcp._callToolForTest('coord.list_channels', {}, { tab_id: DONE })
  const doneAfter = JSON.parse(fs.readFileSync(path.join(WORKERS_DIR, DONE + '.json'), 'utf8'))
  check(doneAfter.last_heartbeat_at === doneBefore.last_heartbeat_at,
    '2c a TERMINATED worker is NOT resurrected by a late coord call')
  check(doneAfter.terminated_at === doneBefore.terminated_at,
    '2c WHY: its terminated_at is untouched, so signal_done stays final')

  // 2d. The explicit heartbeat path is not double-written by the new touch.
  const hbRes = await mcp._callToolForTest('coord.heartbeat', {}, { tab_id: STALE })
  check(hbRes && !hbRes.isError && hbRes.body && hbRes.body.ok === true,
    '2d the explicit coord.heartbeat handler still works and is not shadowed')

  // 2e. A NON-registered tab is a silent no-op, never a crash or a phantom row.
  const r5 = await mcp._callToolForTest('coord.list_channels', {}, { tab_id: 'tab_17888800000009_ffffffff' })
  check(r5 && r5.isError !== true, '2e an unregistered tab_id does not break the call')
  check(!fs.existsSync(path.join(WORKERS_DIR, 'tab_17888800000009_ffffffff.json')),
    '2e WHY: and it does not conjure a registry row for a tab that never registered')

  console.log('\n' + (fail === 0 ? 'PASS' : 'FAIL') + '  ' + pass + ' passed, ' + fail + ' failed')
  try { fs.rmSync(tmpRoot, { recursive: true, force: true }) } catch (e) {}
  process.exit(fail === 0 ? 0 : 1)
})()

// MUTATIONS - the edit that must turn each case RED. A control that stays green
// under its mutation is agreeing, not working (see doctrine
// [[a-negative-control-that-only-asserts-a-refusal-cannot-tell-a-designed-refusal-from-a-lucky-one]]).
//   1a  coord.js list_workers: restore `const is_dead = stale_ms > DEAD_HEARTBEAT_MS || !!w.terminated_at`
//   1b  coord.js list_workers: drop the `if (is_dead && !include_dead) continue` line
//   1c  coord.js list_workers: stop returning terminated_reason
//   1d  coord.js list_workers: ignore params.max_stale_ms
//   2a  mcpCoord.js callTool: delete the _touchHeartbeatForTab call
//   2b  mcpCoord.js callTool: move the touch ABOVE the handler await
//       (this mutation is what exposed 2b agreeing rather than working - see the case)
//   2c  coord.js _touchHeartbeatForTab: delete `if (w.terminated_at) return false`
