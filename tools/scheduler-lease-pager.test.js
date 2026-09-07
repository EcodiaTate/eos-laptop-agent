// scheduler-lease-pager.test.js - the gate on the lease-path exception pager.
//
// WHAT IT GUARDS. On 2026-09-06 leaseDueRows threw on every 30s pass for 7h55m,
// the whole fleet dispatched nothing, and the only response in the code was a
// stderr line. noteLeaseError/noteLeaseOk make N consecutive lease exceptions page
// Tate. This suite proves the page fires, and proves the controls are load-bearing.
//
// HOW THE CONTROLS ARE WRITTEN. Per
// [[a-first-draft-control-usually-passes-for-the-wrong-reason-2026-09-06]], each
// control is the POSITIVE case minus exactly ONE gate, so the only thing between it
// and a fire is that gate. Run `node tools/scheduler-lease-pager.test.js --mutate`
// to delete each gate in a copy of the real source and confirm its control FLIPS to
// red. A control that survives its own mutation is agreeing, not working.
//
// No Postgres, no send: _setPagerSender and _setLeaseBoardWriter are injected.

const fs = require('fs')
const os = require('os')
const path = require('path')

const CRUMB = path.join(os.tmpdir(), 'lease-pager-test-' + process.pid + '.json')
process.env.SCHEDULER_LEASE_BREADCRUMB_FILE = CRUMB
process.env.SCHEDULER_LEASE_ERROR_PAGE_THRESHOLD = '10'

let pass = 0, fail = 0
const failures = []
function ok(cond, label) {
  if (cond) { pass++; console.log('  ok   ' + label) }
  else { fail++; failures.push(label); console.log('  FAIL ' + label) }
}

// Fresh harness per case: a stale latch from a prior case is itself a way for a
// control to pass for the wrong reason.
function harness(mod, opts) {
  opts = opts || {}
  const sent = []
  mod._setPagerSender(function (script, args, done) {
    sent.push({ script, args })
    if (done) done(opts.sendErr || null, opts.sendCode === undefined ? 0 : opts.sendCode)
  })
  const boardRows = []
  mod._setLeaseBoardWriter(async function (crumb) { boardRows.push(crumb); return true })
  mod._resetLeaseErrorState()
  try { fs.unlinkSync(CRUMB) } catch (_e) {}
  return { sent, boardRows }
}

function run(mod, tag) {
  const T = mod.LEASE_ERROR_PAGE_THRESHOLD
  ok(T === 10, tag + 'threshold is the env-set 10 (the suite is driving the real constant)')

  // ---- POSITIVES ----------------------------------------------------------
  {
    const h = harness(mod)
    let fired = null
    for (let i = 0; i < T; i++) fired = mod.noteLeaseError(new Error('getaddrinfo ENOTFOUND pooler')) || fired
    ok(h.sent.length === 1, tag + 'P1. T consecutive lease exceptions fire exactly ONE page')
    ok(!!fired && /FLEET STALLED/.test(fired.message), tag + 'P2. the page names the fleet stall')
    ok(!!fired && fired.args.indexOf('critical') !== -1, tag + 'P3. the page is --urgency critical (fyi tier lints the word DISPATCH)')
    ok(/ENOTFOUND/.test(fired.message), tag + 'P4. the page carries the causing exception')
    const crumb = mod._readLeaseBreadcrumb()
    ok(!!crumb && crumb.paged === true && crumb.consecutive === T,
       tag + 'P5. a LOCAL breadcrumb holds the outage (the leg that survives the DB being the cause)')
    ok(h.boardRows.length === 1, tag + 'P6. the board write is attempted as an independent leg')
  }
  {
    // The live 2026-09-06 log carried `leaseDueRows error: ` with an EMPTY message.
    const h = harness(mod)
    for (let i = 0; i < T; i++) mod.noteLeaseError(new Error(''))
    ok(h.sent.length === 1, tag + 'P7. an exception with an EMPTY message still pages (the live shape)')
    ok(/empty message/.test(h.sent[0].args[4] || ''), tag + 'P8. and the empty message is named, not silently blank')
  }
  {
    const h = harness(mod)
    for (let i = 0; i < T; i++) mod.noteLeaseError(new Error('x'))
    const rec = mod.noteLeaseOk()
    ok(!!rec && rec.consecutive === T, tag + 'P9. noteLeaseOk reports the outage it just ended')
    ok(mod._getLeaseErrorState().consecutive === 0, tag + 'P10. and resets the counter')
  }
  {
    // Recovery flush: a held breadcrumb reaches the board once leasing works again.
    const h = harness(mod)
    for (let i = 0; i < T; i++) mod.noteLeaseError(new Error('pooler down'))
    const before = h.boardRows.length
    mod.noteLeaseOk()
    ok(h.boardRows.length === before + 1, tag + 'P11. recovery flushes the held breadcrumb to the board')
    ok(!!h.boardRows[h.boardRows.length - 1].recovered_at, tag + 'P12. the flushed row is stamped recovered')
  }

  // ---- CONTROLS: each is the positive MINUS exactly one gate ---------------
  {
    // C1 removes ONLY the count. T-1 errors: consecutive, real, unlatched, and the
    // only thing holding the page back is the threshold comparison.
    const h = harness(mod)
    for (let i = 0; i < T - 1; i++) mod.noteLeaseError(new Error('getaddrinfo ENOTFOUND pooler'))
    ok(h.sent.length === 0, tag + 'C1. T-1 errors do NOT page (gate under test: the count threshold)')
  }
  {
    // C2 removes ONLY consecutiveness. T total errors, but ONE successful lease in
    // the middle. Count, latch and message are all identical to the positive.
    const h = harness(mod)
    for (let i = 0; i < T - 1; i++) mod.noteLeaseError(new Error('getaddrinfo ENOTFOUND pooler'))
    mod.noteLeaseOk()
    mod.noteLeaseError(new Error('getaddrinfo ENOTFOUND pooler'))
    ok(h.sent.length === 0, tag + 'C2. T errors broken by ONE good lease do NOT page (gate: the reset in noteLeaseOk)')
  }
  {
    // C3 removes ONLY the latch. Already paged, still failing, twice as many errors.
    const h = harness(mod)
    for (let i = 0; i < T * 2; i++) mod.noteLeaseError(new Error('getaddrinfo ENOTFOUND pooler'))
    ok(h.sent.length === 1, tag + 'C3. a continuing outage pages ONCE, not per pass (gate: the one-page latch)')
  }
  {
    // C4 removes ONLY the unlatch-on-failed-send. Identical to the positive except
    // text-tate exits non-zero, so the page never landed and must be retried.
    const h = harness(mod, { sendCode: 1 })
    for (let i = 0; i < T; i++) mod.noteLeaseError(new Error('getaddrinfo ENOTFOUND pooler'))
    const afterFirst = h.sent.length
    mod.noteLeaseError(new Error('getaddrinfo ENOTFOUND pooler'))
    ok(afterFirst === 1 && h.sent.length === 2,
       tag + 'C4. a page whose send FAILED is retried on the next error (gate: the unlatch)')
  }
  {
    // C5: the honest liveness stamp. _lastLeaseOkAt must NOT move on the throw path.
    const h = harness(mod)
    mod._resetLeaseErrorState({ lastOkAt: 0 })
    for (let i = 0; i < T; i++) mod.noteLeaseError(new Error('boom'))
    ok(mod._getLeaseErrorState().lastOkAt === 0,
       tag + 'C5. lease exceptions do NOT stamp the ok-clock (the heartbeat that read green for 8h)')
    mod.noteLeaseOk()
    ok(mod._getLeaseErrorState().lastOkAt > 0, tag + 'C6. a returning lease DOES stamp it')
  }
  try { fs.unlinkSync(CRUMB) } catch (_e) {}
}

// ── mutation mode ────────────────────────────────────────────────────────────
// Deletes one gate at a time from a COPY of the real source (kept inside tools/ so
// its relative requires still resolve) and asserts the matching control goes red.
const MUTATIONS = [
  { id: 'M1', gate: 'the count threshold', expect: 'C1.',
    from: 'if (_consecutiveLeaseErrors < LEASE_ERROR_PAGE_THRESHOLD || _leaseErrorPageSent) return null',
    to:   'if (_leaseErrorPageSent) return null' },
  { id: 'M2', gate: 'the reset in noteLeaseOk', expect: 'C2.',
    from: '  _consecutiveLeaseErrors = 0\n  _leaseErrorFirstAt = null',
    to:   '  _leaseErrorFirstAt = null' },
  { id: 'M3', gate: 'the one-page latch', expect: 'C3.',
    from: 'if (_consecutiveLeaseErrors < LEASE_ERROR_PAGE_THRESHOLD || _leaseErrorPageSent) return null',
    to:   'if (_consecutiveLeaseErrors < LEASE_ERROR_PAGE_THRESHOLD) return null' },
  { id: 'M4', gate: 'the unlatch on a failed send', expect: 'C4.',
    from: '      _leaseErrorPageSent = false\n      process.stderr.write(\'[scheduler] LEASE OUTAGE PAGE send FAILED',
    to:   '      process.stderr.write(\'[scheduler] LEASE OUTAGE PAGE send FAILED' },
]

if (process.argv.indexOf('--mutate') !== -1) {
  const SRC = path.join(__dirname, 'scheduler.js')
  const MUT = path.join(__dirname, 'scheduler.__mutant.js')
  const src = fs.readFileSync(SRC, 'utf8')
  let mpass = 0, mfail = 0
  for (const m of MUTATIONS) {
    if (src.indexOf(m.from) === -1) {
      console.log('  FAIL ' + m.id + ' target text not found - the mutation is stale, not the code')
      mfail++; continue
    }
    fs.writeFileSync(MUT, src.replace(m.from, m.to))
    const r = require('child_process').spawnSync(process.execPath, [__filename, '--mutant-run'], {
      encoding: 'utf8',
      env: Object.assign({}, process.env, { LEASE_PAGER_MODULE: MUT }),
    })
    try { fs.unlinkSync(MUT) } catch (_e) {}
    const out = (r.stdout || '') + (r.stderr || '')
    const control = out.split('\n').find(l => l.indexOf(m.expect) !== -1) || ''
    const flipped = /^\s*FAIL/.test(control)
    if (flipped) { mpass++; console.log('  ok   ' + m.id + ' deleting ' + m.gate + ' turns ' + m.expect + ' RED (the control is load-bearing)') }
    else { mfail++; console.log('  FAIL ' + m.id + ' deleting ' + m.gate + ' left ' + m.expect + ' GREEN - that control proves nothing') }
  }
  console.log('mutations: pass=' + mpass + ' fail=' + mfail)
  process.exit(mfail > 0 ? 1 : 0)
}

const modPath = process.env.LEASE_PAGER_MODULE || path.join(__dirname, 'scheduler.js')
run(require(modPath), '')
console.log('scheduler-lease-pager: pass=' + pass + ' fail=' + fail)
if (fail) console.log('failed: ' + failures.join(' | '))
process.exit(fail > 0 ? 1 : 0)
