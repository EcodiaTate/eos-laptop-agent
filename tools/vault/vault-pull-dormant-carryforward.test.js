'use strict'
/**
 * Dormant-account carry-forward: an empty statement must still stamp cash truth.
 *
 * Why: importBankCsv reads liveBalance ONLY off a transaction row's Balance column, so an
 * account with no transactions in the export window yields a header-only CSV, liveBalance
 * stays null, and NO bank_reconciliation row is written at all. The account's as_of_date then
 * freezes at its last day of activity while every staleness surface keeps counting up. An
 * account measured on transaction date can NEVER satisfy a freshness window once it goes
 * dormant: the only thing that would clear the alarm is a transaction, and a dormant account
 * has none. That is a permanent false alarm with nothing able to clear it.
 *
 * Measured on ba_personal_savings: emptied to $0.00 on 2026-08-14, and the 2026-08-31 one-tap
 * export DID cover it (vault_inbox dbb9804d, sig_verified true, navDom url .../accounts/history/
 * elCount 199, so the account is reachable and OPEN, merely dormant). Its signed payload decodes
 * to exactly the CSV header and nothing else. It stamped nothing and sat 21d stale in a 35d
 * window, heading for a permanent alarm.
 *
 * This needs a test rather than an eyeball for the same reason the approval-id guard did:
 * nothing downstream goes red when it regresses. The account simply stops being stamped, and
 * the failure only surfaces weeks later as an unclearable staleness alert.
 */
const assert = require('assert')
const { importBankCsv } = require('./vault-pull.js')

// The EXACT payload the 2026-08-31 export returned for ba_personal_savings, decoded from
// vault_inbox dbb9804d-ed86-4133-bad9-cbf3a6ccdc6c.
const HEADER_ONLY_CSV = 'Effective Date,Entered Date,Transaction Description,Amount,Balance\r\n'

function makeMockDb(priorRecon) {
  const staged = []
  const recon = priorRecon ? priorRecon.slice() : []
  const db = (strings, ...values) => {
    const q = strings.join(' ')
    if (/SELECT bank_balance, as_of_date FROM public\.bank_reconciliation/.test(q)) {
      const account = values[0]
      const hits = recon.filter(r => r.account_code === account)
        .sort((a, b) => (a.as_of_date < b.as_of_date ? 1 : -1))
      return Promise.resolve(hits.slice(0, 1))
    }
    if (/SELECT count\(\*\)::int n FROM public\.staged_transactions/.test(q)) {
      return Promise.resolve([{ n: 0 }])
    }
    if (/INSERT INTO public\.staged_transactions/.test(q)) { staged.push(values); return Promise.resolve([]) }
    if (/INSERT INTO public\.bank_reconciliation/.test(q)) {
      const account = values[0], asOfDate = values[1], bal = values[2], note = values[3]
      const exists = recon.some(r => r.account_code === account && r.as_of_date === asOfDate
        && /^phone-vault-feed/.test(r.notes || ''))
      if (exists) return Promise.resolve([])
      recon.push({ account_code: account, as_of_date: asOfDate, bank_balance: bal, notes: note })
      return Promise.resolve([{ id: 'recon-' + recon.length }])
    }
    if (/DELETE FROM public\.staged_transactions/.test(q)) { const r = []; r.count = 0; return Promise.resolve(r) }
    throw new Error('mock db: unhandled query: ' + q)
  }
  return { db, staged, recon }
}

const PRIOR = [{ account_code: 'ba_personal_savings', as_of_date: '2026-08-14', bank_balance: 0,
  notes: 'phone-vault-feed 2026-08-14 via vault-pull.js normaliser: current 0.00' }]

let pass = 0
const results = []
function t(name, fn) { results.push(fn().then(() => { pass++; console.log('  ok - ' + name) })) }

console.log('importBankCsv dormant-account carry-forward')

t('an empty statement STAMPS the carried balance at the export date (the regression this exists for)', async () => {
  const m = makeMockDb(PRIOR)
  const out = await importBankCsv(m.db, HEADER_ONLY_CSV, 'ba_personal_savings', '2026-08-31')
  assert.strictEqual(out.totalStaged, 0, 'no transactions should be staged')
  assert.strictEqual(out.liveBalance, null, 'liveBalance is genuinely absent from a header-only CSV')
  assert.strictEqual(out.carriedForwardFrom, '2026-08-14', 'carries from the last known reconciliation')
  const written = m.recon.filter(r => r.as_of_date === '2026-08-31')
  assert.strictEqual(written.length, 1, 'exactly one row stamped at the export date')
  assert.strictEqual(written[0].bank_balance, 0, 'the balance carried is the prior balance')
})

t('the note keeps the phone-vault-feed PREFIX so the idempotency guard still matches', async () => {
  const m = makeMockDb(PRIOR)
  await importBankCsv(m.db, HEADER_ONLY_CSV, 'ba_personal_savings', '2026-08-31')
  const w = m.recon.find(r => r.as_of_date === '2026-08-31')
  assert.ok(/^phone-vault-feed/.test(w.notes), 'prefix is load-bearing for the NOT-EXISTS guard')
  assert.ok(/CARRIED FORWARD/.test(w.notes), 'and it is visibly marked as carried, not observed')
  assert.ok(/unchanged since 2026-08-14/.test(w.notes), 'and it names what it carried from')
})

t('re-running the same export adds ZERO rows (idempotent)', async () => {
  const m = makeMockDb(PRIOR)
  await importBankCsv(m.db, HEADER_ONLY_CSV, 'ba_personal_savings', '2026-08-31')
  const after1 = m.recon.length
  await importBankCsv(m.db, HEADER_ONLY_CSV, 'ba_personal_savings', '2026-08-31')
  assert.strictEqual(m.recon.length, after1, 'second identical run must add nothing')
})

t('CONTROL: it never claims freshness the export did not have (stamps the SIGNED export date, not today)', async () => {
  const m = makeMockDb(PRIOR)
  await importBankCsv(m.db, HEADER_ONLY_CSV, 'ba_personal_savings', '2026-08-31')
  const today = new Date().toISOString().slice(0, 10)
  assert.ok(!m.recon.some(r => r.as_of_date === today && today !== '2026-08-31'),
    'a carry-forward must never stamp today when the export ran earlier')
})

t('CONTROL: a first-ever empty statement with no prior row stamps NOTHING (invents no balance)', async () => {
  const m = makeMockDb([])
  const out = await importBankCsv(m.db, HEADER_ONLY_CSV, 'ba_personal_savings', '2026-08-31')
  assert.strictEqual(out.carriedForwardFrom, null, 'nothing to carry')
  assert.strictEqual(m.recon.length, 0, 'must not fabricate a balance out of nothing')
})

t('CONTROL: a NON-empty statement is untouched and still stamps its own observed balance', async () => {
  const m = makeMockDb(PRIOR)
  const csv = 'Effective Date,Entered Date,Transaction Description,Amount,Balance\r\n'
    + ',31/08/2026,"Received from SAV 12579151 Transfer from T J Donohoe -",$100.00,$111.80\r\n'
  const out = await importBankCsv(m.db, csv, 'ba_personal', '2026-08-31')
  assert.strictEqual(out.carriedForwardFrom, null, 'carry-forward must NOT fire when a balance was observed')
  assert.strictEqual(out.liveBalance, 11180, 'observed balance wins')
  const w = m.recon.find(r => r.account_code === 'ba_personal')
  assert.ok(!/CARRIED FORWARD/.test(w.notes), 'an observed balance is never marked as carried')
})

// ---------------------------------------------------------------------------------------------
// OBSERVATION DATE vs MOVEMENT DATE.
//
// The cases above all use a transaction dated the SAME day as the export, so they cannot tell
// `asOfDate || liveBalanceDate` from `liveBalanceDate || asOfDate`: both orderings return the
// identical string. That coincidence is why the real defect survived a green suite. Every case
// below deliberately separates the two dates, so each one goes red if the ordering is reverted.
//
// The live shape being pinned: ba_personal_savings on the 2026-09-05 tap. Its CSV was NOT empty
// (so the carry-forward branch above is unreachable), it held exactly one row dated 31/08/2026,
// and it was stamped 2026-08-31 for the second running while the export proving the balance was
// two days old. A dormant account whose last movement is still inside BA's export window can
// therefore never be refreshed by any number of taps.

// The EXACT payload the 2026-09-05 export returned for ba_personal_savings, decoded from
// vault_inbox 34071a81-5169-4e68-97a2-ba0a9cf9e235.
const DORMANT_ONE_OLD_TXN_CSV = 'Effective Date,Entered Date,Transaction Description,Amount,Balance\r\n'
  + ',31/08/2026,"Interest Credit",$0.01,$0.01\r\n'

t('a dormant account with ONE OLD txn stamps the OBSERVATION date, not the movement date', async () => {
  const m = makeMockDb(PRIOR)
  const out = await importBankCsv(m.db, DORMANT_ONE_OLD_TXN_CSV, 'ba_personal_savings', '2026-09-05')
  assert.strictEqual(out.liveBalance, 1, 'the balance is observed, so carry-forward must not fire')
  assert.strictEqual(out.carriedForwardFrom, null, 'this is the OBSERVED branch, not the carried one')
  assert.ok(m.recon.some(r => r.as_of_date === '2026-09-05'),
    'the export date is what we observed the balance at')
  assert.ok(!m.recon.some(r => r.account_code === 'ba_personal_savings' && r.as_of_date === '2026-08-31'),
    'stamping the old transaction date is the defect: it can never be refreshed by a later tap')
})

t('the note keeps the MOVEMENT date visible, so a stale-CSV read is not masked', async () => {
  const m = makeMockDb(PRIOR)
  await importBankCsv(m.db, DORMANT_ONE_OLD_TXN_CSV, 'ba_personal_savings', '2026-09-05')
  const w = m.recon.find(r => r.as_of_date === '2026-09-05')
  assert.ok(/^phone-vault-feed/.test(w.notes), 'prefix stays load-bearing for the NOT-EXISTS guard')
  assert.ok(/NO MOVEMENT SINCE 2026-08-31/.test(w.notes), 'the movement date must remain readable')
  assert.ok(/observed 2026-09-05/.test(w.notes), 'and it must say when we actually looked')
  assert.ok(!/CARRIED FORWARD/.test(w.notes), 'an observed balance is never marked as carried')
})

t('a REPEAT tap of a dormant account advances as_of_date (the whole point: it can be refreshed)', async () => {
  const m = makeMockDb(PRIOR)
  await importBankCsv(m.db, DORMANT_ONE_OLD_TXN_CSV, 'ba_personal_savings', '2026-09-05')
  await importBankCsv(m.db, DORMANT_ONE_OLD_TXN_CSV, 'ba_personal_savings', '2026-09-12')
  const dates = m.recon.filter(r => r.account_code === 'ba_personal_savings').map(r => r.as_of_date).sort()
  assert.ok(dates.includes('2026-09-05') && dates.includes('2026-09-12'),
    'each tap stamps its own observation date, so the account tracks the feed rather than freezing')
})

t('CONTROL: an ACTIVE account whose txn IS the export day is unchanged (same date either way)', async () => {
  const m = makeMockDb(PRIOR)
  const csv = 'Effective Date,Entered Date,Transaction Description,Amount,Balance\r\n'
    + ',05/09/2026,"Card purchase",$-12.50,$99.30\r\n'
  await importBankCsv(m.db, csv, 'ba_ecodia', '2026-09-05')
  const w = m.recon.find(r => r.account_code === 'ba_ecodia')
  assert.strictEqual(w.as_of_date, '2026-09-05', 'unchanged for the common active-account case')
  assert.ok(!/NO MOVEMENT SINCE/.test(w.notes),
    'no marker when the movement IS the observation: the marker must mean something')
})

t('CONTROL: a ts-less read still falls back to the movement date rather than inventing today', async () => {
  const m = makeMockDb(PRIOR)
  await importBankCsv(m.db, DORMANT_ONE_OLD_TXN_CSV, 'ba_personal_savings', null)
  const today = new Date().toISOString().slice(0, 10)
  assert.ok(m.recon.some(r => r.as_of_date === '2026-08-31'),
    'with no export ts, the newest transaction date is the best evidence we have')
  assert.ok(!m.recon.some(r => r.as_of_date === today && today !== '2026-08-31'),
    'and it must not claim today when nothing proves today')
})

Promise.all(results).then(() => {
  console.log(`\nvault-pull dormant carry-forward: ${pass} passed`)
}).catch((e) => { console.error('FAILED:', e.message); process.exit(1) })
