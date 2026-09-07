'use strict'
// tools/vault/vault-pull.js - the Mac conductor end of the phone->host relay. Reads
// unconsumed rows from public.vault_inbox (written by the vault-ingest edge function),
// AUTHORITATIVELY verifies each Secure Enclave signature here on the Mac against the
// paired key (the relay is not trusted; the signature is), applies verified bank results
// to the persisted ledger, and marks every row consumed. An unsigned or bad-signature
// row is consumed as rejected (garbage-collected), never acted on.
//
//   node vault-pull.js            pull + apply once
// Env overrides (for tests, so the real pairing/ledger are never touched):
//   VAULT_PAIRING=<path>   pairing json (default ~/PRIVATE/ecodia-creds/vault/phone-pairing.json)
//   VAULT_LEDGER=<path>    ledger json  (default ~/PRIVATE/ecodia-creds/vault/bank-ledger.json)
const fs = require('fs')
const path = require('path')
const os = require('os')
const crypto = require('crypto')
const { spkiFromX963, canonical, valueBound } = require('./inbox.js')
const { createLedger } = require('./bank-ledger.js')

const VDIR = path.join(os.homedir(), 'PRIVATE', 'ecodia-creds', 'vault')
const PAIRING = process.env.VAULT_PAIRING || path.join(VDIR, 'phone-pairing.json')
const LEDGER = process.env.VAULT_LEDGER || path.join(VDIR, 'bank-ledger.json')

function loadJson(f, dflt) { try { return JSON.parse(fs.readFileSync(f, 'utf8')) } catch (_e) { return dflt } }
function saveJson(f, o) { fs.mkdirSync(path.dirname(f), { recursive: true, mode: 0o700 }); fs.writeFileSync(f, JSON.stringify(o, null, 2), { mode: 0o600 }) }

// Authoritative verify: does this message carry a valid signature from the paired phone?
function verify(msg) {
  const pairing = loadJson(PAIRING, null)
  if (!pairing || !pairing.signing || !msg.sig) return false
  try {
    const key = crypto.createPublicKey({ key: spkiFromX963(Buffer.from(pairing.signing, 'base64')), format: 'der', type: 'spki' })
    const sigOk = crypto.verify(null, canonical(msg), { key, dsaEncoding: 'der' }, Buffer.from(msg.sig, 'base64'))
    return sigOk && valueBound(msg)   // a swapped value (hash mismatch) fails even with a valid sig
  } catch (_e) { return false }
}

// App Attest ENFORCEMENT: the paired signing key is only trusted if Apple attested it is a
// genuine Secure-Enclave key on a real device running our app. Without this, a software
// impostor key paired through a compromised step would pass signature verification. The bind
// requires a vault_attested_keys row whose bound_signing_x963 equals the paired signing key
// (i.e. the App Attest ceremony committed to THIS exact key), with a real App Attest aaguid.
async function pairedKeyIsAttested(db) {
  const pairing = loadJson(PAIRING, null)
  if (!pairing || !pairing.signing) return false
  if (process.env.VAULT_SKIP_ATTEST === '1') return true   // tests inject their own pairing
  try {
    const r = await db`SELECT 1 FROM public.vault_attested_keys
                       WHERE bound_signing_x963 = ${pairing.signing} AND aaguid IN ('appattest', 'appattestdevelop') LIMIT 1`
    return r.length > 0
  } catch (_e) { return false }
}

// Self-heal a clobbered pairing file. App Attest is the source of truth for which signing key is
// genuine Secure-Enclave hardware (the ceremony binds the exact key). If phone-pairing.json's
// signing key is NOT among the attested keys - e.g. a re-pair/enroll overwrote it with a wrong
// key, which silently REJECTED every valid capture on 2026-08-15 - restore it from the attested
// key. Only heals when the attested key is unambiguous (exactly one distinct value); never guesses.
async function healPairingIfClobbered(db) {
  try {
    const fs = require('fs')
    // Only ever heal the CANONICAL pairing file. A test injects its own throwaway pairing via the
    // VAULT_PAIRING override (or VAULT_SKIP_ATTEST); healing that from the REAL attested keys would
    // overwrite the test's key and make its own signature fail to verify - the exact regression this
    // guards (it reddened vault-pull / vault-replay / vault-attest-enforce E2E after f430470 added
    // heal). Both env vars are documented test-only overrides; production sets neither, so heal runs.
    if (process.env.VAULT_PAIRING || process.env.VAULT_SKIP_ATTEST === '1') return false
    const pairing = loadJson(PAIRING, null)
    if (!pairing || !pairing.signing) return false
    const att = await db`SELECT DISTINCT bound_signing_x963 FROM public.vault_attested_keys WHERE aaguid IN ('appattest','appattestdevelop')`
    const distinct = att.map(a => a.bound_signing_x963)
    if (distinct.length !== 1) return false                       // none or ambiguous: do not touch
    if (distinct[0] === pairing.signing) return false             // already correct
    fs.copyFileSync(PAIRING, PAIRING + '.clobber-bak-' + Date.now())
    pairing.signing = distinct[0]
    fs.writeFileSync(PAIRING, JSON.stringify(pairing, null, 2), { mode: 0o600 })
    return true
  } catch (_e) { return false }
}

// Freshness: the signed `ts` (inside the canonical, so an attacker cannot change it without
// breaking the signature) must be recent - a captured old result cannot be replayed later.
// ts-less messages skip this (dedup-by-sig still applies). Rejects future-dated too.
function isFresh(msg, maxAgeMin = 15) {
  if (!msg.ts) return true
  const t = Date.parse(msg.ts)
  if (Number.isNaN(t)) return true
  const now = Date.now()
  return (now - t) <= maxAgeMin * 60_000 && t <= now + 5 * 60_000
}

// Replay: has this EXACT signature already been consumed? An identical re-post is a replay.
async function sigAlreadySeen(db, sig, thisId) {
  if (!sig) return false
  const r = await db`SELECT 1 FROM public.vault_inbox WHERE payload->>'sig' = ${sig} AND consumed_at IS NOT NULL AND id != ${thisId} LIMIT 1`
  return r.length > 0
}

// Apply a verified bank-statement result to the persisted ledger.
function applyBankResult(msg) {
  const state = loadJson(LEDGER, { startingBalance: 0, transactions: [] })
  const L = createLedger({ startingBalance: state.startingBalance, transactions: state.transactions })
  const applied = L.applyScrape(Array.isArray(msg.transactions) ? msg.transactions : [])
  const snap = L.snapshot()
  saveJson(LEDGER, { startingBalance: snap.startingBalance, transactions: snap.transactions, updatedAt: new Date().toISOString() })
  const rec = msg.balance != null ? L.reconcile(msg.balance) : null
  return { added: applied.added, skipped: applied.skipped, runningBalance: applied.runningBalance, reconcile: rec }
}

// ---- Phase 1 bank-feed bridge: BA phone-vault body -> public.staged_transactions ----
// The phone scrapes the BA online-banking account+history page as a FLATTENED text blob and
// posts it as the signed `value` (kind:'field'), NOT as a structured `transactions` array.
// applyBankResult only consumes msg.transactions, so a verified BA read fed the JSON ledger
// nothing AND staged_transactions nothing - the world-model actuals engine ran on pre-27-Jul
// data. This normaliser parses the blob per account and lands rows in staged_transactions
// using the EXACT dedup key + count-topup of budget/import-ba-csv.cjs, keyed on
// (source_account, occurred_at, description, amount_cents), so a later CSV import of the same
// transactions dedups against these and never double-counts (the single critical hazard).
// import-ba-csv's dedup is INLINE (not a shared export) and lives on the budget Management-API
// `sql/lit` client; here we replicate it identically on the postgres tagged-template `db`.

const BA_ACCOUNT_MAP = {
  '12579148': 'ba_ecodia',          // Commercial Access (Ecodia Pty Ltd)
  '12579151': 'ba_ecodia_savings',  // Commercial Saver
  '12566110': 'ba_personal',        // Tate Everyday
  '12566111': 'ba_personal_savings', // Tate Saver
}

// Build 29 recon (2026-08-15) proved BA's statement page carries the SELECTED account as a HIDDEN
// <input> whose value is the 8-digit account number, and the navDom element map lands that value in
// el.text. That hidden field is the reliable attribution key: on Tate's build-29 pass exactly one BA
// number (12566110) appeared as an element value, while the VISIBLE pageContext listed three numbers
// (ambiguous). Match an element whose text is EXACTLY a known BA account number (whole-value match,
// so a transaction description containing digits cannot false-positive); require exactly one distinct.
// public.vault_approvals.id is a uuid, so only a uuid-shaped requestId can name an approval.
// Every other requestId is a caller's trace tag (the bank-feed cron's 'ba-cron-YYYYMMDD'),
// which names the recon file and nothing else. Whole-string match: a tag that merely CONTAINS
// a uuid is still a tag.
const APPROVAL_ID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i
function isApprovalRequestId(requestId) {
  return typeof requestId === 'string' && APPROVAL_ID_RE.test(requestId.trim())
}

function accountFromNavDom(navDom) {
  if (!navDom) return null
  let parsed
  try { parsed = JSON.parse(String(navDom)) } catch (_e) { return null }
  const pages = Array.isArray(parsed) ? parsed : [parsed]
  const nums = new Set()
  for (const pg of pages) {
    for (const el of (pg && Array.isArray(pg.els) ? pg.els : [])) {
      const v = String((el && el.text) || '').trim()
      if (Object.prototype.hasOwnProperty.call(BA_ACCOUNT_MAP, v)) nums.add(v)
    }
  }
  return nums.size === 1 ? BA_ACCOUNT_MAP[[...nums][0]] : null
}

// Attribute a captured BA CSV (kind:'bank-csv') to a source_account. BA names every export
// StatementCsv.csv and the CSV body has NO account column, so attribution comes from the driver:
//   1. an explicit valid msg.account - a legacy per-account link, OR the phone's own confident
//      local resolve from the on-screen account number (bankfeed).
//   2. else the hash-bound navDom hidden account field (build 29): deterministic, single account.
//   3. else the hash-bound pageContext (the statement page text the phone read at download time):
//      attribute ONLY when EXACTLY one known BA account number appears. Zero or many -> null.
// NEVER guess: a wrong tag silently corrupts the world-model cash truth, so an unresolved CSV is
// captured-but-flagged (left for triage with its context intact), never mis-imported.
// pageContext/navDom are bound by their *Sha256 (inside the signed canonical), so a message that
// already passed the signature gate carries exactly what the phone signed; we recompute to be explicit.
function resolveBankCsvAccount(msg) {
  if (msg.account && BA_CSV_ACCOUNTS.includes(msg.account)) return { account: msg.account, via: 'tag' }
  const nd = String(msg.navDom || '')
  if (nd && (!msg.navDomSha256 || crypto.createHash('sha256').update(nd).digest('hex') === msg.navDomSha256)) {
    const a = accountFromNavDom(nd)
    if (a) return { account: a, via: 'navDom-hidden-account-field' }
  }
  const ctx = String(msg.pageContext || '')
  if (!ctx) return { account: null, via: 'no account tag, no navDom account, no pageContext' }
  if (msg.pageContextSha256 && crypto.createHash('sha256').update(ctx).digest('hex') !== msg.pageContextSha256) {
    return { account: null, via: 'pageContext hash mismatch' }
  }
  const hits = Object.keys(BA_ACCOUNT_MAP).filter(num => ctx.includes(num))
  if (hits.length === 1) return { account: BA_ACCOUNT_MAP[hits[0]], via: 'pageContext:' + hits[0] }
  return { account: null, via: hits.length ? `ambiguous (${hits.length} account numbers on page)` : 'no known account number on page' }
}

// Merge post-login DOM recon pages into the per-requestId recon file, NEVER clobbering. Two
// producers land here in ONE pull: the per-capture `navDom` (build 29: the POST-Search page, from
// each bank-csv message, carrying BA's account picker + export button + selected account) and the
// session-end kind:'bank-dom-recon' flush (the page-LOAD pages, balances + history-preload). They
// share a requestId, so both write this file; the bank-csv rows are consumed before the flush row,
// so a plain writeFileSync in the flush branch would ERASE the richer navDom pages. Merging keeps
// the RICHEST dump per URL (more elements) - so the post-Search /accounts/history/ page (picker +
// export) wins over its page-load twin. baseDir is injectable for tests. Returns {file, pages}.
function mergeReconPages(requestId, incomingPages, baseDir) {
  const reconDir = baseDir || path.join(os.homedir(), '.local/state/ecodiaos')
  fs.mkdirSync(reconDir, { recursive: true })
  const f = path.join(reconDir, `bank-dom-recon-${String(requestId || 'x').replace(/[^\w.-]/g, '_')}.json`)
  let existing = []
  try { const p = JSON.parse(fs.readFileSync(f, 'utf8')); if (Array.isArray(p)) existing = p } catch (_e) { /* first write */ }
  const byUrl = new Map()
  for (const pg of existing.concat(Array.isArray(incomingPages) ? incomingPages : [incomingPages])) {
    if (!pg || typeof pg !== 'object' || !pg.url || pg.skip) continue   // skip login-page / malformed entries
    const n = Array.isArray(pg.els) ? pg.els.length : (pg.elCount || 0)
    const prev = byUrl.get(pg.url)
    const pn = prev ? (Array.isArray(prev.els) ? prev.els.length : (prev.elCount || 0)) : -1
    if (!prev || n >= pn) byUrl.set(pg.url, pg)
  }
  const merged = Array.from(byUrl.values())
  fs.writeFileSync(f, JSON.stringify(merged, null, 2))
  return { file: f, pages: merged.length }
}

// Same transfer test import-ba-csv.cjs uses.
const BA_TRANSFER_RE = /Transfer to SAV|Received from SAV|Transfer from/i

// Direction is derived from BA's DESCRIPTION wording, NEVER the scraped dash count: the
// online-banking text uses '--$' for internal transfers, '-$' for card purchases AND for
// credited "Received from" rows, and plain '$' for Osko credits - so a single '-' prefixes
// both a debit (card) and a credit (received-from), making the dash unreliable. signChars is
// only a last-resort tiebreaker for wording the explicit rules do not recognise.
function baDirectionSign(descr, signChars) {
  const d = (descr || '').trim()
  const OUT = /^transfer to\b|^visa\b|^visa-|osko payment to\b|^eftpos\b|^bpay\b|direct debit|^withdrawal\b|^atm\b|^purchase\b/i
  const IN = /received from\b|osko payment from\b|^deposit\b|^interest\b|^salary\b|refund\b|direct credit\b|^credit\b/i
  if (OUT.test(d)) return -1
  if (IN.test(d)) return 1
  if (signChars === '--') return -1                     // double dash = internal transfer OUT
  if (signChars === '+' || signChars === '') return 1   // explicit plus / no dash = credit IN
  return -1                                             // lone unknown '-' -> treat as debit
}

// Does this value blob look like a BA account+history body (vs some other scraped field)?
function looksLikeBaBody(value) {
  return /Bank Australia|DateDescriptionAmount|Acc 1256|Acc 1257/i.test(String(value || ''))
}

function isBankResult(msg) {
  return msg.kind === 'bank-statement' || /bank/i.test(msg.service || '') || (msg.value != null && looksLikeBaBody(msg.value))
}

// Parse the flattened BA multi-account body into normalised rows.
// Each account section carries an `Acc <number>` header line; its transactions follow a
// `DateDescriptionAmount` marker, one per line shaped `DD/MM/YYYY<description>[--|-|+]$<amount>`.
// $0.00 "Insufficient funds #51" rows are FAILED debits: captured (real dunning signal) with
// amount_cents 0 so they never move a balance.
function parseBaBody(value) {
  const lines = String(value || '').split(/\r?\n/)
  const rowRe = /^(\d{2})\/(\d{2})\/(\d{4})(.+?)(--|-|\+)?\$([\d,]+\.\d{2})\s*$/
  const accRe = /^Acc\s+(\d{6,12})\s*$/
  const availRe = /^Avail\.\s+\$([\d,]+\.\d{2})\s*$/
  const currRe = /^Curr\.\s+\$([\d,]+\.\d{2})\s*$/
  const cents = (s) => Math.round(parseFloat(s.replace(/,/g, '')) * 100)
  let currentAccount = null
  const rows = []
  const balances = {}   // source_account -> { curr_cents, avail_cents } (first-seen per account)
  const unmapped = new Set()
  for (const raw of lines) {
    const line = raw.trim()
    const am = line.match(accRe)
    if (am) {
      currentAccount = BA_ACCOUNT_MAP[am[1]] || null
      if (!currentAccount) unmapped.add(am[1])
      continue
    }
    if (currentAccount) {
      const av = line.match(availRe)
      if (av) { (balances[currentAccount] ||= {}); if (balances[currentAccount].avail_cents == null) balances[currentAccount].avail_cents = cents(av[1]); continue }
      const cu = line.match(currRe)
      if (cu) { (balances[currentAccount] ||= {}); if (balances[currentAccount].curr_cents == null) balances[currentAccount].curr_cents = cents(cu[1]); continue }
    }
    const m = line.match(rowRe)
    if (!m) continue
    if (!currentAccount) continue
    const [, dd, mm, yyyy, descrRaw, signChars, amountRaw] = m
    const occurred = `${yyyy}-${mm}-${dd}`
    const magnitude = Math.round(parseFloat(amountRaw.replace(/[$,]/g, '')) * 100)
    const descr = descrRaw.trim()
    const isDecline = magnitude === 0
    const amt = isDecline ? 0 : baDirectionSign(descr, signChars || '') * magnitude
    rows.push({ source_account: currentAccount, occurred, cents: amt, descr, isDecline })
  }
  return { rows, balances, unmapped: [...unmapped] }
}

// Record each account's CURRENT (Curr.) balance into bank_reconciliation, the substrate the
// world-model brief + survival sim read as "cash truth". Mirrors the balance-recording step
// import-ba-csv.cjs performs on every import (bank_balance = statement current balance, NOT
// available - consistent with the existing CSV-derived rows), but idempotent: a NOT-EXISTS
// guard on (account_code, as_of_date, phone-vault provenance) means re-running the same read
// adds zero rows. Available + uncleared are carried in the note for the current-vs-available
// gap (e.g. an inbound transfer sitting as a pending hold).
async function recordBalances(db, balances, asOfDate, opts = {}) {
  const out = {}
  for (const [account, b] of Object.entries(balances)) {
    if (b.curr_cents == null) continue
    const avail = b.avail_cents == null ? null : b.avail_cents
    const uncleared = avail == null ? null : b.curr_cents - avail
    // The 'phone-vault-feed' PREFIX is load-bearing: the NOT-EXISTS guard below keys on
    // `notes LIKE 'phone-vault-feed%'`, so a carry-forward marker is APPENDED, never prefixed.
    const note = `phone-vault-feed ${asOfDate} via vault-pull.js normaliser: current ${(b.curr_cents / 100).toFixed(2)}`
      + (avail == null ? '' : `, available ${(avail / 100).toFixed(2)}${uncleared ? `, uncleared ${(uncleared / 100).toFixed(2)}` : ''}`)
      + (opts.carriedForwardFrom ? ` (CARRIED FORWARD: empty statement, dormant account, balance unchanged since ${opts.carriedForwardFrom})` : '')
      + (opts.observedTxnDate && opts.observedTxnDate !== asOfDate
        ? ` (NO MOVEMENT SINCE ${opts.observedTxnDate}: balance read off that transaction, account observed ${asOfDate})` : '')
    const res = await db`INSERT INTO public.bank_reconciliation
      (id, account_code, as_of_date, bank_balance, ledger_balance, difference, status, notes)
      SELECT gen_random_uuid(), ${account}, ${asOfDate}, ${b.curr_cents}, 0, 0, 'reconciled', ${note}
      WHERE NOT EXISTS (SELECT 1 FROM public.bank_reconciliation
        WHERE account_code = ${account} AND as_of_date = ${asOfDate} AND notes LIKE 'phone-vault-feed%')
      RETURNING id`
    out[account] = { curr_cents: b.curr_cents, avail_cents: avail, recorded: res.length > 0 }
  }
  return out
}

// SHARED STAGER - the single count-topup + insert loop used by BOTH the phone-scrape body path
// (normaliseBankValueToStaged) and the phone-delivered CSV path (importBankCsv). Replicates
// import-ba-csv.cjs's grouping + count-topup EXACTLY: group identical
// (account,date,amount,desc) rows so a genuine second identical charge in one window is kept,
// while a re-run of the same window adds nothing. Because ALL THREE feed paths (Tate's manual
// CSV via import-ba-csv.cjs, the phone scrape, and the phone CSV) key on the IDENTICAL
// (source_account, occurred_at, amount_cents, description) tuple with the same md5 id input, a
// transaction that landed via any one of them dedups against the other two - no double-count.
// The id/source_ref PREFIX (opts.idPrefix / opts.refPrefix) is the ONLY per-path difference; it
// carries provenance and is NOT part of the dedup key, so it cannot cause a cross-path collision.
async function stageGroupedBaRows(db, rows, opts = {}) {
  const idPrefix = opts.idPrefix || 'bankvault_'
  const refPrefix = opts.refPrefix || 'ba-vault-'
  const groups = new Map()
  for (const r of rows) {
    const key = `${r.source_account}|${r.occurred}|${r.cents}|${r.descr}`
    if (!groups.has(key)) groups.set(key, { ...r, n: 0 })
    groups.get(key).n++
  }
  const stats = {}
  const bump = (a) => (stats[a] = stats[a] || { parsed: 0, inserted: 0, dupSkipped: 0, declines: 0 })
  for (const r of rows) { const s = bump(r.source_account); s.parsed++; if (r.isDecline) s.declines++ }
  for (const g of groups.values()) {
    const s = bump(g.source_account)
    const existing = await db`SELECT count(*)::int n FROM public.staged_transactions
      WHERE source_account = ${g.source_account} AND occurred_at = ${g.occurred}
        AND amount_cents = ${g.cents} AND description = ${g.descr}`
    const have = Number(existing[0].n)
    const need = g.n - have
    s.dupSkipped += Math.min(g.n, have)
    const isTransfer = BA_TRANSFER_RE.test(g.descr)
    const isPersonal = g.source_account.includes('personal')
    for (let k = 0; k < need; k++) {
      const occIdx = have + k + 1
      // md5 input is import-ba-csv.cjs's exact key shape (source_account|occurred|descr|cents||occIdx).
      const h = crypto.createHash('md5').update(`${g.source_account}|${g.occurred}|${g.descr}|${g.cents}` + occIdx).digest('hex')
      const id = idPrefix + h.slice(0, 18)
      const sourceRef = refPrefix + h.slice(0, 16)
      await db`INSERT INTO public.staged_transactions
        (id, source, source_ref, occurred_at, amount_cents, description, status, source_account, is_personal, is_transfer)
        VALUES (${id}, 'bank_australia', ${sourceRef}, ${g.occurred}, ${g.cents}, ${g.descr}, 'pending',
                ${g.source_account}, ${isPersonal}, ${isTransfer})`
      s.inserted++
    }
  }
  return stats
}

// Land parsed BA phone-scrape rows into staged_transactions via the shared stager, then refresh
// cash-truth balances (idempotent).
async function normaliseBankValueToStaged(db, value, asOfDate) {
  const { rows, balances, unmapped } = parseBaBody(value)
  const stats = await stageGroupedBaRows(db, rows)   // default bankvault_ / ba-vault- provenance
  // Refresh cash-truth balances (idempotent). as_of defaults to today if the read carried no ts.
  const asOf = asOfDate || new Date().toISOString().slice(0, 10)
  const balancesRecorded = await recordBalances(db, balances, asOf)
  return { stats, balances: balancesRecorded, asOf, unmapped, totalParsed: rows.length }
}

// ---- Phase 2: BA's OWN full-period CSV export (WKDownload-captured on the phone) ----
// The phone captures Bank Australia's own statement CSV inside the on-device WKWebView and POSTs
// it base64-encoded as the signed `value` with kind:'bank-csv' + account:'<source_account>'. This
// is the IDENTICAL file format budget/import-ba-csv.cjs already parses (signed Amount + running
// Balance), so we import it through the SAME parser shape + the SAME shared stager, and it dedups
// idempotently against the Phase 1 phone-scrape rows already in staged_transactions.

// parseCsv - copied VERBATIM from budget/import-ba-csv.cjs (that module runs main() on require,
// so it cannot be imported; keep this in sync if the canonical parser ever changes).
function parseCsv(text) {
  const rows = []
  let cur = [], field = '', inQ = false
  for (let i = 0; i < text.length; i++) {
    const c = text[i]
    if (inQ) {
      if (c === '"' && text[i + 1] === '"') { field += '"'; i++ }
      else if (c === '"') inQ = false
      else field += c
    } else if (c === '"') inQ = true
    else if (c === ',') { cur.push(field); field = '' }
    else if (c === '\n' || c === '\r') {
      if (field !== '' || cur.length) { cur.push(field); rows.push(cur); cur = []; field = '' }
      if (c === '\r' && text[i + 1] === '\n') i++
    } else field += c
  }
  if (field !== '' || cur.length) { cur.push(field); rows.push(cur) }
  return rows
}

const BA_CSV_ACCOUNTS = ['ba_ecodia', 'ba_personal', 'ba_ecodia_savings', 'ba_personal_savings']

// Parse a BA statement CSV into the SAME row shape the shared stager consumes. Mirrors
// import-ba-csv.cjs's field handling EXACTLY: header check on 'Transaction Description',
// occurred_at = Entered Date (col 1), descr = col 2, signed amount from col 3 (the CSV Amount is
// already signed, so NO baDirectionSign is needed - the export is authoritative), running Balance
// from col 4. $0.00 "Insufficient funds" rows are decline evidence and are NOT staged (matching
// import-ba-csv.cjs, which skips them), only counted.
function parseBaCsvRows(text, sourceAccount) {
  const parsed = parseCsv(String(text || ''))
  const header = parsed.shift()
  if (!header || !header.join(',').includes('Transaction Description')) {
    throw new Error('not a BA statement CSV (header mismatch): ' + (header ? header.join(',') : '(empty)'))
  }
  const rows = []
  let liveBalance = null, liveBalanceDate = null, declineEvents = 0
  for (const r of parsed) {
    if (r.length < 5) continue
    const [, entered, descr, amountRaw, balanceRaw] = r
    const dm = String(entered).match(/(\d{2})\/(\d{2})\/(\d{4})/)
    if (!dm) continue
    const occurred = `${dm[3]}-${dm[2]}-${dm[1]}`
    const cents = Math.round(parseFloat(String(amountRaw).replace(/[$,]/g, '')) * 100)
    if (liveBalance === null && balanceRaw) {
      liveBalance = Math.round(parseFloat(String(balanceRaw).replace(/[$,]/g, '')) * 100)
      liveBalanceDate = occurred
    }
    if (cents === 0) { if (String(descr).includes('Insufficient funds')) declineEvents++; continue }
    rows.push({ source_account: sourceAccount, occurred, cents, descr: String(descr).trim(), isDecline: false })
  }
  return { rows, liveBalance, liveBalanceDate, declineEvents }
}

// Import a phone-delivered BA CSV. Verifies nothing here (the caller has already SE-verified the
// signature); parses + stages through the shared count-topup, then records the running Balance as
// cash-truth (idempotent). Distinct provenance prefix (bacsv_/ba-phonecsv-) so a phone-delivered
// CSV is visible in the id, without affecting the dedup key.
async function importBankCsv(db, csvText, sourceAccount, asOfDate) {
  if (!BA_CSV_ACCOUNTS.includes(sourceAccount)) {
    throw new Error(`bank-csv: unknown/absent source_account "${sourceAccount}" (need one of ${BA_CSV_ACCOUNTS.join(', ')})`)
  }
  const { rows, liveBalance, liveBalanceDate, declineEvents } = parseBaCsvRows(csvText, sourceAccount)
  // The CSV is the AUTHORITATIVE full-fidelity record (signed amount + running balance) for the
  // range it covers. Supersede FIRST: delete any Phase-1 phone-SCRAPE rows (ba-vault-) inside that
  // range BEFORE staging, so the CSV becomes the sole record for [lo,hi]. Order matters: the scrape
  // collapsed whitespace and left a trailing dash artifact, so its dedup key never matches the
  // padded CSV and the overlap double-counts; but if a scrape row DID byte-match, staging first
  // would dedup-SKIP the CSV insert and then the delete would drop the txn entirely. Deleting
  // in-range is lossless (BA's export is complete for its range). Origin: ba_personal double-count 2026-08-14.
  let supersededScrapeRows = 0
  const occ = rows.map(r => r.occurred).filter(Boolean).sort()
  if (occ.length) {
    const del = await db`DELETE FROM public.staged_transactions
      WHERE source_account = ${sourceAccount} AND source_ref LIKE 'ba-vault-%'
        AND occurred_at >= ${occ[0]} AND occurred_at <= ${occ[occ.length - 1]}`
    supersededScrapeRows = del.count || 0
  }
  const stats = await stageGroupedBaRows(db, rows, { idPrefix: 'bacsv_', refPrefix: 'ba-phonecsv-' })
  let balances = {}
  let carriedForwardFrom = null
  if (liveBalance != null) {
    // AS-OF IS THE OBSERVATION DATE, NOT THE MOVEMENT DATE.
    //
    // liveBalanceDate is the `occurred` date of the NEWEST TRANSACTION (parseBaCsvRows), which is
    // when the money last moved. It is NOT when we looked. Stamping as_of_date with it says the
    // balance is only known as far back as the last transaction, and that is false: the export
    // rendered the account's own history page at asOfDate, so the running balance on the newest
    // row IS the balance at asOfDate. No transactions since simply means it has not changed.
    //
    // The bug this fixes: a DORMANT account whose last transaction is old but still inside BA's
    // export window takes THIS branch, not the carry-forward branch below, because one old
    // transaction is enough to make liveBalance non-null. It is then re-stamped at that same old
    // date on every single tap, so it ages forever and no export can ever refresh it. Only once
    // the transaction slides out of the export window does the CSV go header-only and the
    // carry-forward branch become reachable. Measured on ba_personal_savings: the 2026-09-05 tap
    // covered it (vault_inbox 34071a81, sig_verified, consumed 02:44:15Z), its CSV held exactly
    // one row dated 31/08/2026 for a $0.01 interest credit, and the account was stamped 2026-08-31
    // for the second time and read 7d stale while the export proving the balance was 2d old.
    //
    // The carry-forward branch below fixed only the strictly-empty case, and its non-empty control
    // could not have caught this: that fixture used a transaction dated 31/08 with asOfDate
    // 31/08, so both orderings of this expression produce the identical answer.
    //
    // Masking risk, handled rather than accepted: stamping the observation date would hide a
    // stale CSV served by the bank. So when the newest movement predates the observation, the
    // note records BOTH (see recordBalances), keeping the movement date visible to a human and
    // to any surface that wants it.
    const asOf = asOfDate || liveBalanceDate || new Date().toISOString().slice(0, 10)
    balances = await recordBalances(db, { [sourceAccount]: { curr_cents: liveBalance } }, asOf,
      { observedTxnDate: liveBalanceDate })
  } else if (rows.length === 0) {
    // DORMANT-ACCOUNT CARRY-FORWARD.
    //
    // liveBalance is only ever read off a TRANSACTION row's Balance column, so an account with
    // no transactions in the export window yields a header-only CSV, liveBalance stays null, and
    // the branch above never runs. No bank_reconciliation row is written at all, so the account's
    // as_of_date freezes at its last day of activity while every staleness surface keeps counting
    // up. An account measured on transaction date can therefore NEVER satisfy a freshness window
    // once it goes dormant: the alarm is permanent, and the only thing that would clear it is a
    // transaction, which by definition a dormant account does not have.
    //
    // Measured on ba_personal_savings: emptied to $0.00 on 2026-08-14, and the 2026-08-31 one-tap
    // export DID cover it (vault_inbox dbb9804d, sig_verified true, navDom url .../accounts/history/
    // with elCount 199, so the account is reachable and OPEN, merely dormant). Its payload decodes
    // to exactly "Effective Date,Entered Date,Transaction Description,Amount,Balance\r\n" and
    // nothing else. It stamped nothing and sat 21d stale inside a 35d window.
    //
    // Carrying the last known balance forward states a TRUE fact: no transactions means no
    // movement, so the balance has not changed. It is stamped at asOfDate, the export's own
    // SIGNED ts, so it never claims freshness the export did not actually have. It is also
    // harmless if the account turns out to be closed rather than dormant, since a closed account
    // holds $0.00 and that is exactly what the carried balance says.
    //
    // Genuine feed breakage is still detected: this only fires when an export ACTUALLY COVERED the
    // account and came back empty. If the export stops covering it, importBankCsv never runs for
    // that account, no row is written, and it goes stale correctly.
    const prior = await db`SELECT bank_balance, as_of_date FROM public.bank_reconciliation
      WHERE account_code = ${sourceAccount} ORDER BY as_of_date DESC LIMIT 1`
    if (prior.length) {
      const asOf = asOfDate || new Date().toISOString().slice(0, 10)
      carriedForwardFrom = String(prior[0].as_of_date).slice(0, 10)
      balances = await recordBalances(db, { [sourceAccount]: { curr_cents: Number(prior[0].bank_balance) } }, asOf, { carriedForwardFrom })
    }
  }
  return { account: sourceAccount, stats, declineEvents, liveBalance, liveBalanceDate, balances, totalStaged: rows.length, supersededScrapeRows, carriedForwardFrom }
}

// The read's own date (signed `ts`, Brisbane-agnostic - a date is a date) is the as-of for the
// balances; falls back to today for a ts-less read.
function readDate(msg) {
  const t = msg && msg.ts ? Date.parse(msg.ts) : NaN
  return Number.isNaN(t) ? new Date().toISOString().slice(0, 10) : new Date(t).toISOString().slice(0, 10)
}

// Backfill: re-run the normaliser over already-consumed, signature-verified inbox rows for a
// given requestId. Idempotent (the count-topup dedups + NOT-EXISTS balance guard), so
// re-running is a no-op.
async function backfillStaged(db, requestId) {
  const rows = await db`SELECT id, payload FROM public.vault_inbox
    WHERE payload->>'requestId' = ${requestId} AND sig_verified = true ORDER BY created_at`
  const out = []
  for (const row of rows) {
    const msg = row.payload || {}
    if (!msg.value || !looksLikeBaBody(msg.value)) { out.push({ id: row.id, skipped: 'no-ba-body' }); continue }
    out.push({ id: row.id, staged: await normaliseBankValueToStaged(db, msg.value, readDate(msg)) })
  }
  return out
}

async function pull(db) {
  const rows = await db`SELECT id, type, payload FROM public.vault_inbox WHERE consumed_at IS NULL ORDER BY created_at`
  const out = []
  // App Attest is a PRECONDITION for trusting any signed result: the paired key must be proven
  // genuine Secure-Enclave hardware. Computed once per pull. (enroll + attest rows are exempt -
  // attest is how a key BECOMES trusted; enroll is ciphertext the host cannot use anyway.)
  if (rows.length) await healPairingIfClobbered(db)   // restore a re-pair-clobbered signing key before verifying
  const attested = await pairedKeyIsAttested(db)
  for (const row of rows) {
    const msg = row.payload || {}
    // Enroll rows carry a sealed credential blob (ciphertext only the phone can open); they
    // are not signed. Store the blob via the enroll store, then consume.
    if (row.type === 'enroll') {
      let r = { id: row.id, type: 'enroll' }
      try {
        const { store } = require('./enroll.js')
        r.stored = store(JSON.stringify({ ...msg, type: 'enroll' }))
        r.action = 'enrolled'
      } catch (e) { r.action = 'enroll-failed'; r.error = e.message }
      await db`UPDATE public.vault_inbox SET consumed_at = now(), note = ${r.action} WHERE id = ${row.id}`
      out.push(r); continue
    }
    // App Attest rows carry their own Apple attestation (not our result signature); verify
    // the attestation and record the attested key, then consume.
    if (row.type === 'attest') {
      let r = { id: row.id, type: 'attest' }
      try {
        const { verifyAttestation } = require('./vault-attest.js')
        r.attest = await verifyAttestation(db, { keyId: msg.keyId, attestation: msg.attestation, challenge: msg.challenge, bindPubX963: msg.bindPubX963 })
        r.action = 'attested'
      } catch (e) { r.action = 'attestation-rejected'; r.error = e.message }
      await db`UPDATE public.vault_inbox SET consumed_at = now(), sig_verified = ${r.action === 'attested'}, note = ${r.action} WHERE id = ${row.id}`
      out.push(r); continue
    }
    const sigOk = verify(msg)
    const fresh = isFresh(msg)
    const replay = sigOk ? await sigAlreadySeen(db, msg.sig, row.id) : false
    // trusted only if: valid signature AND attested hardware AND fresh AND not a replay
    const ok = sigOk && attested && fresh && !replay
    let result = { id: row.id, type: row.type, sigVerified: sigOk, attested }
    if (!sigOk) {
      result.action = 'rejected (bad or missing signature)'
    } else if (!attested) {
      result.action = 'rejected (paired key not App-Attested genuine hardware)'
    } else if (!fresh) {
      result.action = 'rejected (stale - signed ts too old or future-dated)'
    } else if (replay) {
      result.action = 'rejected (replay - this signature was already consumed)'
    } else if (row.type === 'result' && msg.kind === 'bank-csv') {
      // Phase 2: the phone captured Bank Australia's OWN full-period CSV export (WKDownload) and
      // POSTed it base64 in `value`, tagged with the source_account. Decode, import through the
      // same parser + shared count-topup as import-ba-csv.cjs. Idempotent against the Phase 1
      // phone-scrape rows already staged (identical dedup key), so no double-count.
      try {
        const att = resolveBankCsvAccount(msg)
        result.attributedVia = att.via
        if (!att.account) {
          // Cannot confidently name the account. NEVER guess - leave the CSV captured-but-unimported
          // (its base64 value + pageContext stay on the vault_inbox row for triage / manual re-import),
          // and the daily bank-feed-staleness canary will alarm if an account's feed then goes stale.
          result.action = 'bank-csv-unattributed'
        } else {
          const csvText = Buffer.from(String(msg.value || ''), 'base64').toString('utf8')
          result.staged = await importBankCsv(db, csvText, att.account, readDate(msg))
          result.attributedAccount = att.account
          result.action = 'bank-csv-imported'
        }
      } catch (e) { result.stagedError = e.message; result.action = 'bank-csv-error' }
      // BUILD 29: the phone also captured the POST-Search page's nav-DOM (`navDom`: BA's account
      // picker + export button + selected account), bound by navDomSha256. Merge it into the
      // requestId's recon file so the next build can author the autonomous nav + confirm the picker->
      // account mapping. Runs regardless of the import outcome (an UNATTRIBUTED CSV still yields a
      // useful DOM map, and the picker is exactly what disambiguates attribution). Hash-verified;
      // skipped on mismatch. Never lets a recon failure disturb the already-recorded import result.
      try {
        if (msg.navDom != null && String(msg.navDom) !== '') {
          const nav = String(msg.navDom)
          if (msg.navDomSha256 && crypto.createHash('sha256').update(nav).digest('hex') !== msg.navDomSha256) {
            result.navDom = 'hash-mismatch-skipped'
          } else {
            const m = mergeReconPages(msg.requestId, JSON.parse(nav))   // navDom is ONE page object, RAW JSON
            result.navDomReconFile = m.file
            result.navDomReconPages = m.pages
          }
        }
      } catch (e) { result.navDom = 'navdom-merge-error: ' + e.message }
    } else if (row.type === 'result' && msg.kind === 'bank-dom-recon') {
      // RECON (build 28): the phone mapped the nav/interactive elements of each post-login BA page
      // (no transaction rows) so the NEXT build can drive hamburger -> accounts -> history -> export
      // autonomously. MERGE (not overwrite) into the requestId's recon file so a flush that lands
      // AFTER the bank-csv navDom pages cannot erase them - mergeReconPages keeps the richest per URL.
      try {
        // recon `value` is RAW JSON (the phone does not base64 it, unlike the CSV path): a JSON array
        // of page maps. Parse tolerantly; a single object is wrapped.
        let pages = []
        try { const p = JSON.parse(String(msg.value || '[]')); pages = Array.isArray(p) ? p : [p] } catch (_e) { pages = [] }
        const m = mergeReconPages(msg.requestId, pages)
        result.reconFile = m.file
        result.reconPages = m.pages
        result.action = 'bank-dom-recon-stored'
      } catch (e) { result.action = 'bank-dom-recon-error: ' + e.message }
    } else if (row.type === 'result' && isBankResult(msg)) {
      // Structured transactions -> JSON ledger (unchanged). Flattened body `value` -> the
      // Phase 1 staged_transactions bridge. A read can carry either or both.
      if (Array.isArray(msg.transactions) && msg.transactions.length) result.ledger = applyBankResult(msg)
      if (msg.value != null && looksLikeBaBody(msg.value)) {
        try { result.staged = await normaliseBankValueToStaged(db, msg.value, readDate(msg)) }
        catch (e) { result.stagedError = e.message }
      }
      result.action = result.ledger ? 'applied-to-ledger' : (result.staged ? 'normalised-to-staged' : 'bank-result-no-op')
    } else if (row.type === 'session') {
      // SESSION TRANSFER: the phone logged into a Mac-SSO origin (tate@ Google),
      // captured its WKHTTPCookieStore cookies, sealed them to the host recipient
      // pubkey, signed, and POSTed. Only reached when the full trust gate passed
      // (sig + attest + fresh + !replay) - a transferred session is at least as
      // sensitive as a bank result. Open the sealed bundle + inject into canonical
      // Chrome so the Mac is logged in. Host gets a REVOCABLE session, never the pw.
      try {
        const { applySession } = require('./vault-session.js')
        result.session = await applySession(msg)
        result.action = 'session-injected'
      } catch (e) { result.action = 'session-inject-failed: ' + e.message }
    } else {
      result.action = 'stored'
    }
    // If this verified result answers a pending approval, mark it approved (arms the WAKE
    // that re-opens the conductor with the task continuation - so an approval that lands
    // hours later resumes the task instead of it dying while I was asleep).
    //
    // requestId carries TWO unrelated meanings and only one of them is an approval id.
    // bank-feed-refresh-prompt.cjs sets it to a human trace tag ('ba-cron-20260831') purely
    // so a landed capture is traceable to that cron in these logs and in the recon filename.
    // vault_approvals.id is a uuid column, so passing a tag makes the lookup throw
    // '22P02 invalid input syntax for type uuid' on EVERY result of every bank-feed run
    // (measured 2026-08-31: 8 of 8 results on the ba-cron-20260831 pull). Harmless to the
    // import, but it buried a real error class under a permanent one. Only ids that could
    // BE an approval reach approve(); a trace tag records why it was skipped instead.
    if (ok && msg.requestId) {
      if (!isApprovalRequestId(msg.requestId)) {
        result.approvalSkipped = 'requestId is a trace tag, not a uuid approval id'
      } else {
        try {
          const { approve } = require('./vault-approval.js')
          result.approval = await approve(db, msg.requestId, msg.approvedBy || 'tate')
        } catch (e) { result.approvalError = e.message }
      }
    }
    await db`UPDATE public.vault_inbox SET consumed_at = now(), sig_verified = ${ok}, note = ${result.action} WHERE id = ${row.id}`
    out.push(result)
  }
  return out
}

module.exports = {
  pull, verify, applyBankResult, pairedKeyIsAttested, isFresh, sigAlreadySeen,
  // Phase 1 bank-feed bridge
  parseBaBody, baDirectionSign, looksLikeBaBody, isBankResult, normaliseBankValueToStaged, recordBalances, backfillStaged, readDate, BA_ACCOUNT_MAP,
  // Phase 2 phone-delivered CSV import
  stageGroupedBaRows, parseCsv, parseBaCsvRows, importBankCsv, BA_CSV_ACCOUNTS, resolveBankCsvAccount,
  // Build 29 post-Search DOM recon merge (no-clobber, richest-per-URL) + navDom hidden-field attribution
  mergeReconPages, accountFromNavDom, isApprovalRequestId,
}

if (require.main === module) {
  const lib = require('/Users/ecodia/.code/ecodiaos/backend/continuity/lib.cjs')
  const db = lib.db()
  const [cmd, arg] = process.argv.slice(2)
  if (cmd === 'backfill') {
    if (!arg) { console.error('usage: node vault-pull.js backfill <requestId>'); process.exit(2) }
    backfillStaged(db, arg).then(out => { console.log(JSON.stringify({ backfill: arg, results: out }, null, 2)); process.exit(0) })
      .catch(e => { console.error('ERR', e.message); process.exit(1) })
  } else {
    pull(db).then(out => { console.log(JSON.stringify({ pulled: out.length, results: out }, null, 2)); process.exit(0) })
      .catch(e => { console.error('ERR', e.message); process.exit(1) })
  }
}
