'use strict'
// capture-backup-codes-bare10-from-cdp.js - seal recovery codes for a vendor whose codes
// are BARE 10-character alphanumerics with no separator (Namecheap).
//
// Why a third capture tool exists. capture-backup-codes-from-cdp.js matches 5-5 / 4-4-4 /
// bare-10; capture-backup-codes-4-4-from-cdp.js matches 4-4-4 / 5-5 / 4-4 and, despite its
// own comment, carries NO bare-10 branch. Run against Namecheap's list page the 4-4 tool
// matched a single incidental hyphenated run elsewhere on the page and reported
// codesCaptured:1, sealed:true. That is the doctrine failure in a new shape: a capture that
// reports SUCCESS while sealing a rescue set of one. A partial set is worse than none,
// because nothing later tells you it is short.
//
// Two safeguards the other tools lack:
//   * It asserts a MINIMUM count (default 8). Fewer means the extractor missed, and it
//     refuses rather than sealing a short set.
//   * With --replace it REPLACES the stored set for this seed instead of appending, so a
//     previous bad capture cannot survive alongside a good one.
// The codes reach only this process: sealed into the Secure Enclave, never printed.
//
// Usage: node capture-backup-codes-bare10-from-cdp.js <service> <urlSubstr> [--min N] [--replace]
const path = require('path')
const http = require('http')
const crypto = require('crypto')
const V = '/Users/ecodia/.code/eos-laptop-agent/tools/vault'
const WebSocket = require('/Users/ecodia/.code/ecodiaos/backend/node_modules/ws')
const { createKeystore, secureEnclaveBackend } = require(V + '/keystore')
const { createSeedStore } = require(V + '/seed-store')
const { resolveService } = require(V + '/registry')

const SERVICE = process.argv[2]
const URLSUB = process.argv[3]
const MIN = (() => { const i = process.argv.indexOf('--min'); return i >= 0 ? Number(process.argv[i + 1]) : 8 })()
const MAX = (() => { const i = process.argv.indexOf('--max'); return i >= 0 ? Number(process.argv[i + 1]) : 12 })()
const REPLACE = process.argv.includes('--replace')
const VAULT_DB = path.join(require('os').homedir(), 'PRIVATE', 'ecodia-creds', 'vault', 'vault.db')

const httpJson = (p) => new Promise((res, rej) => {
  http.get({ host: '127.0.0.1', port: 9222, path: p }, r => {
    let b = ''; r.on('data', d => b += d); r.on('end', () => { try { res(JSON.parse(b)) } catch (e) { rej(e) } })
  }).on('error', rej)
})

// innerText, not textContent: the codes sit in sibling elements and textContent runs them
// together. No must-mix-letters-and-digits guard: an all-letter code is a real code, and
// that guard is exactly what silently dropped 2 of Sentry's 10. Noise is excluded instead by
// taking only runs of length EXACTLY 10, dropping all-digit runs (ids), and then requiring
// the total to clear MIN.
const CONTAINER = (() => { const i = process.argv.indexOf('--container'); return i >= 0 ? process.argv[i + 1] : '.backup-codes-area' })()

// Scope the scan to the container that holds ONLY the codes. Scanning document.body
// instead over-captures: on Namecheap a whole-body scan returned 26 runs where 10 are
// real, because nav labels and marketing copy contain 10-letter words. Over-capture is
// the mirror of the short-set failure and is just as untrustworthy, so the container is
// required and the tool falls back to body only when no container matches, where the MIN
// and MAX bounds then have to carry the check alone.
// innerText, not textContent: the codes sit in sibling elements and textContent runs them
// together. No must-mix-letters-and-digits guard: an all-letter code is a real code, and
// that guard is what silently dropped 2 of Sentry's 10.
const EXTRACT = `(() => {
  const el = document.querySelector(${JSON.stringify(CONTAINER)}) || document.body;
  const t = el.innerText || '';
  const seen = new Set();
  for (const m of t.matchAll(/\\b([A-Za-z0-9]{10})\\b/g)) {
    const c = m[1];
    if (/^[0-9]+$/.test(c)) continue;
    seen.add(c);
  }
  return { scoped: el !== document.body, selector: ${JSON.stringify(CONTAINER)}, codes: [...seen] };
})()`

async function main () {
  if (!SERVICE || !URLSUB) throw new Error('usage: <service> <urlSubstr> [--min N] [--replace]')
  const se = secureEnclaveBackend({})
  if (!se.provisioned) throw new Error('Secure Enclave key not provisioned')
  const store = createSeedStore({ keystore: createKeystore({ backend: se }), dbPath: VAULT_DB })
  const list = await httpJson('/json/list')
  const page = list.find(t => t.type === 'page' && (t.url || '').includes(URLSUB) && t.webSocketDebuggerUrl)
  if (!page) throw new Error('no CDP page matching ' + URLSUB)
  const ws = new WebSocket(page.webSocketDebuggerUrl, { perMessageDeflate: false })
  let id = 0; const pending = {}
  const send = (m, p) => new Promise((res, rej) => { const mid = ++id; pending[mid] = { res, rej }; ws.send(JSON.stringify({ id: mid, method: m, params: p })) })
  ws.on('message', m => { const d = JSON.parse(m); if (d.id && pending[d.id]) { d.error ? pending[d.id].rej(new Error(d.error.message)) : pending[d.id].res(d.result); delete pending[d.id] } })
  await new Promise((res, rej) => { ws.on('open', res); ws.on('error', rej) })
  await send('Runtime.enable', {})
  const ev = async (e) => (await send('Runtime.evaluate', { expression: e, returnByValue: true })).result.value
  try {
    const res = await ev(EXTRACT)
    const codes = (res && res.codes) || []
    if (!res || !res.scoped) console.error('WARN: container ' + CONTAINER + ' not found, fell back to body (over-capture risk)')
    if (codes.length < MIN || codes.length > MAX) {
      throw new Error('refusing to seal an implausible set: found ' + codes.length + ', want ' + MIN + '..' + MAX +
        ' (a partial set fails at rescue time; an over-captured set is page noise, not codes)')
    }
    const resolved = resolveService(SERVICE, store.loadRegistry())
    if (!resolved.ok) throw new Error('service not enrolled: ' + resolved.reason)
    let removed = 0
    if (REPLACE) {
      const before = store._db.prepare('SELECT COUNT(*) c FROM vault_backup_code WHERE seed_id=?').get(resolved.seed_id).c
      store._db.prepare('DELETE FROM vault_backup_code WHERE seed_id=?').run(resolved.seed_id)
      removed = before
    }
    store.addBackupCodes(resolved.seed_id, codes)
    const fp = crypto.createHash('sha256').update(codes.slice().sort().join('|')).digest('hex').slice(0, 8)
    store.audit({ service: SERVICE, backend: 'backup_code', event: 'enroll',
      detail: `captured ${codes.length} recovery codes (bare-10 shape), replaced ${removed}, SE-sealed` })
    const held = store._db.prepare('SELECT COUNT(*) c FROM vault_backup_code WHERE seed_id=?').get(resolved.seed_id).c
    console.log(JSON.stringify({ service: SERVICE, seed_id: resolved.seed_id, codesCaptured: codes.length, scoped: res.scoped,
      priorRowsRemoved: removed, rowsHeldNow: held, setFingerprint: fp, sealed: true }))
  } finally { ws.close(); store.close() }
}
main().catch(e => { console.log(JSON.stringify({ error: e.message })); process.exit(1) })
