'use strict'
// capture-codes-4-4.js - regenerate + seal recovery codes for a vendor whose codes are
// shaped XXXX-XXXX (four-four). The vault's capture-backup-codes-from-cdp.js EXTRACT
// matches 5-5, 4-4-4 and bare-10 only, so Sentry's 4-4 codes fall through every branch
// and it reports "no recovery codes found in the DOM" on a page that is visibly full of
// them. That silent miss is the dangerous half: it reads as "this vendor has no codes".
//
// Regenerate first, on purpose: the previously displayed set reached a transcript, and a
// recovery code is a durable secret (valid until used), so the exposed set must be
// invalidated rather than sealed. We seal only the fresh set, daemon-side, and print
// nothing but a count and a set fingerprint.
//
// Usage: node capture-codes-4-4.js <service> <urlSubstr> [--regenerate]
const path = require('path')
const http = require('http')
const crypto = require('crypto')
const V = '/Users/ecodia/.code/eos-laptop-agent/tools/vault'
const WebSocket = require('/Users/ecodia/.code/ecodiaos/backend/node_modules/ws')
const { createKeystore, secureEnclaveBackend } = require(V + '/keystore')
const { createSeedStore } = require(V + '/seed-store')
const { resolveService } = require(V + '/registry')

const SERVICE = process.argv[2] || 'sentry'
const URLSUB = process.argv[3] || 'security/mfa/'
const REGEN = process.argv.includes('--regenerate')
const VAULT_DB = path.join(require('os').homedir(), 'PRIVATE', 'ecodia-creds', 'vault', 'vault.db')

const httpJson = (p) => new Promise((res, rej) => {
  http.get({ host: '127.0.0.1', port: 9222, path: p }, r => {
    let b = ''; r.on('data', d => b += d); r.on('end', () => { try { res(JSON.parse(b)) } catch (e) { rej(e) } })
  }).on('error', rej)
})

// 4-4 plus the shapes the vault tool already knows, so this is a superset.
const EXTRACT = `(() => {
  // innerText, NOT textContent: Sentry renders each code in its own element, so textContent
  // concatenates them ("...IPW3OYI7-ZACN...") and every \\b-anchored pattern misses. innerText
  // preserves the line breaks that make the boundaries real.
  const tc = (document.body && document.body.innerText) || '';
  const set = new Set();
  for (const m of tc.matchAll(/\\b([a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4})\\b/gi)) set.add(m[1].toUpperCase());
  if (set.size >= 5) return [...set];
  for (const m of tc.matchAll(/\\b([a-z0-9]{5}-[a-z0-9]{5})\\b/gi)) set.add(m[1].toUpperCase());
  if (set.size >= 5) return [...set];
  // No must-mix-letters-and-digits guard on this branch. That guard exists to stop an
  // ordinary WORD being mistaken for a bare code; a hyphenated 4-4 run is already
  // discriminating, and requiring the mix silently DROPPED 2 of Sentry's 10 codes
  // (the all-letter ones) on the first pass. A partially captured recovery set is the
  // worst outcome here: it reports success and leaves you short at rescue time.
  for (const m of tc.matchAll(/\\b([a-z0-9]{4}-[a-z0-9]{4})\\b/gi)) set.add(m[1].toUpperCase());
  return [...set];
})()`

const CLICK_REGEN = `(() => {
  const b = [...document.querySelectorAll('button,a')].find(e => /regenerate codes/i.test(e.innerText||''));
  if (!b) return 'no-regen-button';
  b.click();
  return 'clicked';
})()`
const CONFIRM_REGEN = `(() => {
  const b = [...document.querySelectorAll('button')].find(e => /^(confirm|regenerate|yes|continue)/i.test((e.innerText||'').trim()));
  if (!b) return 'no-confirm';
  b.click();
  return 'confirmed:' + (b.innerText||'').trim().slice(0,20);
})()`

async function main() {
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
  const ev = async (expr) => (await send('Runtime.evaluate', { expression: expr, returnByValue: true })).result.value
  const sleep = (ms) => new Promise(r => setTimeout(r, ms))
  try {
    const before = await ev(EXTRACT)
    const beforeFp = crypto.createHash('sha256').update((before || []).slice().sort().join('|')).digest('hex').slice(0, 8)
    let regen = 'skipped'
    if (REGEN) {
      regen = await ev(CLICK_REGEN); await sleep(1500)
      const c = await ev(CONFIRM_REGEN); await sleep(3000)
      regen = regen + '/' + c
    }
    const codes = await ev(EXTRACT)
    if (!codes || !codes.length) throw new Error('no recovery codes found in the DOM (4-4 aware)')
    const afterFp = crypto.createHash('sha256').update(codes.slice().sort().join('|')).digest('hex').slice(0, 8)
    const resolved = resolveService(SERVICE, store.loadRegistry())
    if (!resolved.ok) throw new Error('service not enrolled: ' + resolved.reason)
    store.addBackupCodes(resolved.seed_id, codes)
    store.audit({ service: SERVICE, backend: 'backup_code', event: 'enroll', detail: `captured ${codes.length} recovery codes (4-4 shape), SE-sealed` })
    console.log(JSON.stringify({
      service: SERVICE, seed_id: resolved.seed_id, regen,
      codesCaptured: codes.length, beforeFingerprint: beforeFp, setFingerprint: afterFp,
      rotated: beforeFp !== afterFp, sealed: true,
    }))
  } finally { ws.close(); store.close() }
}
main().catch(e => { console.log(JSON.stringify({ error: e.message })); process.exit(1) })
