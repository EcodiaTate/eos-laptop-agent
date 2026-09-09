'use strict'
// enroll-svg-qr.js - daemon-side seed capture for vendors whose authenticator QR is an
// SVG data URI. The vault's enroll-from-cdp.js QR branch only matches data:image/(png|jpeg)
// and its three DOM-text branches need the secret as text, so CapGo (SVG QR, no manual
// setup key, no otpauth anywhere in the HTML) falls through all four strategies and reports
// "no otpauth URI or base32 setup key found in the DOM (and no decodable QR)".
//
// Fix: rasterise the ALREADY-LOADED <img> onto a canvas and run BarcodeDetector on the
// canvas. Measured on console.capgo.app 2026-08-29: createImageBitmap refuses an SVG blob
// ("The source image could not be decoded") and fetch() refuses the non-standard
// "data:image/svg+xml;utf-8," form ("Failed to fetch"), so drawImage on the live element
// is the only path that works. Upscale x4 with imageSmoothingEnabled=false or the detector
// misses the modules.
//
// The secret reaches ONLY this process: sealed into the Secure Enclave, never printed.
//
// Usage: node enroll-svg-qr.js <service> <tier> <urlSubstr>
const path = require('path')
const http = require('http')
const V = '/Users/ecodia/.code/eos-laptop-agent/tools/vault'
const WebSocket = require('/Users/ecodia/.code/ecodiaos/backend/node_modules/ws')
const { createKeystore, secureEnclaveBackend } = require(V + '/keystore')
const { createSeedStore } = require(V + '/seed-store')
const totp = require(V + '/totp')

const SERVICE = process.argv[2] || 'capgo'
const TIER = process.argv[3] || 'OPEN'
const URLSUB = process.argv[4] || 'manage-2fa'
const VAULT_DB = path.join(require('os').homedir(), 'PRIVATE', 'ecodia-creds', 'vault', 'vault.db')

const httpJson = (p) => new Promise((res, rej) => {
  http.get({ host: '127.0.0.1', port: 9222, path: p }, r => {
    let b = ''; r.on('data', d => b += d); r.on('end', () => { try { res(JSON.parse(b)) } catch (e) { rej(e) } })
  }).on('error', rej)
})

async function connect() {
  const list = await httpJson('/json/list')
  const page = list.find(t => t.type === 'page' && (t.url || '').includes(URLSUB) && t.webSocketDebuggerUrl)
  if (!page) throw new Error('no CDP page matching ' + URLSUB)
  const ws = new WebSocket(page.webSocketDebuggerUrl, { perMessageDeflate: false })
  let id = 0; const pending = {}
  const send = (method, params) => new Promise((res, rej) => { const mid = ++id; pending[mid] = { res, rej }; ws.send(JSON.stringify({ id: mid, method, params })) })
  ws.on('message', m => { const d = JSON.parse(m); if (d.id && pending[d.id]) { d.error ? pending[d.id].rej(new Error(d.error.message)) : pending[d.id].res(d.result); delete pending[d.id] } })
  await new Promise((res, rej) => { ws.on('open', res); ws.on('error', rej) })
  await send('Runtime.enable', {})
  const evaluate = async (expr) => {
    const r = await send('Runtime.evaluate', { expression: expr, returnByValue: true, awaitPromise: true })
    if (r.exceptionDetails) throw new Error('eval: ' + (r.exceptionDetails.text || ''))
    return r.result.value
  }
  return { evaluate, close: () => ws.close(), url: page.url }
}

const EXTRACT_SVG_QR = `(async () => {
  try {
    if (!('BarcodeDetector' in window)) return { kind:'none', why:'no-BarcodeDetector' };
    const img = [...document.querySelectorAll('img')].find(i => /qr/i.test(i.alt||'')) ||
                [...document.querySelectorAll('img')].find(i => /^data:image\\//.test(i.src||''));
    if (!img) return { kind:'none', why:'no-qr-img' };
    if (!img.complete) await new Promise(r => { img.onload = r; setTimeout(r, 3000) });
    const S = 4;
    const w = img.naturalWidth || img.width || 256, h = img.naturalHeight || img.height || 256;
    const c = document.createElement('canvas'); c.width = w*S; c.height = h*S;
    const x = c.getContext('2d');
    x.fillStyle = '#fff'; x.fillRect(0,0,c.width,c.height);
    x.imageSmoothingEnabled = false; x.drawImage(img, 0, 0, c.width, c.height);
    const codes = await new BarcodeDetector({ formats:['qr_code'] }).detect(c);
    const v = codes.length ? (codes[0].rawValue||'') : '';
    if (/^otpauth:\\/\\//.test(v)) return { kind:'otpauth', value:v };
    return { kind:'none', why:'decoded-not-otpauth:' + (codes.length?'yes':'no-code') };
  } catch (e) { return { kind:'none', why:String(e).slice(0,120) } }
})()`

async function main() {
  const se = secureEnclaveBackend({})
  if (!se.provisioned) throw new Error('Secure Enclave key not provisioned')
  const ks = createKeystore({ backend: se })
  const store = createSeedStore({ keystore: ks, dbPath: VAULT_DB })
  const page = await connect()
  try {
    const got = await page.evaluate(EXTRACT_SVG_QR)
    if (got.kind !== 'otpauth') throw new Error('no otpauth from QR: ' + (got.why || 'unknown'))
    const fingerprint = require('crypto').createHash('sha256').update(got.value).digest('hex').slice(0, 8)
    const seed_id = store.enroll({
      service: SERVICE, tier: TIER, backend: 'totp',
      registered_origin: new URL(page.url).origin, enrolled_under_presence: true, otpauthUri: got.value,
    })
    const seed = store.loadSeed(seed_id)
    const code = totp.totp(seed.secret, { algorithm: seed.algorithm, digits: seed.digits, step: seed.period })
    store.audit({ service: SERVICE, tier: TIER, backend: 'totp', event: 'enroll', detail: 'daemon-side SVG-QR capture, SE-sealed' })
    console.log(JSON.stringify({ seed_id, seedKind: got.kind, seedFingerprint: fingerprint, codeIsSixDigits: /^\d{6}$/.test(code), db: VAULT_DB }))
  } finally { page.close(); store.close() }
}
main().catch(e => { console.log(JSON.stringify({ error: e.message })); process.exit(1) })
