// asc.js - App Store Connect API helpers.
//
// Collapses the JWT + REST loop duplicated across every asc-*.py script.
// Every script reads the same p8, builds the same ES256 token, hits the
// same base URL with the same auth header. This module exposes the auth
// header builder + a few high-value queries as one-shot agent tools.
//
// Canonical key IDs live in docs/secrets/apple-asc-keys.md. The default
// values below match the org-shared key (R8P6K38X47, issuer
// 4b45186b-49e4-4a25-8a63-afd28cf12d3f). Per-app keys can be passed in
// the params object to override.
//
// STUB STATUS: proposed/, not autoloaded. Move to tools/asc.js +
// restart the agent to activate. Tests live in tools/asc.test.js (TBD).

const fs = require('fs')
const os = require('os')
const path = require('path')
const https = require('https')

const KEY_ID = 'R8P6K38X47'
const ISSUER = '4b45186b-49e4-4a25-8a63-afd28cf12d3f'
const BASE = 'https://api.appstoreconnect.apple.com'

const P8_CANDIDATES = [
  path.join(os.homedir(), 'PRIVATE', 'ecodia-creds', 'apple', `AuthKey_${KEY_ID}.p8`),
  path.join(os.homedir(), '.appstoreconnect', 'private_keys', `AuthKey_${KEY_ID}.p8`),
]

function loadP8(keyId) {
  const candidates = keyId === KEY_ID ? P8_CANDIDATES : [
    path.join(os.homedir(), 'PRIVATE', 'ecodia-creds', 'apple', `AuthKey_${keyId}.p8`),
    path.join(os.homedir(), '.appstoreconnect', 'private_keys', `AuthKey_${keyId}.p8`),
  ]
  for (const p of candidates) {
    if (fs.existsSync(p)) return fs.readFileSync(p, 'utf8')
  }
  throw new Error(`asc: p8 key not found for ${keyId} (looked in ${candidates.join(', ')})`)
}

// ES256 JWT for ASC. 20-minute window per Apple's hard cap.
function mintToken({ keyId = KEY_ID, issuer = ISSUER } = {}) {
  const jwt = require('jsonwebtoken') // already in package.json (used by other tools)
  const p8 = loadP8(keyId)
  const now = Math.floor(Date.now() / 1000)
  return jwt.sign(
    { iss: issuer, iat: now, exp: now + 20 * 60, aud: 'appstoreconnect-v1' },
    p8,
    { algorithm: 'ES256', header: { alg: 'ES256', kid: keyId, typ: 'JWT' } }
  )
}

function request({ method = 'GET', endpoint, token, body = null }) {
  return new Promise((resolve, reject) => {
    const url = new URL(endpoint.startsWith('http') ? endpoint : BASE + endpoint)
    const req = https.request({
      method,
      hostname: url.hostname,
      path: url.pathname + url.search,
      headers: {
        Authorization: `Bearer ${token}`,
        'Content-Type': 'application/json',
        Accept: 'application/json',
      },
    }, (res) => {
      let data = ''
      res.on('data', (c) => (data += c))
      res.on('end', () => {
        const parsed = data ? (() => { try { return JSON.parse(data) } catch (_) { return data } })() : null
        if (res.statusCode >= 400) return reject(new Error(`asc ${method} ${endpoint} -> ${res.statusCode}: ${JSON.stringify(parsed).slice(0, 400)}`))
        resolve({ status: res.statusCode, body: parsed })
      })
    })
    req.on('error', reject)
    if (body) req.write(JSON.stringify(body))
    req.end()
  })
}

// Public tool: asc.build_list. Given a bundle id, return the last N builds
// across all platforms with their processingState + uploadedDate. Replaces
// the 30-line "look up app -> list builds -> filter -> print" snippet that
// appears in 6+ scripts.
exports.build_list = async function ({ bundleId, limit = 10, keyId, issuer } = {}) {
  if (!bundleId) throw new Error('asc.build_list: bundleId required')
  const token = mintToken({ keyId, issuer })
  const appRes = await request({
    endpoint: `/v1/apps?filter[bundleId]=${encodeURIComponent(bundleId)}&limit=1`,
    token,
  })
  const app = appRes.body && appRes.body.data && appRes.body.data[0]
  if (!app) return { app: null, builds: [], note: `no app for bundle ${bundleId}` }
  const buildsRes = await request({
    endpoint: `/v1/builds?filter[app]=${app.id}&sort=-uploadedDate&limit=${limit}`,
    token,
  })
  const builds = (buildsRes.body.data || []).map((b) => ({
    id: b.id,
    version: b.attributes.version,
    uploadedDate: b.attributes.uploadedDate,
    processingState: b.attributes.processingState,
    expired: b.attributes.expired,
    minOsVersion: b.attributes.minOsVersion,
  }))
  return { app: { id: app.id, bundleId: app.attributes.bundleId, name: app.attributes.name }, builds }
}

// Public tool: asc.token. Mint a token + return it (caller does raw REST).
// Useful for one-off probes that don't justify a dedicated helper.
exports.token = async function ({ keyId, issuer } = {}) {
  return { token: mintToken({ keyId, issuer }), expires_in: 20 * 60 }
}

// Public tool: asc.app_versions. Given a bundle id, return all
// appStoreVersions with state + version string + release type. Replaces the
// "is there a READY_FOR_SALE version" check in every release-side script.
exports.app_versions = async function ({ bundleId, keyId, issuer } = {}) {
  if (!bundleId) throw new Error('asc.app_versions: bundleId required')
  const token = mintToken({ keyId, issuer })
  const appRes = await request({
    endpoint: `/v1/apps?filter[bundleId]=${encodeURIComponent(bundleId)}&limit=1`,
    token,
  })
  const app = appRes.body && appRes.body.data && appRes.body.data[0]
  if (!app) return { app: null, versions: [], note: `no app for bundle ${bundleId}` }
  const vRes = await request({
    endpoint: `/v1/apps/${app.id}/appStoreVersions?limit=20`,
    token,
  })
  const versions = (vRes.body.data || []).map((v) => ({
    id: v.id,
    versionString: v.attributes.versionString,
    appStoreState: v.attributes.appStoreState,
    releaseType: v.attributes.releaseType,
    platform: v.attributes.platform,
    createdDate: v.attributes.createdDate,
  }))
  return { app: { id: app.id, bundleId: app.attributes.bundleId, name: app.attributes.name }, versions }
}
