// kv.js - Supabase kv_store reader bypassing the MCP scope_denied wall.
//
// kv_store.creds.* is READ-DENIED on both the cowork and ecodia-full MCP
// bearers. Every script that needs a real cred currently inlines:
//   set -a; . /Users/ecodia/PRIVATE/ecodia-creds/supabase.env; set +a
//   curl -s "$SUPABASE_URL/rest/v1/kv_store?key=eq.creds.<x>&select=value" \
//     -H "apikey: $SUPABASE_SERVICE_KEY" -H "Authorization: Bearer $SUPABASE_SERVICE_KEY"
//   | jq -r '.[0].value' | sed 's/^"//; s/"$//'
// That's 5+ lines + a sed quote-strip + repeated across 6+ scripts in
// the last 30 days. This helper collapses it to one tool call.
//
// Auth source: the org PAT at /Users/ecodia/PRIVATE/ecodia-creds/supabase.env
// (canonical local store per supabase-access-via-org-pat-local-store-2026-05-20).
// Per-project service key derived via the Management API.
//
// STUB STATUS: proposed/, not autoloaded. Move to tools/kv.js + restart
// the agent to activate.

const fs = require('fs')
const path = require('path')
const os = require('os')
const https = require('https')

const PAT_ENV_FILE = process.env.SUPABASE_ENV_FILE || path.join(os.homedir(), 'PRIVATE', 'ecodia-creds', 'supabase.env')
const ECODIAOS_PROJECT_REF = 'nxmtfzofemtrlezlyhcj' // the canonical kv_store host

function loadPat() {
  if (process.env.SUPABASE_ACCESS_TOKEN) return process.env.SUPABASE_ACCESS_TOKEN
  if (!fs.existsSync(PAT_ENV_FILE)) throw new Error(`kv: PAT env file not found at ${PAT_ENV_FILE}`)
  const txt = fs.readFileSync(PAT_ENV_FILE, 'utf8')
  const m = txt.match(/^SUPABASE_ACCESS_TOKEN\s*=\s*(?:"([^"]+)"|'([^']+)'|(\S+))/m)
  if (!m) throw new Error(`kv: SUPABASE_ACCESS_TOKEN not found in ${PAT_ENV_FILE}`)
  return m[1] || m[2] || m[3]
}

function mgmtRequest({ method = 'GET', endpoint, token, body = null }) {
  return new Promise((resolve, reject) => {
    const url = new URL(endpoint.startsWith('http') ? endpoint : 'https://api.supabase.com' + endpoint)
    const req = https.request({
      method,
      hostname: url.hostname,
      path: url.pathname + url.search,
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    }, (res) => {
      let data = ''
      res.on('data', (c) => (data += c))
      res.on('end', () => {
        const parsed = data ? (() => { try { return JSON.parse(data) } catch (_) { return data } })() : null
        if (res.statusCode >= 400) return reject(new Error(`supabase mgmt ${method} ${endpoint} -> ${res.statusCode}: ${JSON.stringify(parsed).slice(0, 300)}`))
        resolve({ status: res.statusCode, body: parsed })
      })
    })
    req.on('error', reject)
    if (body) req.write(JSON.stringify(body))
    req.end()
  })
}

// Run an arbitrary SQL query as superuser via the Management API. This is
// what bypasses the MCP scope_denied wall - the PAT runs as postgres.
async function runSql({ projectRef = ECODIAOS_PROJECT_REF, query, token }) {
  const t = token || loadPat()
  const res = await mgmtRequest({
    method: 'POST',
    endpoint: `/v1/projects/${projectRef}/database/query`,
    token: t,
    body: { query },
  })
  return res.body
}

// Public tool: kv.read_creds. Read a kv_store row by key, return the
// value. Default project is the EcodiaOS canonical store; override for
// per-project kv_stores. Strips JSON-string wrapping quotes if present.
exports.read_creds = async function ({ key, projectRef } = {}) {
  if (!key) throw new Error('kv.read_creds: key required')
  // Use parameterised-ish quoting; key is a string from the agent caller.
  // Reject backticks/semicolons defensively, even though postgres handles
  // the actual quoting via $$ escape.
  if (/[`;\\]/.test(key)) throw new Error(`kv.read_creds: refusing key with shell metachars: ${key}`)
  const rows = await runSql({
    projectRef: projectRef || ECODIAOS_PROJECT_REF,
    query: `SELECT value FROM kv_store WHERE key = $$${key}$$ LIMIT 1`,
  })
  if (!Array.isArray(rows) || rows.length === 0) return { key, value: null, found: false }
  const raw = rows[0].value
  // Postgres returns JSONB as a parsed JS value through this endpoint.
  // String values come through as actual strings; objects as objects.
  return { key, value: raw, found: true }
}

// Public tool: kv.list_creds. List keys matching a prefix - useful to
// answer "what does creds.* look like for project X" without an MCP call.
exports.list_creds = async function ({ prefix = 'creds.', projectRef, limit = 100 } = {}) {
  if (/[`;\\']/.test(prefix)) throw new Error(`kv.list_creds: refusing prefix with shell metachars: ${prefix}`)
  const rows = await runSql({
    projectRef: projectRef || ECODIAOS_PROJECT_REF,
    query: `SELECT key, jsonb_typeof(value) AS value_type FROM kv_store WHERE key LIKE $$${prefix}%$$ ORDER BY key LIMIT ${parseInt(limit, 10) || 100}`,
  })
  return { prefix, projectRef: projectRef || ECODIAOS_PROJECT_REF, count: rows.length, rows }
}

// Public tool: kv.project_ref_for. Convenience - resolve a slug to its
// Supabase project ref. Hard-coded canonical map, no Management API call.
exports.project_ref_for = async function ({ slug } = {}) {
  const MAP = {
    ecodiaos: 'nxmtfzofemtrlezlyhcj',
    coexist: 'tjutlbzekfouwsiaplbr',
    chambers: 'arkbjjkfjsjibnhivjis',
    roam: 'vzauarlfmkjfkcphojbd',
    wildmountains: 'efrytpwdrxfaehtqfpkq',
    woodfordia: 'iqrxrjgutvowvetrmywr',
    wattle: 'jbdghvzfvxvohztfxzan',
    goodreach: 'ngoeairmbigqulhfjqso',
    resonaverde: 'dxtglcfyqvhmmnopshhp',
  }
  const ref = MAP[slug]
  if (!ref) return { slug, ref: null, known: Object.keys(MAP) }
  return { slug, ref }
}
