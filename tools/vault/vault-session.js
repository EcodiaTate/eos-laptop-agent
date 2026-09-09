// vault-session.js - HOST side of SESSION TRANSFER (tate@ Google SSO -> Mac Chrome).
//
// WHY this exists: some vault jobs are PHONE-AS-WORKER (the phone logs in, reads a
// value, returns it - e.g. a bank balance). But logging into tate@ Google is
// MAC-NEEDS-A-SESSION: the whole point is that the *Mac's* canonical Chrome ends up
// logged into Google so every Google-SSO site the Mac drives is authenticated. A
// phone that logs itself in is useless for that. So the phone becomes a session
// COURIER: it authenticates (password + TOTP that only it can unseal), captures its
// WKHTTPCookieStore cookies for the login origin (WKHTTPCookieStore, unlike
// document.cookie, can read the HttpOnly session cookies - __Secure-1PSID, SSID,
// HSID, OSID - that actually carry the Google session), SEALS the cookie bundle to
// this host's recipient public key (ECIES, login origin bound as AAD), signs the
// message with its Secure-Enclave key, and POSTs type='session'.
//
// Here we: verify the phone signature + App-Attest + freshness + no-replay (the SAME
// gate as a bank result - a session is at least as sensitive), OPEN the sealed bundle
// with the recipient PRIVATE key (only the host holds it; the phone can seal but never
// open), and inject each cookie into the canonical Chrome so the Mac is logged in.
//
// SECURITY POSTURE: the host receives a REVOCABLE SESSION, never the password. The
// password + TOTP seed live only on the phone, sealed to the phone's own key. Tate's
// "sign out everywhere" on the Google account revokes every transferred cookie. The
// origin is bound as AAD so a bundle captured for accounts.google.com cannot be
// replayed against a different origin.
//
// Doctrine: patterns/handle-a-secret-without-ever-seeing-it-2026-06-19.md,
//           patterns/cdp-cannot-borrow-real-chrome-logged-in-session-2026-06-15.md.

const fs = require('fs')
const path = require('path')
const os = require('os')
const crypto = require('crypto')
const { openWith } = require('./envelope.js')

const RECIP_FILE = path.join(os.homedir(), 'PRIVATE/ecodia-creds/vault/session-recipient.json')

// Persist a recipient keypair with the PRIVATE key as base64 - NOT the ECDH object.
// envelope.newRecipient() returns { ecdh, publicX963B64, ... } where ecdh is a
// crypto ECDH instance; JSON.stringify serialises it to {} and the private key is
// LOST silently. Anything the phone then seals to the pubkey can never be opened.
// This function is the fix: it stores getPrivateKey() so loadRecipient() can rebuild.
function genRecipient() {
  const ecdh = crypto.createECDH('prime256v1')
  ecdh.generateKeys()
  const pub = ecdh.getPublicKey() // 65-byte x963 uncompressed point
  const rec = {
    privB64: ecdh.getPrivateKey().toString('base64'),
    publicX963B64: pub.toString('base64'),
    publicRawB64: pub.subarray(1).toString('base64'),
  }
  fs.mkdirSync(path.dirname(RECIP_FILE), { recursive: true, mode: 0o700 })
  fs.writeFileSync(RECIP_FILE, JSON.stringify(rec, null, 2), { mode: 0o600 })
  return { publicX963B64: rec.publicX963B64, publicRawB64: rec.publicRawB64 }
}

// Rebuild the recipient's ECDH (with the private key) for openWith().
function loadRecipient() {
  const rec = JSON.parse(fs.readFileSync(RECIP_FILE, 'utf8'))
  if (!rec.privB64) {
    throw new Error('recipient private key missing - run genRecipient() (the file was written from an unserialisable ecdh object; the pubkey must be re-delivered to the phone after regen)')
  }
  const ecdh = crypto.createECDH('prime256v1')
  ecdh.setPrivateKey(Buffer.from(rec.privB64, 'base64'))
  return { ecdh, publicX963B64: rec.publicX963B64, publicRawB64: rec.publicRawB64 }
}

// Return just the public key encodings, for delivery to the phone (eosvault://config
// &recip=). Never exposes the private key.
function recipientPublic() {
  const { publicX963B64, publicRawB64 } = loadRecipient()
  return { publicX963B64, publicRawB64 }
}

// Open the sealed cookie bundle. msg.value is the base64 ECIES blob; msg.origin is
// the AAD - openWith THROWS if it does not match what the phone bound at seal time.
// Returns the captured cookie array.
function openSession(msg) {
  if (!msg || !msg.value) throw new Error('session message has no sealed value')
  const { ecdh } = loadRecipient()
  const plain = openWith(ecdh, msg.value, msg.origin || null)
  const cookies = JSON.parse(plain)
  if (!Array.isArray(cookies)) throw new Error('session payload is not a cookie array')
  return cookies
}

// Map one captured WKHTTPCookieStore cookie to the cdp.setCookie opts shape.
// httpOnly/secure/sameSite/expires/domain/path all carry through so the injected
// cookie is byte-identical in flags to what Chrome would have set itself. Without
// httpOnly on __Secure-1PSID/SSID/HSID the Google session is not recognised.
function toSetCookieOpts(c, target) {
  const o = Object.assign({ name: c.name, value: c.value }, target)
  if (c.domain) o.domain = c.domain
  if (c.path) o.path = c.path
  if (typeof c.expires === 'number' && c.expires > 0) o.expires = c.expires
  if (c.httpOnly) o.httpOnly = true
  if (c.secure) o.secure = true
  if (c.sameSite) o.sameSite = c.sameSite // 'Strict' | 'Lax' | 'None'
  return o
}

// Inject the cookies into the canonical Chrome (~/chrome-canonical:9222) and verify.
// opts.cdp lets tests pass a fake; production requires ../cdp.js.
async function injectToChrome(cookies, opts = {}) {
  const cdp = opts.cdp || require('../cdp.js')
  const urlContains = opts.urlContains || 'google.com'
  const verifyUrl = opts.verifyUrl || 'https://myaccount.google.com/'
  let target = { urlContains }
  // Ensure a live page on the origin exists so the cookie writes have context and
  // so the reload afterwards actually re-authenticates.
  //
  // AMBIGUITY (2026-08-29): the default needle 'google.com' matches EVERY google
  // tab, and several are normally open at once in the fleet-shared canonical
  // Chrome (mail.google.com + calendar.google.com were both live when this was
  // written). resolveTarget now REFUSES a non-unique needle rather than silently
  // coin-flipping, so this call site has to say which tab it wants. Here, and
  // only here, ANY match is equally correct: CDP cookie writes go to the browser
  // context, not to a tab, so the tab is just somewhere to stand. Pin the first
  // candidate by explicit targetId. That structured catch-and-pin is the
  // sanctioned two-step; it is deliberately not a general allowAmbiguous flag.
  const _pin = (e) => {
    if (e && e.code === 'AMBIGUOUS_TARGET' && e.candidates && e.candidates.length) {
      target = { targetId: e.candidates[0].targetId }
      return true
    }
    return false
  }
  try { await cdp.navigate(Object.assign({}, target, { url: verifyUrl })) }
  catch (e) {
    if (_pin(e)) { try { await cdp.navigate(Object.assign({}, target, { url: verifyUrl })) } catch (_e2) {} }
  }
  const set = []
  const failed = []
  for (const c of cookies) {
    try { await cdp.setCookie(toSetCookieOpts(c, target)); set.push(c.name) }
    catch (e) { failed.push({ name: c.name, error: e.message }) }
  }
  // Reload and read back a login signal. A logged-in myaccount page exposes the
  // signed-in email in the account switcher; absence of the sign-in form is the
  // cheap discriminating probe. Full proof is the E2E device test with Tate.
  let verified = null
  try {
    await cdp.navigate(Object.assign({}, target, { url: verifyUrl }))
    const r = await cdp.runJs(Object.assign({}, target, {
      js: "(function(){var b=document.body?document.body.innerText:'';return {signIn:/Sign in|Use your Google Account/i.test(b),hasAccount:/Manage your.*Account|Personal info/i.test(b)};})()",
    }))
    verified = r && (r.result || r)
  } catch (e) { verified = { error: e.message } }
  return { set, failed, count: set.length, verified }
}

// Full host path for a type='session' inbox row: caller has already run the trust
// gate (sig + attest + fresh + !replay). Open + inject.
async function applySession(msg, opts = {}) {
  const cookies = openSession(msg)
  const inject = await injectToChrome(cookies, opts)
  return { cookies: cookies.length, inject }
}

module.exports = {
  RECIP_FILE, genRecipient, loadRecipient, recipientPublic,
  openSession, toSetCookieOpts, injectToChrome, applySession,
}
