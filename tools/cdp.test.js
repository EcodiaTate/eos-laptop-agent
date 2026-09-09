'use strict'

// cdp.test.js - regression suite for the resolveTarget ambiguity guard.
//
// The defect: urlContains / titleContains were `for (p of pages) { if (match) return p }`,
// so an ambiguous needle silently coin-flipped onto whichever tab puppeteer
// listed first. Fixed 2026-08-29 by collecting ALL matches and refusing when
// more than one survives the exact / path-canonical tiebreaks.
//
// THE CONTROL IS THE POINT. Test 5 reimplements the OLD loop verbatim and runs
// it against the SAME fixture the guard rejects, asserting it returns the first
// match silently. Without that, a vacuous guard (or a fixture whose two tabs
// were never actually ambiguous) would let the whole suite pass green. The
// control lives in the test file, never in prod, so it adds no bypass.
//
// Modelled on scripts/claude-hooks-backup/ecodia/test_cdp_tab_ledger.py, the
// sibling fix for tab OWNERSHIP attribution (commit ead8a8f).
//
// Run: node tools/cdp.test.js
// NOT autoloaded: SKIP_RE excludes *.test.js, and the require.main guard below
// means a require() of this file runs nothing (the 2026-06-22 outage was a test
// file detonating during autoload).

const assert = require('assert')
const cdp = require('./cdp.js')
const pickUnique = cdp._pickUnique
const canonicalUrl = cdp._canonicalUrl

let passed = 0
function t(name, fn) {
  try { fn(); passed++; console.log('  ok   ' + name) }
  catch (e) { console.error('  FAIL ' + name + '\n       ' + e.message); process.exitCode = 1 }
}

// Fixture mirroring the real 2026-08-29 census: five studio.ecodia.au tabs, all
// titled "Studio", belonging to five different live workers.
const STUDIO = [
  { targetId: 'T1', url: 'https://studio.ecodia.au/sites/aaa', title: 'Studio' },
  { targetId: 'T2', url: 'https://studio.ecodia.au/sites/bbb', title: 'Studio' },
]

console.log('cdp ambiguity guard')

t('1 unambiguous single match returns it (unchanged common case)', () => {
  const one = [{ targetId: 'T1', url: 'https://friend.ecodia.au/x', title: 'Friend' }]
  assert.strictEqual(pickUnique(one, 'friend.ecodia.au', 'url').targetId, 'T1')
})

t('2 TWO url matches THROW rather than returning either', () => {
  let threw = null
  try { pickUnique(STUDIO, 'studio.ecodia.au', 'url', 'studio.ecodia.au') }
  catch (e) { threw = e }
  assert.ok(threw, 'expected a throw, got a silently-picked tab')
  assert.strictEqual(threw.code, 'AMBIGUOUS_TARGET')
  assert.strictEqual(threw.candidates.length, 2)
  // the error must NAME every candidate so the caller can disambiguate
  assert.ok(threw.message.indexOf('T1') !== -1 && threw.message.indexOf('T2') !== -1,
    'error must name every candidate targetId')
  assert.ok(threw.message.indexOf('sites/aaa') !== -1 && threw.message.indexOf('sites/bbb') !== -1,
    'error must name every candidate url')
})

t('3 TWO title matches THROW (titles collide harder than urls)', () => {
  let threw = null
  try { pickUnique(STUDIO, 'studio', 'title', 'Studio') } catch (e) { threw = e }
  assert.ok(threw, 'expected a throw on 2 tabs titled Studio')
  assert.strictEqual(threw.code, 'AMBIGUOUS_TARGET')
})

t('4a exact url tiebreak resolves an otherwise-ambiguous set', () => {
  const m = [
    { targetId: 'T1', url: 'https://studio.ecodia.au/sites/aaa' },
    { targetId: 'T2', url: 'https://studio.ecodia.au/sites/aaa/settings' },
  ]
  assert.strictEqual(pickUnique(m, 'https://studio.ecodia.au/sites/aaa', 'url').targetId, 'T1')
})

t('4b path-canonical tiebreak ignores the tab query string', () => {
  const m = [
    { targetId: 'T1', url: 'https://studio.ecodia.au/sites/aaa?tab=pages#top' },
    { targetId: 'T2', url: 'https://studio.ecodia.au/sites/aaa/settings' },
  ]
  assert.strictEqual(pickUnique(m, 'https://studio.ecodia.au/sites/aaa', 'url').targetId, 'T1')
})

t('4c exact title tiebreak resolves substring collisions', () => {
  const m = [
    { targetId: 'T1', url: 'u1', title: 'Studio' },
    { targetId: 'T2', url: 'u2', title: 'Studio - Site Settings' },
  ]
  assert.strictEqual(pickUnique(m, 'studio', 'title').targetId, 'T1')
})

t('4d a tiebreak that still leaves TWO must refuse, not pick', () => {
  // both canonicalise to the same path: genuinely indistinguishable by url
  const m = [
    { targetId: 'T1', url: 'https://studio.ecodia.au/sites/aaa?a=1' },
    { targetId: 'T2', url: 'https://studio.ecodia.au/sites/aaa?a=2' },
  ]
  assert.throws(() => pickUnique(m, 'https://studio.ecodia.au/sites/aaa', 'url'),
    /AMBIGUOUS/, 'two tabs on one canonical path must refuse')
})

t('4e the unique-param convention still resolves (needle query NOT stripped)', () => {
  const m = [
    { targetId: 'T1', url: 'https://studio.ecodia.au/sites/aaa?eos-1e24=1' },
    { targetId: 'T2', url: 'https://studio.ecodia.au/sites/aaa?eos-1e24=2' },
  ]
  // the whole point of the fleet workaround: a unique param narrows to one MATCH,
  // so pickUnique is never even asked to disambiguate.
  const narrowed = m.filter(x => x.url.toLowerCase().indexOf('eos-1e24=2') !== -1)
  assert.strictEqual(narrowed.length, 1)
  assert.strictEqual(pickUnique(narrowed, 'eos-1e24=2', 'url').targetId, 'T2')
})

// ---- CONTROL -------------------------------------------------------------
// Verbatim shape of the PRE-FIX loop. If this does NOT silently return the
// first of the two STUDIO tabs, then the fixture is not actually ambiguous and
// tests 2 and 3 above are vacuous.
function oldResolve_SILENT_FIRST_MATCH(pages, needle) {
  for (const p of pages) {
    if (String(p.url).toLowerCase().indexOf(needle) !== -1) return p
  }
  return null
}

t('5 CONTROL: the OLD loop silently returns the first match on the SAME fixture', () => {
  const got = oldResolve_SILENT_FIRST_MATCH(STUDIO, 'studio.ecodia.au')
  assert.ok(got, 'control must match something or the fixture is wrong')
  assert.strictEqual(got.targetId, 'T1',
    'old code must silently pick T1 - this is the bug the guard replaces')
  // and the guard must disagree with the control on that same input
  assert.throws(() => pickUnique(STUDIO, 'studio.ecodia.au', 'url'), /AMBIGUOUS/,
    'guard must REFUSE exactly where the old code silently picked')
})

t('6 canonicalUrl strips query and fragment, whichever comes first', () => {
  assert.strictEqual(canonicalUrl('https://a.b/c?x=1'), 'https://a.b/c')
  assert.strictEqual(canonicalUrl('https://a.b/c#f'), 'https://a.b/c')
  assert.strictEqual(canonicalUrl('https://a.b/c#f?x=1'), 'https://a.b/c')
  assert.strictEqual(canonicalUrl('https://a.b/c'), 'https://a.b/c')
})

t('7 zero matches is the callers job, pickUnique never invents one', () => {
  assert.throws(() => pickUnique([], 'nothing', 'url'), /AMBIGUOUS/)
})

if (require.main === module) {
  console.log('\n' + passed + ' passed' + (process.exitCode ? ', SOME FAILED' : ', 0 failed'))
}
