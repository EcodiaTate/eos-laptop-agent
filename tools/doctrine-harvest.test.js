// doctrine-harvest.test.js - REAL-git proof that a worker's stranded doctrine
// reaches the surface knowledge.lookup actually reads.
//
// Stubbed git would prove nothing here: the mechanism reads a branch through git
// plumbing and then writes the corpus through the filesystem, so the test stands
// up a throwaway bare origin plus a shared tree, plants a worker branch carrying
// a dummy patterns/*.md exactly the way a dispatched worker leaves one behind,
// runs the harvest, and asserts the file is ON DISK in the shared tree.
//
// WHY DISK IS THE ASSERTION (revised 2026-08-27)
// The first version of this suite asserted the file reached origin/main, and it
// passed for four days while the mechanism was failing at its actual purpose.
// knowledge-index/indexer.js reads the corpus from disk with fs, never from a git
// ref, and nothing on any schedule brings origin/main back into the conductor
// tree. So "on origin/main" was never the property worth testing, and asserting
// it made a broken outcome look green. Test 1 now carries an explicit NEGATIVE
// control: the file must NOT be on origin/main, which is what stops this suite
// silently re-passing if the publish path is ever reinstated.
//
// Same posture as scheduler-worktree-alloc.test.js: real git, throwaway dirs,
// nothing touches the live repo.
//
// Run with: node tools/doctrine-harvest.test.js   (exit 0 = pass)

'use strict'

const fs = require('fs')
const os = require('os')
const path = require('path')
const { execFileSync } = require('child_process')

// Isolate the audit log BEFORE requiring the module. Fixture runs must never
// write into ~/.claude/logs/ecodia/doctrine-harvest.jsonl, which is the real
// signal a human reads to see whether harvest actually fired on a live prune.
// A test that pollutes the log it is testing makes that log unreadable.
process.env.DOCTRINE_HARVEST_LOG = path.join(os.tmpdir(), 'doctrine-harvest-test-audit.jsonl')

const { harvestDoctrine, BUILD } = require('./doctrine-harvest')

let passed = 0, failed = 0
function ok (name, cond, detail) {
  if (cond) { passed++; console.log('  PASS  ' + name) }
  else { failed++; console.log('  FAIL  ' + name + (detail ? '  :: ' + detail : '')) }
}

function git (cwd, args) {
  return execFileSync('git', ['-C', cwd].concat(args), {
    encoding: 'utf8',
    env: Object.assign({}, process.env, {
      GIT_AUTHOR_NAME: 'test', GIT_AUTHOR_EMAIL: 't@t',
      GIT_COMMITTER_NAME: 'test', GIT_COMMITTER_EMAIL: 't@t',
      ECODIAOS_BRANCH_OK: '1',
    }),
  })
}

function setup () {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'doctrine-harvest-test-'))
  const origin = path.join(root, 'origin.git')
  const shared = path.join(root, 'shared')

  fs.mkdirSync(origin)
  execFileSync('git', ['init', '--bare', '-b', 'main', origin])

  execFileSync('git', ['clone', origin, shared], { stdio: 'ignore' })
  fs.mkdirSync(path.join(shared, 'patterns', '_archived'), { recursive: true })
  fs.writeFileSync(path.join(shared, 'patterns', 'existing-rule-2026-01-01.md'), '# an existing rule\n')
  fs.writeFileSync(path.join(shared, 'patterns', '_archived', 'old-rule-2025-01-01.md'), '# archived\n')
  git(shared, ['add', '-A'])
  git(shared, ['commit', '-q', '-m', 'base'])
  git(shared, ['push', '-q', 'origin', 'main'])
  git(shared, ['fetch', 'origin', '--quiet'])
  return { root, origin, shared }
}

// Plant a worker branch the way the dispatcher does: a linked worktree off
// origin/main, commits made inside it, worktree still present at harvest time.
function plantWorkerBranch (shared, root, branch, files) {
  const wt = path.join(root, 'wt-' + branch.replace(/\//g, '-'))
  git(shared, ['worktree', 'add', '-B', branch, wt, 'origin/main'])
  for (const [rel, body] of Object.entries(files)) {
    const full = path.join(wt, rel)
    fs.mkdirSync(path.dirname(full), { recursive: true })
    fs.writeFileSync(full, body)
  }
  git(wt, ['add', '-A'])
  git(wt, ['commit', '-q', '-m', 'doctrine: worker authored this and cannot push it'])
  return wt
}

// THE surface under test: the conductor corpus on disk, which is what
// knowledge-index walks with fs.
function patternsOnDisk (shared) {
  const out = []
  const root = path.join(shared, 'patterns')
  const walk = (dir, prefix) => {
    for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
      const rel = prefix ? prefix + '/' + e.name : e.name
      if (e.isDirectory()) walk(path.join(dir, e.name), rel)
      else out.push('patterns/' + rel)
    }
  }
  walk(root, '')
  return out.sort()
}

function filesOnMain (shared) {
  git(shared, ['fetch', 'origin', '--quiet'])
  return git(shared, ['ls-tree', '-r', '--name-only', 'origin/main']).split('\n').map((s) => s.trim()).filter(Boolean)
}

function readDisk (shared, rel) {
  return fs.readFileSync(path.join(shared, rel), 'utf8')
}

// Absence is an assertion in its own right here: a refusal that still wrote the
// file would pass every reason-string check while doing the exact damage the
// refusal exists to prevent.
function onDisk (shared, rel) {
  return fs.existsSync(path.join(shared, rel))
}

async function main () {
  console.log('doctrine-harvest: real-git proof\n')

  // 1. The headline case, restated. A worker's new pattern must reach the DISK
  //    corpus, and must NOT be published to origin/main.
  {
    const { root, shared } = setup()
    plantWorkerBranch(shared, root, 'worker/row-aaa', {
      'patterns/dummy-harvest-proof-2026-08-23.md': '---\ntriggers: dummy harvest proof\n---\n# dummy harvest proof\n\nA throwaway file planted by the harvest test.\n',
    })

    const before = patternsOnDisk(shared)
    ok('before: dummy file is NOT in the disk corpus', !before.includes('patterns/dummy-harvest-proof-2026-08-23.md'))

    const res = await harvestDoctrine({ sharedTree: shared, branch: 'worker/row-aaa', rowId: 'row-aaa' })
    const after = patternsOnDisk(shared)

    ok('harvest reports ok', res.ok === true, JSON.stringify(res))
    ok('harvest landed exactly the dummy file', res.landed.length === 1 && res.landed[0] === 'patterns/dummy-harvest-proof-2026-08-23.md', JSON.stringify(res.landed))
    ok('AFTER: dummy file IS in the disk corpus', after.includes('patterns/dummy-harvest-proof-2026-08-23.md'), JSON.stringify(after))
    ok('landed content is byte-identical to what the worker wrote',
      readDisk(shared, 'patterns/dummy-harvest-proof-2026-08-23.md').includes('A throwaway file planted by the harvest test.'))

    // NEGATIVE CONTROL. This is the assertion that would have caught the
    // original design. Landing must not publish, and main must not move.
    ok('NEGATIVE CONTROL: dummy file was NOT published to origin/main',
      !filesOnMain(shared).includes('patterns/dummy-harvest-proof-2026-08-23.md'))
    ok('NEGATIVE CONTROL: origin/main did not advance', git(shared, ['rev-list', '--count', 'origin/main']).trim() === '1')
    ok('no commit is claimed in the result', res.commit === null, JSON.stringify(res.commit))

    // The file must be untracked-and-new, so corpus-snapshot-committer picks it
    // up on its own cadence. That is the durability lane the design relies on.
    const st = git(shared, ['status', '--porcelain', '--', 'patterns/']).trim()
    ok('lands as an untracked add the committer will collect', /^\?\? patterns\/dummy-harvest-proof-2026-08-23\.md$/m.test(st), st)
    fs.rmSync(root, { recursive: true, force: true })
  }

  // 2. add-only, and WHICH rail catches what. There are two, they fire at
  //    different layers, and conflating them hides a hole.
  //
  //    (a) A worker REWRITING a pattern that its branch base already carries is
  //        a modification, so --diff-filter=A drops it before it is ever a
  //        candidate. It never reaches the basename check, which is why skipped[]
  //        is empty here rather than carrying a reason. Asserting a skip reason
  //        on this case would be asserting a rail that does not run.
  //
  //    (b) A worker ADDING a basename the DISK corpus already holds is a genuine
  //        add against its base, so only the basename check can stop it. This is
  //        the case the origin/main baseline used to miss completely: a pattern
  //        written by the conductor but not yet pushed is absent from origin/main,
  //        so the old check waved the duplicate through and the harvest published
  //        a second copy of doctrine that already existed on disk.
  {
    const { root, shared } = setup()
    plantWorkerBranch(shared, root, 'worker/row-bbb', {
      'patterns/existing-rule-2026-01-01.md': '# HOSTILE REWRITE of a live pattern\n',
    })
    const res = await harvestDoctrine({ sharedTree: shared, branch: 'worker/row-bbb', rowId: 'row-bbb' })
    ok('(a) existing pattern is untouched on disk', readDisk(shared, 'patterns/existing-rule-2026-01-01.md').trim() === '# an existing rule')
    ok('(a) nothing was landed', res.landed.length === 0, JSON.stringify(res))
    ok('(a) a rewrite is dropped by diff-filter=A, so it never reaches the basename rail',
      res.skipped.length === 0 && /no harvestable file added/.test(res.reason), JSON.stringify(res))
    ok('(a) the working tree is clean', git(shared, ['status', '--porcelain']).trim() === '')

    // (b) The disk-baseline case. Written to disk, deliberately NOT committed, so
    // origin/main does not know about it. The old baseline would have landed a
    // duplicate here.
    fs.writeFileSync(path.join(shared, 'patterns', 'uncommitted-rule-2026-08-27.md'), '# authored by the conductor, not yet pushed\n')
    ok('(b) fixture: on disk, absent from origin/main',
      !filesOnMain(shared).includes('patterns/uncommitted-rule-2026-08-27.md'))

    plantWorkerBranch(shared, root, 'worker/row-bbb2', {
      'patterns/uncommitted-rule-2026-08-27.md': '# a worker DUPLICATE of an uncommitted conductor pattern\n',
    })
    const res2 = await harvestDoctrine({ sharedTree: shared, branch: 'worker/row-bbb2', rowId: 'row-bbb2' })
    ok('(b) a basename present only on DISK still blocks the add',
      res2.landed.length === 0 && res2.skipped.some((s) => /already in the corpus/.test(s.reason)), JSON.stringify(res2))
    ok('(b) the uncommitted conductor pattern was not overwritten',
      readDisk(shared, 'patterns/uncommitted-rule-2026-08-27.md').trim() === '# authored by the conductor, not yet pushed')
    fs.rmSync(root, { recursive: true, force: true })
  }

  // 3. _archived is out of scope BOTH ways: never harvested FROM, and a basename
  //    already sitting in _archived/ is never resurrected into the live corpus.
  //    The second half is the one that bites: re-landing a pattern the conductor
  //    deliberately archived would put superseded doctrine back in front of
  //    knowledge.lookup.
  {
    const { root, shared } = setup()
    plantWorkerBranch(shared, root, 'worker/row-ccc', {
      'patterns/_archived/sneaky-2026-08-23.md': '# should never be harvested\n',
      'src/not-a-pattern.js': 'console.log(1)\n',
    })
    const res = await harvestDoctrine({ sharedTree: shared, branch: 'worker/row-ccc', rowId: 'row-ccc' })
    const after = patternsOnDisk(shared)
    ok('archived file not landed', !after.includes('patterns/_archived/sneaky-2026-08-23.md'))
    ok('non-pattern source not landed', !after.includes('patterns/not-a-pattern.js'))
    ok('reports nothing to harvest', res.ok === true && res.landed.length === 0, JSON.stringify(res))

    // 2026-08-30. The second half above USED to assert the opposite: that an
    // archived basename blocks the landing. It was reversed after the block was
    // caught destroying the highest-value rescue there is. Row 102b5849 restored
    // never-cdp-drive-live-client-editor-with-autosave-2026-07-16 (status active,
    // severity HIGH, the rule that stops a CDP probe mutating a client's live
    // editor) out of _archived at 2026-08-29T16:09:47Z. Harvest ran 10 minutes
    // later, matched the basename against the ARCHIVED copy, and skipped it as
    // 'basename already in the corpus'. The file reached neither disk nor
    // origin/main and knowledge.lookup could not return it.
    //
    // The block was aimed at a stale branch resurrecting superseded doctrine.
    // It cannot tell that apart from a deliberate un-archival, and the two
    // errors are not symmetric: a wrongly-resurrected pattern is VISIBLE in the
    // corpus and re-archived in one move, while a wrongly-refused un-archival is
    // INVISIBLE and permanent, because nothing ever retries a harvest. So the
    // landing goes through and the audit says so loudly, which turns a silent
    // permanent loss into a reviewable reversible one.
    plantWorkerBranch(shared, root, 'worker/row-ccc2', {
      'patterns/old-rule-2025-01-01.md': '# a deliberately un-archived rule\n',
    })
    const res2 = await harvestDoctrine({ sharedTree: shared, branch: 'worker/row-ccc2', rowId: 'row-ccc2' })
    ok('an UN-ARCHIVAL lands in the live corpus',
      res2.landed.includes('patterns/old-rule-2025-01-01.md') &&
      fs.existsSync(path.join(shared, 'patterns', 'old-rule-2025-01-01.md')),
      JSON.stringify(res2))
    ok('the un-archival is announced, not silent',
      Array.isArray(res2.unarchived) && res2.unarchived.includes('old-rule-2025-01-01.md'),
      JSON.stringify(res2))
    ok('the archived copy is left alone (removing it is the conductor\'s call)',
      fs.existsSync(path.join(shared, 'patterns', '_archived', 'old-rule-2025-01-01.md')))
    ok('the landed body is the branch version, not the archived one',
      readDisk(shared, 'patterns/old-rule-2025-01-01.md') === '# a deliberately un-archived rule\n',
      JSON.stringify(readDisk(shared, 'patterns/old-rule-2025-01-01.md')))

    // CONTROL, the direction that must NOT change: a LIVE top-level basename
    // still blocks. Without this the fix reads as 'add-only was removed'.
    plantWorkerBranch(shared, root, 'worker/row-ccc3', {
      'patterns/existing-rule-2026-01-01.md': '# a divergent second copy\n',
    })
    const res3 = await harvestDoctrine({ sharedTree: shared, branch: 'worker/row-ccc3', rowId: 'row-ccc3' })
    ok('a LIVE top-level basename still blocks (add-only holds)',
      res3.landed.length === 0 && readDisk(shared, 'patterns/existing-rule-2026-01-01.md') === '# an existing rule\n',
      JSON.stringify(res3))
    ok('the live-duplicate skip is NOT reported as an un-archival',
      !(res3.unarchived || []).length, JSON.stringify(res3))
    fs.rmSync(root, { recursive: true, force: true })
  }

  // 3b. The origin/main leg of the baseline splits the same way. basenamesOnMain
  //     used `ls-tree -r` and kept every .md at any depth, so a stale copy
  //     archived ON ORIGIN/MAIN poisoned the baseline exactly like the disk leg.
  //     Fixing only the disk leg leaves the bug alive on the other half.
  {
    const { root, shared } = setup()
    // archive it on origin/main only, and remove it from disk entirely, so the
    // ONLY thing that can block the landing is the origin/main archived copy.
    fs.rmSync(path.join(shared, 'patterns', '_archived', 'old-rule-2025-01-01.md'))
    fs.mkdirSync(path.join(shared, 'patterns', '_archived'), { recursive: true })
    fs.writeFileSync(path.join(shared, 'patterns', '_archived', 'main-archived-2025-02-02.md'), '# archived on main\n')
    git(shared, ['add', '-A'])
    git(shared, ['commit', '-q', '-m', 'archive on main'])
    git(shared, ['push', '-q', 'origin', 'main'])
    fs.rmSync(path.join(shared, 'patterns', '_archived', 'main-archived-2025-02-02.md'))
    git(shared, ['fetch', 'origin', '--quiet'])

    plantWorkerBranch(shared, root, 'worker/row-mainarch', {
      'patterns/main-archived-2025-02-02.md': '# un-archived from the main copy\n',
    })
    const res = await harvestDoctrine({ sharedTree: shared, branch: 'worker/row-mainarch', rowId: 'row-mainarch' })
    ok('a basename archived on ORIGIN/MAIN does not block the un-archival',
      res.landed.includes('patterns/main-archived-2025-02-02.md'), JSON.stringify(res))
    ok('the origin/main un-archival is announced too',
      (res.unarchived || []).includes('main-archived-2025-02-02.md'), JSON.stringify(res))
    fs.rmSync(root, { recursive: true, force: true })
  }

  // 3c. The build stamp. The fix is inert until the laptop-agent restarts,
  //     because require() caches the module, and the ONLY way this run could
  //     prove the previous fix had loaded was a reason-string flip that appears
  //     only when something lands. A stamp on EVERY audit line lets a successor
  //     prove the load from any prune at all, including the silent no-ops.
  {
    const { root, shared } = setup()
    const res = await harvestDoctrine({ sharedTree: shared, branch: 'worker/does-not-exist', rowId: 'row-stamp' })
    ok('every result carries the build stamp, even a no-op',
      res.build === BUILD && typeof BUILD === 'string' && BUILD.length > 0, JSON.stringify(res))
    fs.rmSync(root, { recursive: true, force: true })
  }

  // 4. em-dash refusal (character-level ban)
  {
    const { root, shared } = setup()
    plantWorkerBranch(shared, root, 'worker/row-ddd', {
      // Escaped, never a literal: this file is itself subject to the character-level ban.
      'patterns/has-em-dash-2026-08-23.md': '# a rule\n\nthis line carries an em dash ' + '\u2014' + ' right here\n',
      'patterns/is-clean-2026-08-23.md': '# a clean rule\n\nno banned characters here\n',
    })
    const res = await harvestDoctrine({ sharedTree: shared, branch: 'worker/row-ddd', rowId: 'row-ddd' })
    const after = patternsOnDisk(shared)
    ok('em-dash file refused', !after.includes('patterns/has-em-dash-2026-08-23.md'))
    ok('refusal is reported with a reason', res.refused.some((r) => /em-dash/.test(r.reason)), JSON.stringify(res.refused))
    ok('clean sibling still landed', after.includes('patterns/is-clean-2026-08-23.md'), JSON.stringify(after))
    fs.rmSync(root, { recursive: true, force: true })
  }

  // 5. clean no-ops + idempotence
  {
    const { root, shared } = setup()
    const a = await harvestDoctrine({ sharedTree: shared, branch: 'worker/does-not-exist', rowId: 'row-eee' })
    ok('missing branch is a clean no-op, not an error', a.ok === true && /does not exist/.test(a.reason), JSON.stringify(a))

    plantWorkerBranch(shared, root, 'worker/row-fff', { 'README.md': 'no doctrine here\n' })
    const b = await harvestDoctrine({ sharedTree: shared, branch: 'worker/row-fff', rowId: 'row-fff' })
    ok('branch with no patterns is a clean no-op', b.ok === true && b.landed.length === 0, JSON.stringify(b))

    // Re-running the same harvest must be idempotent: the basename is now in the
    // disk corpus, so the second pass lands nothing and the file is not rewritten.
    plantWorkerBranch(shared, root, 'worker/row-ggg', { 'patterns/idem-2026-08-23.md': '# idempotence\n' })
    await harvestDoctrine({ sharedTree: shared, branch: 'worker/row-ggg', rowId: 'row-ggg' })
    const bodyAfterFirst = readDisk(shared, 'patterns/idem-2026-08-23.md')
    const second = await harvestDoctrine({ sharedTree: shared, branch: 'worker/row-ggg', rowId: 'row-ggg' })
    ok('re-harvest is idempotent',
      second.landed.length === 0 && readDisk(shared, 'patterns/idem-2026-08-23.md') === bodyAfterFirst,
      JSON.stringify(second))

    // A dry run must vet without writing anything.
    plantWorkerBranch(shared, root, 'worker/row-hhh', { 'patterns/dry-2026-08-27.md': '# dry run\n' })
    const dry = await harvestDoctrine({ sharedTree: shared, branch: 'worker/row-hhh', rowId: 'row-hhh', dryRun: true })
    ok('dry run reports the file but writes nothing',
      dry.landed.length === 1 && !fs.existsSync(path.join(shared, 'patterns', 'dry-2026-08-27.md')),
      JSON.stringify(dry))
    fs.rmSync(root, { recursive: true, force: true })
  }

  // 6. EVERY invocation is audited, including the silent no-ops.
  //
  // Regression guard for the 2026-08-24 observability fix. Before it, audit() sat
  // inside the success paths only, so a prune that invoked harvest and correctly
  // found nothing wrote no line anywhere. Two live prunes did exactly that on
  // 2026-08-23 and left the mechanism unprovable from its own telemetry.
  // The rule now: one audit line per call, whatever the outcome.
  {
    const { root, shared } = setup()
    const logPath = process.env.DOCTRINE_HARVEST_LOG
    try { fs.unlinkSync(logPath) } catch (_e) {}

    const readLog = () => {
      let raw = ''
      try { raw = fs.readFileSync(logPath, 'utf8') } catch (_e) { return [] }
      return raw.split('\n').filter(Boolean).map((l) => JSON.parse(l))
    }

    await harvestDoctrine({ sharedTree: shared, branch: 'worker/nope', rowId: 'audit-missing-branch' })
    ok('a missing-branch no-op writes an audit line',
      readLog().some((r) => r.rowId === 'audit-missing-branch' && /does not exist/.test(r.reason)),
      JSON.stringify(readLog()))

    plantWorkerBranch(shared, root, 'worker/audit-nopat', { 'README.md': 'no doctrine\n' })
    await harvestDoctrine({ sharedTree: shared, branch: 'worker/audit-nopat', rowId: 'audit-no-patterns' })
    ok('a branch with no patterns writes an audit line',
      readLog().some((r) => r.rowId === 'audit-no-patterns'),
      JSON.stringify(readLog()))

    await harvestDoctrine({ sharedTree: shared, branch: null, rowId: 'audit-bad-args' })
    ok('a bad-args refusal writes an audit line',
      readLog().some((r) => r.rowId === 'audit-bad-args' && r.ok === false),
      JSON.stringify(readLog()))

    ok('one audit line per invocation, no duplicates',
      readLog().filter((r) => r.rowId === 'audit-no-patterns').length === 1,
      JSON.stringify(readLog()))

    fs.rmSync(root, { recursive: true, force: true })
  }

  // 7. The union rail. A basename that is on origin/main but NOT yet on disk is
  //    skipped rather than written, because writing it would create a second,
  //    divergent copy of the same doctrine that the next merge turns into a
  //    conflict. Surfacing that gap is doctrine-branch-drift-canary's job.
  {
    const { root, origin, shared } = setup()

    // Publish a pattern from a SEPARATE clone, so the shared tree's disk never
    // holds it while its origin/main does.
    const other = path.join(root, 'other')
    execFileSync('git', ['clone', origin, other], { stdio: 'ignore' })
    fs.writeFileSync(path.join(other, 'patterns', 'main-only-2026-08-27.md'), '# published elsewhere\n')
    git(other, ['add', '-A'])
    git(other, ['commit', '-q', '-m', 'a pattern that never reached the conductor tree'])
    git(other, ['push', '-q', 'origin', 'main'])
    git(shared, ['fetch', 'origin', '--quiet'])

    ok('fixture: the basename is on origin/main but NOT on disk',
      filesOnMain(shared).includes('patterns/main-only-2026-08-27.md') &&
      !fs.existsSync(path.join(shared, 'patterns', 'main-only-2026-08-27.md')))

    plantWorkerBranch(shared, root, 'worker/row-union', { 'patterns/main-only-2026-08-27.md': '# a DIVERGENT second copy\n' })
    const res = await harvestDoctrine({ sharedTree: shared, branch: 'worker/row-union', rowId: 'row-union' })
    ok('a basename already on origin/main is not written to disk as a second copy',
      res.landed.length === 0 && !fs.existsSync(path.join(shared, 'patterns', 'main-only-2026-08-27.md')),
      JSON.stringify(res))
    fs.rmSync(root, { recursive: true, force: true })
  }

  // 8. The exclusive-create rail. Two harvests racing on the same branch must not
  //    produce a duplicated or half-written file. The old publish path had no
  //    equivalent and the live audit log records the race it allowed: row
  //    d85e97cd landed the same three files twice, commits 995a6658 and
  //    1718914339, eight seconds apart.
  {
    const { root, shared } = setup()
    plantWorkerBranch(shared, root, 'worker/row-race', { 'patterns/race-2026-08-27.md': '# exactly one copy\n' })
    const [r1, r2] = await Promise.all([
      harvestDoctrine({ sharedTree: shared, branch: 'worker/row-race', rowId: 'row-race-1' }),
      harvestDoctrine({ sharedTree: shared, branch: 'worker/row-race', rowId: 'row-race-2' }),
    ])
    const total = r1.landed.length + r2.landed.length
    ok('two concurrent harvests land the file exactly once', total === 1, JSON.stringify([r1.landed, r2.landed]))
    ok('the raced file is intact, not truncated or doubled',
      readDisk(shared, 'patterns/race-2026-08-27.md') === '# exactly one copy\n',
      JSON.stringify(readDisk(shared, 'patterns/race-2026-08-27.md')))
    ok('both invocations report ok', r1.ok === true && r2.ok === true, JSON.stringify([r1.reason, r2.reason]))
    fs.rmSync(root, { recursive: true, force: true })
  }

  // 9. BRANCH-NAME MISMATCH (2026-08-30 lane H1). The scheduler asks for
  //    'worker/' + row.id, but a worker may commit on a ref of its own naming.
  //    Row 5fd046e8-8e0b-4a13-923f-16198b7acd07 used worker/5fd0-seedtree-lane-N1-pass4,
  //    so harvest resolved an empty ref and logged 'no commits off main' while the
  //    doctrine sat one ref away. RED before the resolveBranch fallback: landed
  //    was empty and the reason read as a clean no-op.
  {
    const { root, shared } = setup()
    const rowId = '5fd046e8-8e0b-4a13-923f-16198b7acd07'
    plantWorkerBranch(shared, root, 'worker/5fd0-seedtree-lane-N1-pass4',
      { 'patterns/mismatched-branch-2026-08-30.md': '# found via the id prefix\n' })
    const res = await harvestDoctrine({
      sharedTree: shared, branch: 'worker/' + rowId, rowId,
    })
    ok('a branch named off-convention is still found by the row id',
      res.landed.length === 1, JSON.stringify(res))
    ok('the mismatched file reaches disk',
      readDisk(shared, 'patterns/mismatched-branch-2026-08-30.md') === '# found via the id prefix\n',
      JSON.stringify(res))
    ok('the audit names the ref it actually harvested and the one it was asked for',
      res.branch === 'worker/5fd0-seedtree-lane-N1-pass4' &&
      res.requestedBranch === 'worker/' + rowId &&
      /^id-prefix:/.test(res.resolvedVia || ''), JSON.stringify(res))
    fs.rmSync(root, { recursive: true, force: true })
  }

  // 10. AMBIGUITY REFUSES RATHER THAN GUESSES. Harvesting the wrong branch writes
  //     real files into the corpus under a real basename, so two candidates at the
  //     same prefix must produce a named refusal, not a coin flip.
  {
    const { root, shared } = setup()
    const rowId = '7ab12cde-1111-2222-3333-444455556666'
    plantWorkerBranch(shared, root, 'worker/7ab1-lane-A1', { 'patterns/amb-a-2026-08-30.md': '# a\n' })
    plantWorkerBranch(shared, root, 'worker/7ab1-lane-B2', { 'patterns/amb-b-2026-08-30.md': '# b\n' })
    const res = await harvestDoctrine({ sharedTree: shared, branch: 'worker/' + rowId, rowId })
    ok('two candidates at one prefix refuse instead of guessing',
      res.landed.length === 0 && /ambiguous/.test(res.reason || ''), JSON.stringify(res))
    ok('the refusal names both candidates so a human can pick',
      /7ab1-lane-A1/.test(res.reason || '') && /7ab1-lane-B2/.test(res.reason || ''), JSON.stringify(res))
    ok('neither ambiguous file was written to disk',
      !onDisk(shared, 'patterns/amb-a-2026-08-30.md') &&
      !onDisk(shared, 'patterns/amb-b-2026-08-30.md'), 'one of them landed')
    fs.rmSync(root, { recursive: true, force: true })
  }

  // 11. ANOTHER ROW'S WORKER BRANCH IS NEVER A CANDIDATE. 'worker/<other-uuid>'
  //     can share a 4-char prefix by chance, and landing its files under this row's
  //     audit line would misattribute doctrine to the wrong job.
  {
    const { root, shared } = setup()
    const rowId = '9c3d0a11-0000-1111-2222-333344445555'
    const other = '9c3d0a99-8888-7777-6666-555544443333'
    plantWorkerBranch(shared, root, 'worker/' + other, { 'patterns/other-row-2026-08-30.md': '# not mine\n' })
    const res = await harvestDoctrine({ sharedTree: shared, branch: 'worker/' + rowId, rowId })
    ok('a sibling row\'s worker branch is excluded from the id-prefix search',
      res.landed.length === 0 && res.reason === 'branch does not exist (nothing to harvest)',
      JSON.stringify(res))
    ok('the sibling row\'s file did not land',
      !onDisk(shared, 'patterns/other-row-2026-08-30.md'), 'it landed')
    fs.rmSync(root, { recursive: true, force: true })
  }

  // 12. CONTROL: the exact branch still wins and is never re-resolved. Without
  //     this the fallback could quietly take over the common path.
  {
    const { root, shared } = setup()
    const rowId = 'aa11bb22-cccc-dddd-eeee-ffff00001111'
    plantWorkerBranch(shared, root, 'worker/' + rowId, { 'patterns/exact-wins-2026-08-30.md': '# exact\n' })
    plantWorkerBranch(shared, root, 'worker/aa11-decoy', { 'patterns/decoy-2026-08-30.md': '# decoy\n' })
    const res = await harvestDoctrine({ sharedTree: shared, branch: 'worker/' + rowId, rowId })
    ok('the exact branch is used when it has commits',
      res.resolvedVia === 'exact' && res.branch === 'worker/' + rowId, JSON.stringify(res))
    ok('the decoy branch was not harvested',
      !onDisk(shared, 'patterns/decoy-2026-08-30.md') &&
      readDisk(shared, 'patterns/exact-wins-2026-08-30.md') === '# exact\n', JSON.stringify(res))
    fs.rmSync(root, { recursive: true, force: true })
  }

  // ==========================================================================
  // PREFIX WIDENING (2026-09-02, board 8b7b19e4). The harvest used to hardcode
  // 'patterns/' in three places, so every non-pattern deliverable a worker
  // committed died with its branch. These cases pin the widened shape, and the
  // one that actually matters is the cross-namespace basename: a global
  // basename set would refuse a postmortem for colliding with a PATTERN it can
  // never shadow, and that refusal is invisible and permanent because nothing
  // ever retries a harvest.
  // ==========================================================================

  // 14. A postmortem lands in its OWN directory, not flattened into patterns/.
  {
    const { root, shared } = setup()
    plantWorkerBranch(shared, root, 'worker/row-pm1', {
      'drafts/postmortems/chat-postmortem-2099-01-01.md': '# a postmortem the worker could not push\n',
    })
    const res = await harvestDoctrine({ sharedTree: shared, branch: 'worker/row-pm1', rowId: 'row-pm1' })
    ok('a postmortem is harvested at all',
      res.ok === true && res.landed.includes('drafts/postmortems/chat-postmortem-2099-01-01.md'), JSON.stringify(res))
    ok('it lands under drafts/postmortems/, NOT flattened into patterns/',
      onDisk(shared, 'drafts/postmortems/chat-postmortem-2099-01-01.md') &&
      !onDisk(shared, 'patterns/chat-postmortem-2099-01-01.md'), JSON.stringify(patternsOnDisk(shared)))
    ok('the landed postmortem body is byte-identical to what the worker wrote',
      readDisk(shared, 'drafts/postmortems/chat-postmortem-2099-01-01.md') === '# a postmortem the worker could not push\n')
    ok('NEGATIVE CONTROL: the postmortem was NOT published to origin/main',
      !filesOnMain(shared).includes('drafts/postmortems/chat-postmortem-2099-01-01.md'))
    fs.rmSync(root, { recursive: true, force: true })
  }

  // 15. THE CASE A GLOBAL BASELINE GETS WRONG. Same basename, two namespaces.
  //     The pattern already exists on disk; the postmortem of the same name has
  //     never been seen. They land in different directories and neither can
  //     shadow the other, so the postmortem must NOT be refused.
  {
    const { root, shared } = setup()
    plantWorkerBranch(shared, root, 'worker/row-pm2', {
      'drafts/postmortems/existing-rule-2026-01-01.md': '# a postmortem that happens to share a pattern basename\n',
    })
    ok('fixture: the basename is already LIVE in patterns/',
      onDisk(shared, 'patterns/existing-rule-2026-01-01.md'))
    const res = await harvestDoctrine({ sharedTree: shared, branch: 'worker/row-pm2', rowId: 'row-pm2' })
    ok('a basename live in ANOTHER namespace does not block this one',
      res.landed.includes('drafts/postmortems/existing-rule-2026-01-01.md'), JSON.stringify(res))
    ok('the same-named pattern was left untouched',
      readDisk(shared, 'patterns/existing-rule-2026-01-01.md').trim() === '# an existing rule')
    ok('the cross-namespace landing is not mislabelled an un-archival',
      !(res.unarchived || []).includes('existing-rule-2026-01-01.md'), JSON.stringify(res.unarchived))
    fs.rmSync(root, { recursive: true, force: true })
  }

  // 16. Add-only still holds WITHIN the new namespace. A postmortem basename
  //     already on disk blocks, exactly as a pattern basename does.
  {
    const { root, shared } = setup()
    fs.mkdirSync(path.join(shared, 'drafts', 'postmortems'), { recursive: true })
    fs.writeFileSync(path.join(shared, 'drafts', 'postmortems', 'chat-postmortem-2099-02-02.md'),
      '# the conductor wrote this one\n')
    plantWorkerBranch(shared, root, 'worker/row-pm3', {
      'drafts/postmortems/chat-postmortem-2099-02-02.md': '# HOSTILE REWRITE from a worker branch\n',
    })
    const res = await harvestDoctrine({ sharedTree: shared, branch: 'worker/row-pm3', rowId: 'row-pm3' })
    ok('a live postmortem basename blocks the add',
      res.landed.length === 0 &&
      res.skipped.some((x) => x.path === 'drafts/postmortems/chat-postmortem-2099-02-02.md'), JSON.stringify(res))
    ok('the existing postmortem was NOT overwritten',
      readDisk(shared, 'drafts/postmortems/chat-postmortem-2099-02-02.md') === '# the conductor wrote this one\n')
    fs.rmSync(root, { recursive: true, force: true })
  }

  // 17. TOP LEVEL OF THE PREFIX ONLY, and the SCOPE LIMIT this ships with.
  //     Measured 2026-09-02 across the 150 most recent worker/* branches: 50
  //     added paths were stranded and NONE of them was drafts/postmortems/*.md.
  //     The real stranding population is nested per-run scratch, and this case
  //     pins that it is still NOT harvested, so the review row's claim is
  //     proven by a test rather than asserted in prose.
  {
    const { root, shared } = setup()
    plantWorkerBranch(shared, root, 'worker/row-pm4', {
      'drafts/postmortems/nested/deep-2099-03-03.md': '# nested under the prefix\n',
      'drafts/ceiling-wrapper-census-2099-03-03/report.md': '# the shape that actually strands today\n',
      'scripts/some-worker-helper.js': 'module.exports = {}\n',
      'drafts/postmortems/chat-postmortem-2099-03-03.md': '# the one in scope\n',
    })
    const res = await harvestDoctrine({ sharedTree: shared, branch: 'worker/row-pm4', rowId: 'row-pm4' })
    ok('the in-scope postmortem landed', res.landed.includes('drafts/postmortems/chat-postmortem-2099-03-03.md'), JSON.stringify(res))
    ok('a file NESTED under the prefix is not harvested',
      !onDisk(shared, 'drafts/postmortems/nested/deep-2099-03-03.md') &&
      res.landed.length === 1, JSON.stringify(res.landed))
    ok('SCOPE LIMIT: per-run scratch (the measured stranding population) is still not harvested',
      !onDisk(shared, 'drafts/ceiling-wrapper-census-2099-03-03/report.md'))
    ok('SCOPE LIMIT: a non-md worker deliverable is still not harvested',
      !onDisk(shared, 'scripts/some-worker-helper.js'))
    fs.rmSync(root, { recursive: true, force: true })
  }

  // 18. The em-dash rail is a property of the harvest, not of patterns/, so it
  //     must hold on every namespace the table grows.
  {
    const { root, shared } = setup()
    plantWorkerBranch(shared, root, 'worker/row-pm5', {
      'drafts/postmortems/dirty-2099-04-04.md': '# a postmortem carrying a banned ' + '\u2014' + ' dash\n',
      'drafts/postmortems/clean-2099-04-04.md': '# a clean sibling\n',
    })
    const res = await harvestDoctrine({ sharedTree: shared, branch: 'worker/row-pm5', rowId: 'row-pm5' })
    ok('an em-dash postmortem is refused',
      !onDisk(shared, 'drafts/postmortems/dirty-2099-04-04.md') &&
      res.refused.some((r) => /em-dash/.test(r.reason)), JSON.stringify(res))
    ok('its clean sibling still lands',
      onDisk(shared, 'drafts/postmortems/clean-2099-04-04.md'), JSON.stringify(res.landed))
    fs.rmSync(root, { recursive: true, force: true })
  }

  // 19. Mixed batch across both namespaces in one commit, which is the shape a
  //     real worker leaves behind (it writes doctrine AND a report).
  {
    const { root, shared } = setup()
    plantWorkerBranch(shared, root, 'worker/row-pm6', {
      'patterns/mixed-batch-rule-2099-05-05.md': '# doctrine\n',
      'drafts/postmortems/mixed-batch-2099-05-05.md': '# the report\n',
    })
    const res = await harvestDoctrine({ sharedTree: shared, branch: 'worker/row-pm6', rowId: 'row-pm6' })
    ok('both namespaces land from one commit',
      onDisk(shared, 'patterns/mixed-batch-rule-2099-05-05.md') &&
      onDisk(shared, 'drafts/postmortems/mixed-batch-2099-05-05.md') &&
      res.landed.length === 2, JSON.stringify(res))
    ok('the build stamp records which harvest wrote them', typeof res.build === 'string' && res.build === BUILD)
    fs.rmSync(root, { recursive: true, force: true })
  }

  console.log('\n  ' + passed + ' passed, ' + failed + ' failed')
  process.exit(failed === 0 ? 0 : 1)
}

main().catch((e) => { console.error(e); process.exit(1) })
