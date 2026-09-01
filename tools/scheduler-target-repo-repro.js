// Version-agnostic reproduction of the cross-repo dispatch defect (b686e9c8 L3).
// Runs identically against OLD and NEW scheduler.js: it never calls an API the
// old code lacks, it only asks what the ALLOCATED WORKTREE ACTUALLY CONTAINS.
// OLD: the worktree holds the shared tree's code. NEW: it holds the target repo's.
'use strict'
const fs = require('fs'), os = require('os'), path = require('path')
const { execFileSync } = require('child_process')
const TMP = fs.mkdtempSync(path.join(os.tmpdir(), 'L3-repro-'))
const git = (c, a) => execFileSync('git', ['-C', c].concat(a), { encoding: 'utf8' })
function makeRepo(n, marker) {
  const o = path.join(TMP, n + '.git'), t = path.join(TMP, n)
  fs.mkdirSync(o, { recursive: true }); execFileSync('git', ['init', '--bare', '-b', 'main', o])
  fs.mkdirSync(t, { recursive: true }); git(t, ['init', '-b', 'main'])
  git(t, ['config', 'user.email', 'r@e.au']); git(t, ['config', 'user.name', 'repro'])
  fs.writeFileSync(path.join(t, marker), n + '\n')
  git(t, ['add', '-A']); git(t, ['commit', '-q', '-m', 's'])
  git(t, ['remote', 'add', 'origin', o]); git(t, ['push', '-q', '-u', 'origin', 'main'])
  return t
}
const SHARED = makeRepo('shared', 'ONLY-IN-SHARED.md')
const OTHER = makeRepo('ecosphere', 'ONLY-IN-OTHER.md')
process.env.SCHEDULER_SHARED_TREE = SHARED
process.env.SCHEDULER_WORKTREE_ROOT = path.join(TMP, 'wt')
process.env.SCHEDULER_CODE_ROOT = TMP
const scheduler = require(process.argv[2])
const row = { id: '00000000-0000-4000-8000-0000000000aa', prompt: 'work on it\nTARGET_REPO: ' + OTHER + '\n' }
scheduler.allocateWorktreeForRow(row).then(wt => {
  const hasTarget = fs.existsSync(path.join(wt, 'ONLY-IN-OTHER.md'))
  const hasShared = fs.existsSync(path.join(wt, 'ONLY-IN-SHARED.md'))
  console.log('worktree:', wt)
  console.log('contains the TARGET repo code (ONLY-IN-OTHER.md): ', hasTarget)
  console.log('contains the SHARED tree code (ONLY-IN-SHARED.md):', hasShared)
  console.log(hasTarget && !hasShared
    ? 'RESULT: PASS - the worker gets the repo its task named'
    : 'RESULT: FAIL - the worker gets a worktree of the WRONG repo and will fall back to the target repo live checkout')
  process.exitCode = (hasTarget && !hasShared) ? 0 : 1
}).catch(e => { console.error('UNCAUGHT:', e && e.message); process.exitCode = 2 })
  .finally(() => { try { fs.rmSync(TMP, { recursive: true, force: true }) } catch (_e) {} })
