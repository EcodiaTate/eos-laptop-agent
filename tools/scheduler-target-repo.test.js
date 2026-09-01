// scheduler-target-repo.test.js - REAL-git regression battery for cross-repo
// dispatch worktree allocation (2026-09-02, status_board b686e9c8, postmortem
// chat-postmortem-2026-08-20 item L3).
//
// The defect: defaultAllocateWorktreeForRow allocated EVERY worktree off the
// hardcoded SHARED_TREE (ecodiaos-backend). A task targeting another repo got a
// worktree holding none of its code, abandoned it, and worked directly on that
// repo's live main checkout beside another worker's uncommitted WIP. Cluster
// dispatch-worktree-wrong-repo-forces-shared-tree-collision, session b25ce101,
// frequency 2, both NOT_CAUGHT. Doctrine:
// ecodiaos/backend/patterns/dispatch-worktree-must-branch-off-task-target-repo-2026-08-20.md
//
// Every assertion below runs against real git in a throwaway tree. The two
// CONTROLS are the load-bearing ones: without them a resolver that simply
// returned SHARED_TREE for everything, or one that honoured any string it was
// handed, would both read green.
//
// Run with: node tools/scheduler-target-repo.test.js  (exit 0 = pass)

'use strict'

const fs = require('fs')
const os = require('os')
const path = require('path')
const { execFileSync } = require('child_process')

let passed = 0, failed = 0
function assert(c, label) { if (c) { console.log('  PASS:', label); passed++ } else { console.error('  FAIL:', label); failed++ } }
function git(cwd, args) { return execFileSync('git', ['-C', cwd].concat(args), { encoding: 'utf8' }) }

const TMP = fs.mkdtempSync(path.join(os.tmpdir(), 'sched-target-repo-'))
const WTROOT = path.join(TMP, 'worktrees')

// Stand up a repo with an origin, a main branch, and one uniquely-named file so
// a worktree can be identified by CONTENT, not just by path.
function makeRepo(name, marker) {
  const origin = path.join(TMP, name + '.git')
  const tree = path.join(TMP, name)
  fs.mkdirSync(origin, { recursive: true }); execFileSync('git', ['init', '--bare', '-b', 'main', origin])
  fs.mkdirSync(tree, { recursive: true })
  git(tree, ['init', '-b', 'main'])
  git(tree, ['config', 'user.email', 'test@ecodia.au'])
  git(tree, ['config', 'user.name', 'target-repo-test'])
  fs.writeFileSync(path.join(tree, marker), name + '\n')
  git(tree, ['add', '-A']); git(tree, ['commit', '-q', '-m', 'seed ' + name])
  git(tree, ['remote', 'add', 'origin', origin]); git(tree, ['push', '-q', '-u', 'origin', 'main'])
  return tree
}

const SHARED = makeRepo('shared', 'ONLY-IN-SHARED.md')
const OTHER = makeRepo('ecosphere', 'ONLY-IN-OTHER.md')

process.env.SCHEDULER_SHARED_TREE = SHARED
process.env.SCHEDULER_WORKTREE_ROOT = WTROOT
process.env.SCHEDULER_CODE_ROOT = TMP
const scheduler = require('./scheduler')

const rid = (n) => '00000000-0000-4000-8000-00000000000' + n

async function main() {
  // ── 1. THE DEFAULT IS UNCHANGED. This is the regression guard on the 99pct
  // case: a row with neither field allocates off SHARED_TREE exactly as before.
  {
    const row = { id: rid(1) }
    const wt = await scheduler.allocateWorktreeForRow(row)
    assert(fs.existsSync(path.join(wt, 'ONLY-IN-SHARED.md')),
      'default row (no target_repo, no directive) still allocates off SHARED_TREE')
    assert(git(SHARED, ['worktree', 'list', '--porcelain']).includes(wt),
      'default worktree is registered in SHARED_TREE')
    assert(scheduler.resolveTargetRepo(row).source === 'default',
      'resolveTargetRepo reports source=default when nothing is set')
  }

  // ── 2. THE FIX. A prompt directive routes the allocation to the target repo.
  // The discriminating assertion is the CONTENT: the worktree holds the target
  // repo's file and NOT the shared tree's, which is the whole defect.
  {
    const row = { id: rid(2), prompt: 'do the thing\nTARGET_REPO: ' + OTHER + '\nmore text' }
    const r = scheduler.resolveTargetRepo(row)
    assert(r.repo === OTHER && r.source === 'prompt', 'prompt directive resolves to the target repo')
    const wt = await scheduler.allocateWorktreeForRow(row)
    assert(fs.existsSync(path.join(wt, 'ONLY-IN-OTHER.md')),
      'cross-repo worktree CONTAINS the target repo code (the defect this fixes)')
    assert(!fs.existsSync(path.join(wt, 'ONLY-IN-SHARED.md')),
      'control: the cross-repo worktree is NOT a shared-tree checkout')
    assert(git(OTHER, ['worktree', 'list', '--porcelain']).includes(wt),
      'cross-repo worktree is registered in the TARGET repo')
    assert(!git(SHARED, ['worktree', 'list', '--porcelain']).includes(wt),
      'control: it is not registered in SHARED_TREE')
  }

  // ── 3. row.target_repo wins over a prompt directive (a real column, when one
  // is added, must outrank text a prompt can carry).
  {
    const row = { id: rid(3), target_repo: OTHER, prompt: 'TARGET_REPO: ' + SHARED }
    const r = scheduler.resolveTargetRepo(row)
    assert(r.repo === OTHER && r.source === 'row', 'row.target_repo outranks the prompt directive')
  }

  // ── 4. VALIDATION. row.prompt carries ingested external text on some rows, so
  // an unvalidated directive is an injection surface. Every rejection must fall
  // back to SHARED_TREE and say so.
  {
    const cases = [
      ['relative path', 'tools'],
      ['dot-dot escape', TMP + '/../etc'],
      ['outside CODE_ROOT', '/etc'],
      ['does not exist', path.join(TMP, 'no-such-repo')],
      ['not a git repo', (() => { const d = path.join(TMP, 'plain-dir'); fs.mkdirSync(d, { recursive: true }); return d })()],
    ]
    for (const [label, candidate] of cases) {
      const r = scheduler.resolveTargetRepo({ id: rid(4), prompt: 'TARGET_REPO: ' + candidate })
      assert(r.repo === SHARED && r.rejected === candidate,
        'rejects ' + label + ' and falls back to SHARED_TREE')
    }
  }

  // ── 5. CONTROL on the regex anchor. A TARGET_REPO: token that is not at the
  // start of a line (quoted inside a sentence, which is how ingested text most
  // often carries one) must NOT be honoured.
  {
    const r = scheduler.resolveTargetRepo({
      id: rid(5),
      prompt: 'the worker said TARGET_REPO: ' + OTHER + ' in its report',
    })
    assert(r.repo === SHARED && r.source === 'default' && r.rejected === null,
      'control: a mid-line TARGET_REPO: mention is not a directive')
  }

  // ── 6. repoForWorktreePath reads ownership off disk.
  {
    const wt = path.join(WTROOT, rid(2))
    // realpath both sides: macOS resolves /var to /private/var and git writes
    // the REALPATH into the .git file, so a literal string compare here would
    // fail on a difference that is not a difference. The production paths under
    // /Users/ecodia/.code have no symlinked ancestor, and sameRepo() in
    // scheduler.js normalises the same way for the harvest gate.
    assert(fs.realpathSync(scheduler.repoForWorktreePath(wt)) === fs.realpathSync(OTHER),
      'repoForWorktreePath resolves a linked worktree to its owning repo')
    assert(scheduler.repoForWorktreePath(SHARED) === null,
      'control: a normal repo (.git is a directory) is not a linked worktree')
    assert(scheduler.repoForWorktreePath(path.join(TMP, 'plain-dir')) === null,
      'control: a plain directory resolves to null, which callers treat as preserve')
  }

  // ── 7. CROSS-REPO PRUNE. The row handed to prune deliberately carries NO
  // directive, so the only way to remove the worktree is to read the owner off
  // disk. Under the old code this `worktree remove` ran against SHARED_TREE and
  // was a silent no-op, leaking the directory forever.
  {
    const wt = path.join(WTROOT, rid(2))
    assert(fs.existsSync(wt), 'precondition: the cross-repo worktree is still on disk')
    await scheduler.pruneWorktreeForRow({ id: rid(2) })
    assert(!fs.existsSync(wt), 'prune removes a cross-repo worktree from a directive-less row')
    assert(!git(OTHER, ['worktree', 'list', '--porcelain']).includes(wt),
      'the target repo no longer lists it')
  }

  // ── 8. doctrine-harvest is SHARED_TREE only. Harvesting a foreign branch
  // would either fail on a missing patterns/ dir or commit a pattern file into
  // a client repo, so a cross-repo worker's doctrine must NOT land in the corpus.
  {
    const row = { id: rid(6), prompt: 'TARGET_REPO: ' + OTHER }
    const wt = await scheduler.allocateWorktreeForRow(row)
    const rel = 'patterns/foreign-doctrine-must-not-be-harvested-2026-09-02.md'
    fs.mkdirSync(path.join(wt, 'patterns'), { recursive: true })
    fs.writeFileSync(path.join(wt, rel), '# authored on a foreign repo branch\n')
    git(wt, ['config', 'user.email', 'test@ecodia.au'])
    git(wt, ['config', 'user.name', 'target-repo-test'])
    git(wt, ['add', '-A']); git(wt, ['commit', '-q', '-m', 'foreign doctrine'])
    await scheduler.pruneWorktreeForRow(row)
    assert(!fs.existsSync(path.join(SHARED, rel)),
      'a foreign repo branch is NOT harvested into the shared-tree patterns corpus')
    assert(git(OTHER, ['ls-tree', '-r', '--name-only', 'worker/' + rid(6)]).includes(rel),
      'control: the file is still on the target repo branch, so it was preserved not lost')
  }

  // ── 9. The brief names the repo the worker must avoid. A cross-repo dispatch
  // used to be warned off ecodiaos-backend while its real collision hazard was
  // the target repo's own live checkout, which the brief never named.
  {
    const brief = scheduler.buildBrief({
      id: rid(7), prompt: 'TARGET_REPO: ' + OTHER, worktree_path: path.join(WTROOT, rid(7)),
    })
    assert(brief.includes('Do NOT operate on ' + OTHER),
      'buildBrief warns the worker off the RESOLVED shared tree')
    assert(!brief.includes('Do NOT operate on ' + SHARED),
      'control: it does not name the wrong tree on a cross-repo dispatch')
    const plain = scheduler.buildBrief({ id: rid(8), worktree_path: path.join(WTROOT, rid(8)) })
    assert(plain.includes('Do NOT operate on ' + SHARED),
      'a same-repo dispatch still names SHARED_TREE (default unchanged)')
  }
}

main()
  .then(() => {})
  .catch(e => { console.error('  UNCAUGHT:', e && e.stack || e); failed++ })
  .finally(() => {
    try { fs.rmSync(TMP, { recursive: true, force: true }) } catch (_e) {}
    console.log('\nResults: ' + passed + ' passed, ' + failed + ' failed')
    process.exit(failed > 0 ? 1 : 0)
  })
