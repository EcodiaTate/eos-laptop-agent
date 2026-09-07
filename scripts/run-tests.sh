#!/usr/bin/env bash
# shell-lint-ok
# run-tests.sh - THE commit gate for eos-laptop-agent. Invoked as `npm test`.
#
# WHY THIS EXISTS (2026-08-02, account-switch + usage rebuild stage 0): this repo had
# no test script, no git hooks and no CI, so "each stage is one revertable commit" had
# no gate behind it. It also had two live foot-guns that made an ad-hoc `node x.test.js`
# unsafe:
#   1. usage.test.js and coord.test.js resolve COORD_ROOT from the environment and
#      write PRODUCTION ~/.ecodiaos/coordination state when it is unset. A bare test run
#      would clobber the live accounts/worker substrate.
#   2. cred-refresher.test.js case 4 spawns the real text-tate.js and TEXTS TATE unless
#      TEXT_TATE_PATH points at a stub.
# Every sandbox env below is load-bearing for one of those. Do not "simplify" them away.
#
# Also enforces a syntax gate (node --check) over the files the rebuild touches: a file
# that does not parse is a broken baseline, and the suites would not surface it.
#
# Usage: npm test            (syntax gate + all suites)
#        npm test -- --quick (syntax gate only)
set -uo pipefail
cd "$(dirname "$0")/.." || exit 2
ROOT="$(pwd)"

SANDBOX="$(mktemp -d "${TMPDIR:-/tmp}/eos-agent-tests.XXXXXX")"
# cred-refresher.test.js copies live Keychain material into fixtures - the trap wipes it
# even on interrupt, so a sandbox never outlives the run.
#
# THE SIGNAL HANDLER MUST EXIT, NOT FALL THROUGH (fixed 2026-08-28, lane R1).
# It was `trap cleanup EXIT INT TERM` with a cleanup() that only rm -rf'd. A bash
# trap handler that does not exit RESUMES the script, so a single stray SIGTERM
# wiped $SANDBOX and then let the remaining suites keep running against a deleted
# directory. Every one of them failed on `out.NNN: No such file or directory` and
# was counted as a genuine FAIL. Measured this arc: one SIGTERM at suite 7 of 42
# turned a real `pass=40 fail=2` into a reported `pass=6 fail=36`, i.e. 34 fabricated
# failures, in the script that IS this repo's commit gate. A gate that reports red
# for a reason unrelated to the code under test is worse than no gate: it trains
# every reader to ignore it, and it hid a live regression (04dd65d added a 13th
# setInterval while scheduler.test.js still asserted 6 in start()).
cleanup() { rm -rf "$SANDBOX"; }
on_signal() {
  echo "" >&2
  echo "[run-tests] ABORTED by signal - results up to this point are incomplete." >&2
  cleanup
  exit 130
}
trap cleanup EXIT
trap on_signal INT TERM

mkdir -p "$SANDBOX/coord" "$SANDBOX/tmp"
# Stub text-tate: records the call, never sends. cred-refresher.test.js case 4 shells this.
cat > "$SANDBOX/stub-text-tate.js" <<'STUB'
console.log('[stub-text-tate] suppressed: ' + process.argv.slice(2).join(' ').slice(0, 120))
process.exit(0)
STUB

export COORD_ROOT="$SANDBOX/coord"
export COORD_DISABLE_SWEEP=1
export TMPDIR="$SANDBOX/tmp"
export TEXT_TATE_PATH="$SANDBOX/stub-text-tate.js"

TIMEOUT_S="${TEST_TIMEOUT_S:-120}"
fails=0; passes=0; timeouts=0; suite_n=0

# REDACTION IS MANDATORY, NOT HYGIENE. cred-refresher.test.js seeds its fixtures from the
# LIVE Keychain, and its assertion messages print the value they compared ("got <token>").
# A raw failure tail therefore leaks a live-lineage OAuth token into whatever reads this
# gate's stdout (terminal scrollback, CI log, or an agent transcript). Observed live
# 2026-08-02. Every suite's output passes through here before it is ever echoed.
redact() {
  sed -E \
    -e 's/sk-ant-[A-Za-z0-9._-]{8,}/sk-ant-<REDACTED>/g' \
    -e 's/(eyJ[A-Za-z0-9_-]{10,})\.[A-Za-z0-9._-]+/<JWT-REDACTED>/g' \
    -e 's/(gh[pousr]_)[A-Za-z0-9]{10,}/\1<REDACTED>/g' \
    -e 's/(refresh_?[Tt]oken"?[: =]+"?)[A-Za-z0-9._-]{12,}/\1<REDACTED>/g' \
    -e 's/(access_?[Tt]oken"?[: =]+"?)[A-Za-z0-9._-]{12,}/\1<REDACTED>/g'
}

# node has no built-in per-process timeout; this wrapper bounds a hung suite (input.test.js
# passes 33/33 but never exits, which would wedge the whole gate).
#
# The watchdog MUST have its stdout closed (>/dev/null 2>&1). A command substitution waits
# for every process holding the pipe, not just the one it is waiting on, so a watchdog that
# inherits stdout makes EVERY suite take the full TIMEOUT_S even when node exits in 200ms
# (18 suites x 100s = a 30-minute "hang"). Writing the output to a file instead of capturing
# it inline keeps the substitution out of the picture entirely.
run_bounded() {
  local file="$1" label="$2"
  local out rc
  # Fail LOUD if the sandbox went away mid-run. Without this the redirect below
  # fails per suite and every remaining suite reports FAIL(1) for a reason that has
  # nothing to do with the code under test (see the trap note above).
  if [ ! -d "$SANDBOX" ]; then
    echo "[run-tests] FATAL: sandbox $SANDBOX vanished mid-run; aborting rather than reporting fabricated failures" >&2
    exit 3
  fi
  suite_n=$((suite_n+1))
  local tmpout="$SANDBOX/out.$$.$suite_n"
  node "$file" >"$tmpout" 2>&1 &
  local pid=$!
  ( sleep "$TIMEOUT_S"; kill -9 "$pid" 2>/dev/null ) >/dev/null 2>&1 &
  local watchdog=$!
  wait "$pid"; rc=$?
  kill "$watchdog" 2>/dev/null; wait "$watchdog" 2>/dev/null
  out="$(redact < "$tmpout" 2>/dev/null)"; rm -f "$tmpout"
  if [ "$rc" = "0" ]; then
    passes=$((passes+1)); echo "  PASS  $label"
  elif [ "$rc" = "137" ]; then
    timeouts=$((timeouts+1)); echo "  TIMEOUT($TIMEOUT_S s)  $label"
  else
    fails=$((fails+1)); echo "  FAIL($rc)  $label"
    printf '%s\n' "$out" | tail -12 | sed 's/^/        /'
  fi
}

echo "=== syntax gate (node --check) ==="
SYNTAX_FILES=(
  tools/usage.js tools/creds.js tools/account-cap-decide.js tools/real-limit-watch.js
  daemons/usage-poller.js daemons/cred-refresher.js tools/cowork.js routes/mcpCoord.js
  tools/scheduler.js
)
# New rebuild modules are gated too, once they exist.
for extra in tools/usage-config.js tools/usage-real.js tools/accounts-registry.js \
             tools/switch-core.js tools/usage-capacity.js scripts/switch-run.js; do
  [ -f "$ROOT/$extra" ] && SYNTAX_FILES+=("$extra")
done
syntax_fails=0
for f in "${SYNTAX_FILES[@]}"; do
  if node --check "$ROOT/$f" 2>/dev/null; then echo "  ok    $f"
  else echo "  BROKEN $f"; node --check "$ROOT/$f" 2>&1 | head -3 | sed 's/^/        /'; syntax_fails=$((syntax_fails+1)); fi
done
[ "${1:-}" = "--quick" ] && { echo "syntax_fails=$syntax_fails (quick mode, suites skipped)"; exit $((syntax_fails > 0)); }

echo "=== selftests ==="
for s in tools/account-cap-decide.js tools/real-limit-watch.js tools/usage-real.js tools/switch-core.js tools/usage-capacity.js; do
  [ -f "$ROOT/$s" ] || continue
  if out="$(node "$ROOT/$s" --selftest 2>&1 | redact)" ; then passes=$((passes+1)); echo "  PASS  $s --selftest"
  else fails=$((fails+1)); echo "  FAIL  $s --selftest"; printf '%s\n' "$out" | tail -8 | sed 's/^/        /'; fi
done

# MUTATION GATE (2026-09-07 lane D1). The suite above proves the lease pager fires.
# This proves its CONTROLS are load-bearing: each gate is deleted in a copy of the real
# source and the matching control must flip RED. A control that survives its own
# mutation is agreeing, not working, which is the default failure shape per
# [[a-first-draft-control-usually-passes-for-the-wrong-reason-2026-09-06]]. Enrolled
# here because an artifact nothing runs is this repo's recurring defect.
if [ -f "$ROOT/tools/scheduler-lease-pager.test.js" ]; then
  echo "=== mutation gate ==="
  if out="$(node "$ROOT/tools/scheduler-lease-pager.test.js" --mutate 2>&1 | redact)"; then
    passes=$((passes+1)); echo "  PASS  scheduler-lease-pager --mutate"
  else
    fails=$((fails+1)); echo "  FAIL  scheduler-lease-pager --mutate"
    printf '%s\n' "$out" | tail -10 | sed 's/^/        /'
  fi
fi

echo "=== suites ==="
# lib/ was NOT in this glob until 2026-08-29, so a test living beside the module
# it guards was invisible to the commit gate. That is the enrolment half of the
# same failure this repo keeps hitting: the artifact exists, nothing runs it.
for t in "$ROOT"/tools/*.test.js "$ROOT"/lib/*.test.js "$ROOT"/daemons/*.test.js; do
  [ -f "$t" ] || continue
  run_bounded "$t" "${t#$ROOT/}"
done

echo "=== summary ==="
echo "pass=$passes fail=$fails timeout=$timeouts syntax_broken=$syntax_fails"
# BASELINE as of 2026-08-02 stage 1: FULLY GREEN. Any red is a regression - there is no
# longer a known-failing file to wave through.
#
# Repaired on the way here (each was red or actively misleading):
#   daemons/cred-refresher.test.js - was 5/9 red STRUCTURALLY. The daemon reads the macOS
#     Keychain first and a hardcoded ~/.claude.json, so on darwin the real live account
#     always beat the fixtures and results depended on which account the operator happened
#     to be on. Fixed with an injectable keychain reader plus CLAUDE_JSON_PATH, and the
#     suite now pins COORD_ROOT/SWITCH_LOCK_FILE itself so it is hermetic. Grew to 12
#     cases covering oauthAccount preservation, dead-snapshot skip, and switch-lock skip.
#   tools/usage.test.js + tools/coord.test.js - both hardcoded the Corazon-era
#     'D:\.code\EcodiaOS\coordination' and proxied fs to reroute it. That isolated nothing
#     unless the caller exported that exact Windows string: with COORD_ROOT unset they
#     wrote PRODUCTION substrate, with any other sandbox value they asserted against a
#     directory the module never wrote. Both now bind process.env.COORD_ROOT to their own
#     tmpdir before the require.
#   tools/input.test.js - printed 33/33 then never exited (require('./input') leaves a live
#     handle), wedging any runner that waits on it. Now exits explicitly.
#   tools/scheduler.test.js - one startupCleanup case still asserted the pre-2026-05-29-H2
#     contract (null the tab handle even when the close failed). Rewritten to pin both
#     branches of the current contract.
# The exit code IS the gate. (A prior line here referenced an unbound `$syntax_broken`
# with a malformed `:-0` default and, under `set -u`, aborted the script with exit 1 on
# EVERY run - green included - so any wrapper keying on the exit code saw perpetual
# failure. The var is `syntax_fails`; line 144 already uses it correctly.)
exit $(( (fails > 0 || syntax_fails > 0) ? 1 : 0 ))
