#!/usr/bin/env node
/**
 * coord-shouldwake.test - the wake test, checked against the build it replaced.
 *
 * WHY A MATCHED PAIR. shouldWake() was inverted from a closed notify_types
 * ALLOWLIST to a MACHINE_CHATTER DENYLIST on 2026-09-02 (lane D1). The failure
 * mode of that fix is not that it wakes too little in the abstract, it is that
 * it drops one of the four types a human actually depends on (worker_report,
 * error, inbound_sms, inbound_telegram) while every aggregate count improves.
 * A lone "the new build wakes on N messages" number cannot see that. So this
 * runs BOTH builds over the same corpus and asserts the asymmetry directly:
 * the fix may only ADD wakes, never remove one the old build delivered.
 *
 * The baseline is not a hand copy. It is read out of git at PRE_FIX_SHA, so it
 * stays honest after the fix lands and this file keeps meaning something.
 * Both builds are the REAL source text: the spans are extracted and evaluated
 * in a bare vm context rather than reimplemented here, because a reimplemented
 * mirror is the exact defect class this whole lane exists to close (see
 * SHOULD_WAKE_SHAPE in coord.js and the scanner that probes it).
 *
 * Usage: node tools/coord-shouldwake.test.cjs
 * Exit 0 all green, 1 on any failure. Needs the live coord message store for
 * the corpus half; the unit half runs anywhere.
 */
'use strict';
const fs = require('fs');
const os = require('os');
const path = require('path');
const vm = require('vm');
const { execFileSync } = require('child_process');

// The commit immediately before the denylist inversion. The pre-fix allowlist
// build is read from here, never retyped.
const PRE_FIX_SHA = 'b281294';
const REPO = path.resolve(__dirname, '..');
const SUBJECT = path.join(REPO, 'tools', 'coord.js');

function span(src, start, endTok) {
  const i = src.indexOf(start);
  if (i < 0) return null;
  const j = src.indexOf(endTok, i);
  if (j < 0) return null;
  return src.slice(i, j + endTok.length);
}

// Extract the real spans and evaluate them. Nothing here is a copy of the logic.
function build(src, label) {
  const parts = [
    span(src, 'const WAKE_TOPIC_PREFIXES', '\n'),
    span(src, 'const DEFAULT_WAKE_POLICY = Object.freeze({', '})\n'),
    span(src, 'const MACHINE_CHATTER = new Set([', '])\n'),
    span(src, 'function isWakeTopic(topic) {', '\n}\n'),
    span(src, 'function shouldWake(msg, policy) {', '\n}\n'),
  ].filter(Boolean);
  if (parts.length < 4) throw new Error(label + ': could not extract shouldWake and its dependencies');
  const shape = /const SHOULD_WAKE_SHAPE = '([a-z]+)'/.exec(src);
  const code = parts.join('\n') + '\nmodule.exports = { shouldWake: shouldWake };';
  const sandbox = { module: { exports: {} }, console: console };
  sandbox.exports = sandbox.module.exports;
  vm.createContext(sandbox);
  new vm.Script(code, { filename: label }).runInContext(sandbox);
  return { fn: sandbox.module.exports.shouldWake, shape: shape ? shape[1] : 'allowlist' };
}

let failures = 0;
function check(name, got, want) {
  const ok = got === want;
  if (!ok) failures++;
  console.log('  ' + (ok ? 'PASS' : 'FAIL') + '  ' + name + '  got=' + got + ' want=' + want);
}

function loadPolicy() {
  const p = path.join(os.homedir(), '.ecodiaos', 'coordination', 'wake_policy.json');
  try { return JSON.parse(fs.readFileSync(p, 'utf8')); }
  catch (e) { return { mode: 'auto_type', notify_types: ['worker_report', 'error', 'inbound_sms', 'inbound_telegram'] }; }
}

function main() {
  const baseSrc = execFileSync('git', ['-C', REPO, 'show', PRE_FIX_SHA + ':tools/coord.js'], { encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 });
  const OLD = build(baseSrc, 'pre-fix@' + PRE_FIX_SHA);
  const NEW = build(fs.readFileSync(SUBJECT, 'utf8'), 'deployed');
  console.log('baseline ' + PRE_FIX_SHA + ' shape=' + OLD.shape + '   subject shape=' + NEW.shape);

  const policy = loadPolicy();
  const msg = (type, to) => ({ to: to || 'chat.conductor.inbox', body: type === undefined ? {} : { type: type } });

  console.log('\n--- the four types a human depends on must survive the inversion ---');
  for (const t of ['worker_report', 'error', 'inbound_sms', 'inbound_telegram']) {
    check(t + ' wakes under the live policy', NEW.fn(msg(t), policy), true);
    check(t + ' wakes under the shipped DEFAULT policy', NEW.fn(msg(t), { mode: 'auto_type' }), true);
  }

  console.log('\n--- named machine chatter stays silent ---');
  for (const t of ['bound', 'done', 'progress', 'account_switch']) check(t, NEW.fn(msg(t), policy), false);

  console.log('\n--- invented escalation vocabulary: the whole point ---');
  for (const t of ['escalation', 'blocked_need_action', 'comms_health_surface', 'conductor_lane_surface', 'merge_ready', 'guardrail_flag', 'push_request', 'action_required']) {
    check('"' + t + '" wakes now', NEW.fn(msg(t), policy), true);
    check('"' + t + '" was LOST before', OLD.fn(msg(t), policy), false);
  }

  console.log('\n--- carve-outs that must survive unchanged ---');
  check('no type field cannot wake', NEW.fn(msg(undefined), policy), false);
  check('non-conductor topic ignored', NEW.fn(msg('error', 'chat.tab_x.inbox'), policy), false);
  check('scoped conductor topic wakes', NEW.fn(msg('error', 'chat.conductor.lane.inbox'), policy), true);
  check('mode=silent kills every wake', NEW.fn(msg('worker_report'), Object.assign({}, policy, { mode: 'silent' })), false);
  check('worker_report satisfies a legacy done subscription', NEW.fn(msg('worker_report'), { mode: 'auto_type', notify_types: ['done'] }), true);
  check('done does NOT satisfy a worker_report subscription (one-way)', NEW.fn(msg('done'), { mode: 'auto_type', notify_types: ['worker_report'] }), false);
  check('an explicit subscription outranks the denylist', NEW.fn(msg('bound'), { mode: 'auto_type', notify_types: ['bound'] }), true);
  check('["*"] still means everything', NEW.fn(msg('bound'), { mode: 'auto_type', notify_types: ['*'] }), true);

  console.log('\n--- matched pair over the live corpus, 14d on created_at ---');
  const MD = path.join(os.homedir(), '.ecodiaos', 'coordination', 'messages');
  if (!fs.existsSync(MD)) {
    console.log('  SKIP no coord message store at ' + MD);
  } else {
    const cut = Date.now() - 14 * 24 * 3600 * 1000;
    let n = 0, added = 0, regressed = 0;
    const addedTypes = Object.create(null);
    for (const ent of fs.readdirSync(MD, { withFileTypes: true })) {
      if (!ent.isFile()) continue;
      const fp = path.join(MD, ent.name);
      let st; try { st = fs.statSync(fp); } catch (e) { continue; }
      if (st.size === 0) continue;
      let m; try { m = JSON.parse(fs.readFileSync(fp, 'utf8')); } catch (e) { continue; }
      if (!m || String(m.to || '').indexOf('chat.conductor.') !== 0) continue;
      const ct = m.created_at ? Date.parse(m.created_at) : NaN;
      if (!((Number.isFinite(ct) ? ct : st.mtimeMs) >= cut)) continue;
      n++;
      const a = OLD.fn(m, policy), b = NEW.fn(m, policy);
      if (a === b) continue;
      if (b) { added++; const t = (m.body || {}).type; addedTypes[t] = (addedTypes[t] || 0) + 1; }
      else regressed++;
    }
    console.log('  scanned ' + n + '  newly waking ' + added + ' across ' + Object.keys(addedTypes).length + ' type(s)  regressed ' + regressed);
    console.log('  ' + JSON.stringify(addedTypes));
    check('the fix removes NO wake the old build delivered', regressed, 0);
  }

  console.log(failures ? '\nFAIL: ' + failures : '\nOK: all green');
  return failures ? 1 : 0;
}

process.exit(main());
