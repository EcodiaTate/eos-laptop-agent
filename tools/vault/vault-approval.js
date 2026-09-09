'use strict'
// tools/vault/vault-approval.js - the approval + wake + fallback state machine.
//
// The problem Tate named: I fire an approval request, then go to sleep. He approves HOURS
// later. If nothing re-awakens me, the task dies and I never even know he approved. And if
// he can't get to it in time, someone else (his mum Helen) must be able to approve.
//
// So every request:
//   1. records a pending approval with an urgency-based primary deadline,
//   2. notifies the primary approver (Tate),
//   3. arms a ONE-SHOT escalation timer at the deadline (fallback -> Helen),
// and when ANY approver approves (the phone posts the signed result, the puller calls
// approve()), it arms a ONE-SHOT WAKE that re-opens the conductor with the task's
// continuation, so I resume the instant the approval lands instead of stalling.
//
// Wake + escalation are one-shot os_scheduled_tasks rows (next_run_at = run_at, per the
// direct-insert trap). Notifications are injected so the state machine is deterministically
// testable; the CLI wires real SMS.
const lib = require('/Users/ecodia/.code/ecodiaos/backend/continuity/lib.cjs')

const URGENCY_MIN = { quick: 15, normal: 120, low: 720 }   // primary window, minutes
const APPROVAL_TOOL = '/Users/ecodia/.code/eos-laptop-agent/tools/vault/vault-approval.js'

function deadlineFrom(now, urgency) {
  const mins = URGENCY_MIN[urgency] || URGENCY_MIN.normal
  return new Date(now.getTime() + mins * 60_000)
}

// Arm a one-shot scheduled task the Mac laptop-agent will lease + open as a fresh CC tab.
async function armTask(db, name, prompt, whenIso, priority) {
  await db`INSERT INTO os_scheduled_tasks (type, name, prompt, run_at, next_run_at, status, max_runs, session_mode, priority)
           VALUES ('delayed', ${name}, ${prompt}, ${whenIso}, ${whenIso}, 'active', 1, 'inherit_fork', ${priority || 3})`
}

async function request(db, { service, action, urgency = 'normal', continuation, replyTo }, notify, now = new Date()) {
  if (!service || !action || !continuation) throw new Error('request needs {service, action, continuation}')
  const primary = deadlineFrom(now, urgency)
  // replyTo = the coord topic of the session that should resume when approved. Default the
  // persistent conductor, so an approved result lands in the live chat, not a fresh tab. A
  // scheduled worker passes its own tab inbox topic.
  const reply = replyTo || 'chat.conductor.inbox'
  const row = (await db`INSERT INTO public.vault_approvals (service, action, urgency, continuation, primary_deadline, reply_to)
                        VALUES (${service}, ${action}, ${urgency}, ${continuation}, ${primary.toISOString()}, ${reply}) RETURNING id`)[0]
  await notify('tate', `Approval needed: ${action} for ${service} (${urgency}). Open Friend to approve. ref ${row.id.slice(0, 8)}`)
  // arm the escalation-to-Helen timer at the primary deadline
  await armTask(db, `cowork.vault-escalate.${row.id}`,
    `A vault approval may be overdue. Run: node ${APPROVAL_TOOL} escalate ${row.id}  then follow its output (it notifies the fallback approver Helen only if still pending).`,
    primary.toISOString(), 2)
  return { id: row.id, primary_deadline: primary.toISOString() }
}

// Inject a continuation into the RUNNING conductor session via the coord inbox (the same
// path inbound SMS/Telegram use to land as a turn - reflex/coord, no new tab). Returns true
// if the coord server accepted the write. Best-effort; the deadman wake is the fallback.
async function injectToConductor(topic, body) {
  const fs = require('fs')
  let token = ''
  try { token = ((fs.readFileSync('/Users/ecodia/.code/eos-laptop-agent/.env', 'utf8').match(/AGENT_TOKEN=(.+)/) || [])[1] || '').trim() } catch (_e) {}
  const payload = JSON.stringify({ tool: 'coord.send_message', params: { to: topic, body } })
  return await new Promise((resolve) => {
    const req = require('http').request({ host: '127.0.0.1', port: 7456, path: '/api/tool', method: 'POST',
      headers: { 'content-type': 'application/json', 'authorization': 'Bearer ' + token, 'content-length': Buffer.byteLength(payload) } },
      (res) => { let s = ''; res.on('data', c => s += c); res.on('end', () => resolve(res.statusCode === 200 && !/error/i.test(s))) })
    req.on('error', () => resolve(false)); req.write(payload); req.end()
  })
}

// Called by the puller when a signed approval result lands. Marks approved, then INJECTS the
// continuation into the running session that requested it (no new tab); a deadman new-tab wake
// is armed only as a fallback for when that session has already ended.
async function approve(db, requestId, by = 'tate', now = new Date()) {
  const rows = await db`SELECT id, service, action, continuation, status, reply_to FROM public.vault_approvals WHERE id = ${requestId}`
  if (!rows.length) throw new Error('no such approval ' + requestId)
  const a = rows[0]
  if (a.status === 'approved') return { id: a.id, already: true }
  await db`UPDATE public.vault_approvals SET status='approved', approver=${by}, resolved_at=now() WHERE id=${requestId}`
  const PULL_TOOL = APPROVAL_TOOL.replace('vault-approval.js', 'vault-pull.js')
  const cont = `A vault approval was APPROVED by ${by} (${a.action} for ${a.service}). First run: node ${PULL_TOOL} to apply the returned data. Then CONTINUE:\n\n${a.continuation}\n\nWhen done, mark it handled so the fallback wake stands down: node ${APPROVAL_TOOL} handled ${a.id}`

  // PRIMARY: inject into the running session that requested this (default = the conductor).
  const topic = a.reply_to || 'chat.conductor.inbox'
  const injected = await injectToConductor(topic, { type: 'done', task_id: a.id, summary: `vault approval approved: ${a.action} for ${a.service}`, text: cont })

  // FALLBACK: a deadman new-tab wake. It stands down if the running session handled the inject
  // (handled_in_session=true); it only re-opens a fresh tab when the session was already gone.
  // Give an injected session longer before the deadman checks (it needs time to act).
  const wakeAt = new Date(now.getTime() + (injected ? 8 : 1) * 60_000).toISOString()
  await armTask(db, `cowork.vault-wake.${a.id}`,
    `Vault approval ${a.id.slice(0, 8)} follow-up. FIRST check whether the running session already handled it:\n  node -e "const l=require('/Users/ecodia/.code/ecodiaos/backend/continuity/lib.cjs');const db=l.db();db\\\`SELECT handled_in_session h FROM public.vault_approvals WHERE id='${a.id}'\\\`.then(r=>{console.log(r[0]&&r[0].h?'HANDLED':'UNHANDLED');process.exit(0)})"\nIf it prints HANDLED, this was already done in the live session - run coord.close_my_tab and STOP. If UNHANDLED, continue: run node ${PULL_TOOL} then:\n\n${a.continuation}`,
    wakeAt, 1)
  return { id: a.id, approved_by: by, injected, wake_at: wakeAt }
}

// Fired by the escalation timer at the primary deadline. Only acts if still pending.
async function escalate(db, requestId, notify, now = new Date()) {
  const rows = await db`SELECT id, service, action, urgency, status FROM public.vault_approvals WHERE id = ${requestId}`
  if (!rows.length) throw new Error('no such approval ' + requestId)
  const a = rows[0]
  if (a.status !== 'pending') return { id: a.id, noop: a.status }   // already approved/expired
  const fb = deadlineFrom(now, a.urgency)
  await db`UPDATE public.vault_approvals SET status='escalated', fallback_deadline=${fb.toISOString()} WHERE id=${requestId}`
  await notify('helen', `Ecodia needs an approval Tate hasn't actioned: ${a.action} for ${a.service}. Open Friend to approve for him. ref ${a.id.slice(0, 8)}`)
  await armTask(db, `cowork.vault-expire.${a.id}`,
    `A vault approval escalated to the fallback approver may now be expired. Run: node ${APPROVAL_TOOL} expire ${a.id}  and follow its output.`,
    fb.toISOString(), 2)
  return { id: a.id, escalated_to: 'helen', fallback_deadline: fb.toISOString() }
}

// The live session calls this once it has resumed + finished the continuation, so the deadman
// new-tab wake stands down instead of re-doing the work in a fresh tab.
async function markHandled(db, requestId) {
  await db`UPDATE public.vault_approvals SET handled_in_session = true WHERE id = ${requestId}`
  await db`UPDATE os_scheduled_tasks SET status='cancelled', archived_at=now() WHERE name = ${'cowork.vault-wake.' + requestId} AND status='active'`
  return { id: requestId, handled: true }
}

// Fired by the fallback timer. If still unresolved, expire + tell Tate it lapsed.
async function expire(db, requestId, notify) {
  const rows = await db`SELECT id, service, action, status FROM public.vault_approvals WHERE id = ${requestId}`
  if (!rows.length) throw new Error('no such approval ' + requestId)
  const a = rows[0]
  if (a.status === 'approved') return { id: a.id, noop: 'approved' }
  await db`UPDATE public.vault_approvals SET status='expired', resolved_at=now() WHERE id=${requestId}`
  await notify('tate', `Approval lapsed unactioned: ${a.action} for ${a.service}. I did NOT proceed. ref ${a.id.slice(0, 8)}`)
  return { id: a.id, expired: true }
}

// Real delivery. Tate is reachable by iMessage (text-tate.js: pure osascript, zero budget,
// works under any cap) - the canonical Tate channel, NOT Twilio. Helen is on ANDROID so
// iMessage cannot reach her; her channel is the Friend app push once paired, SMS only as a
// fallback if the comms Twilio path is healthy. Never a silent swallow.
const { execFileSync } = require('child_process')
async function notifyDefault(target, msg) {
  if (target === 'tate') {
    try {
      // process.execPath, NOT 'node'. vault-pull runs from launchd, whose default PATH
      // is /usr/bin:/bin:/usr/sbin:/sbin and holds no node (ours is /opt/homebrew/bin/node).
      // A bare 'node' ENOENTed here on every launchd fire, so the catch below logged a
      // failure and returned via:'failed' and Tate was never texted. The comment above
      // says never a silent swallow; under launchd this was one.
      execFileSync(process.execPath, ['/Users/ecodia/.code/ecodiaos/backend/imessage-agent/text-tate.js', '--from', 'vault-approval', msg], { stdio: 'ignore' })
      return { via: 'imessage' }
    } catch (e) { console.log(`[notify:tate iMessage FAILED: ${e.message}] ${msg}`); return { via: 'failed' } }
  }
  console.log(`\n>>> NOTIFY HELEN (Android - Friend app push once paired; SMS fallback if comms healthy):\n${msg}\n`)
  return { via: 'surfaced' }
}

module.exports = { request, approve, escalate, expire, markHandled, injectToConductor, deadlineFrom, armTask, notifyDefault }

if (require.main === module) {
  const [cmd, a1, a2] = process.argv.slice(2)
  const db = lib.db()
  const smsNotify = notifyDefault
  ;(async () => {
    if (cmd === 'escalate') console.log(JSON.stringify(await escalate(db, a1, smsNotify), null, 2))
    else if (cmd === 'expire') console.log(JSON.stringify(await expire(db, a1, smsNotify), null, 2))
    else if (cmd === 'approve') console.log(JSON.stringify(await approve(db, a1, a2 || 'tate'), null, 2))
    else if (cmd === 'handled') console.log(JSON.stringify(await markHandled(db, a1), null, 2))
    else console.log('usage: vault-approval.js escalate <id> | expire <id> | approve <id> [by] | handled <id>')
    process.exit(0)
  })().catch(e => { console.error('ERR', e.message); process.exit(1) })
}
