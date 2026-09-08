// Load .env early so SCHEDULER_ENABLED + DATABASE_URL + AGENT_TOKEN are
// available before any tool is required. Tolerant fallback when dotenv or
// the file is missing - env may still be set by the launch shell or PM2.
try {
  require('dotenv').config({ path: require('path').join(__dirname, '.env') })
} catch (_e) {
  // dotenv missing or file absent - env still functional via parent env
}

const express = require('express')
const cors = require('cors')
const os = require('os')
const fs = require('fs')
const path = require('path')

// ── crash-loop detector (2026-08-13 survival hardening) ──────────────────────
// Runs FIRST, before the risky tool autoload, so a boot that later dies mid-load
// is still counted. If N boots land in a short window (launchd respawning a
// failing boot every ~10s), fire ONE rate-limited page to Tate. Wrapped so the
// detector can never itself crash boot. See lib/crash-loop-detector.js.
try {
  const { recordBootAndDetect } = require('./lib/crash-loop-detector')
  const logDir = path.join(__dirname, 'logs')
  recordBootAndDetect({
    historyPath: path.join(logDir, 'boot-history.json'),
    stampPath: path.join(logDir, 'crashloop-page.stamp'),
  })
} catch (e) {
  console.error('[boot] crash-loop detector skipped (non-fatal):', e && e.message || e)
}

const app = express()
const PORT = process.env.AGENT_PORT || 7456
const TOKEN = process.env.AGENT_TOKEN || ''

app.use(cors())
app.use(express.json({ limit: '50mb' }))

app.use((err, req, res, next) => {
  if (err && (err.type === 'entity.parse.failed' || err instanceof SyntaxError)) {
    console.error(`[body-parser] rejected malformed JSON from ${req.ip || 'unknown'}: ${err.message}`)
    return res.status(400).json({ error: 'invalid JSON in request body', detail: err.message })
  }
  next(err)
})

function auth(req, res, next) {
  if (!TOKEN) return next()
  const header = req.headers.authorization || ''
  if (header === `Bearer ${TOKEN}`) return next()
  res.status(401).json({ error: 'Unauthorized' })
}

// Fault-tolerant tool autoload (2026-08-13): each tool file is required inside a
// per-file try/catch, so one bad file is skipped + logged instead of crashing
// boot into a silent launchd respawn loop. The test/harness skip filter is
// preserved inside the helper. See lib/tool-autoload.js.
const toolDir = path.join(__dirname, 'tools')
const { loadTools } = require('./lib/tool-autoload')
const { tools, failed: toolLoadFailures } = loadTools({ toolDir, logger: console })

app.get('/api/health', (_req, res) => {
  // 2026-07-17: surface the scheduler dispatch-loop liveness so an external probe
  // can distinguish "process green but loop wedged" (the silent-death fingerprint)
  // from a genuinely healthy loop. Best-effort: absent if the scheduler is off.
  let scheduler_health = null
  try {
    const scheduler = require('./tools/scheduler')
    if (typeof scheduler.schedulerHealthSnapshot === 'function') {
      scheduler_health = scheduler.schedulerHealthSnapshot()
    }
  } catch (e) { scheduler_health = { error: e.message } }
  res.json({
    status: 'ok',
    uptime: process.uptime(),
    platform: os.platform(),
    arch: os.arch(),
    memory: {
      total: os.totalmem(),
      free: os.freemem(),
      usedPercent: Math.round((1 - os.freemem() / os.totalmem()) * 100),
    },
    hostname: os.hostname(),
    scheduler_health,
    // 2026-08-13: expose skipped tool files so a canary can see a DEGRADED boot
    // (agent up, but one tool failed to load) rather than only up/down.
    // 2026-08-29: the content screen refuses script-shaped files BEFORE require,
    // and those are by design, not degradation. Keep them OUT of
    // tool_load_failures so the field still means "something broke" instead of
    // going permanently amber on a screened one-shot CLI, and surface them
    // separately so a wrongly-screened tool is still diagnosable from health.
    tool_load_failures: (toolLoadFailures || []).filter(f => !f.contentSkip).map(f => f.file),
    tool_content_skips: (toolLoadFailures || []).filter(f => f.contentSkip)
      .map(f => ({ file: f.file, reason: f.reason })),
  })
})

app.get('/api/info', auth, (_req, res) => {
  res.json({
    platform: os.platform(),
    arch: os.arch(),
    release: os.release(),
    hostname: os.hostname(),
    cpus: os.cpus().length,
    cpuModel: os.cpus()[0]?.model,
    memory: { total: os.totalmem(), free: os.freemem() },
    uptime: os.uptime(),
    homeDir: os.homedir(),
    tmpDir: os.tmpdir(),
    nodeVersion: process.version,
    user: os.userInfo().username,
    networkInterfaces: Object.fromEntries(
      Object.entries(os.networkInterfaces()).map(([name, addrs]) => [
        name,
        addrs.filter(a => !a.internal).map(a => ({ address: a.address, family: a.family })),
      ]).filter(([, addrs]) => addrs.length > 0)
    ),
    tools: Object.keys(tools),
  })
})

app.post('/api/tool', auth, async (req, res) => {
  const { tool, params = {} } = req.body
  if (!tool) return res.status(400).json({ error: 'Missing tool name' })

  const fn = tools[tool]
  if (!fn) return res.status(404).json({ error: `Unknown tool: ${tool}`, available: Object.keys(tools) })

  // Thread ctx into tool handlers that accept it (coord.* family). Falls back
  // gracefully for tools that ignore the second arg.
  const ctx = {
    tab_id: req.headers['x-tab-id'] || (params && params.tab_id),
    tab_credential: req.headers['x-tab-credential'] || (params && params.tab_credential),
  }

  try {
    const result = await fn(params, ctx)
    // 2026-06-01: ANY successful tool call from a registered worker tab
    // bumps that worker's last_heartbeat_at. This stops the sweep loop from
    // killing workers mid-task during long-running tool calls (Read, Bash,
    // db_execute) where the model wouldn't naturally call coord.heartbeat
    // between every step. Non-worker tabs are a no-op. Skip the coord.*
    // family to avoid double-write on the explicit heartbeat path.
    if (ctx.tab_id && !String(tool).startsWith('coord.')) {
      try {
        const coord = require('./tools/coord')
        if (typeof coord._touchHeartbeatForTab === 'function') {
          // 2026-09-09 lane coord-L1 VERIFY-2: pass the asserted credential so a
          // manufactured death can be revived from this route too. Omitting it
          // would silently make this the one path where proof of life is
          // discarded, which is the exact shape e7ab40c was fixing.
          coord._touchHeartbeatForTab(ctx.tab_id, ctx.tab_credential)
        }
      } catch (e) {}
    }
    res.json({ ok: true, result })
  } catch (err) {
    res.status(500).json({ error: err.message, tool })
  }
})

// Coord substrate: register-worker REST + /api/mcp/coord JSON-RPC shim
try {
  require('./routes/comms').mount(app, auth)
  require('./routes/mcpCoord').mount(app, auth)
  console.log('Coord routes mounted: /api/comms/register-worker + /api/mcp/coord')
} catch (e) {
  console.error('Coord routes failed to mount:', e.message)
}

// Cowork REST surface: POST /api/cowork/dispatch-worker.
// Wrapped defensively so a syntax error or missing dep in routes/cowork.js
// never takes down the laptop-agent startup. The observer_signal.py self-heal
// hook depends on this endpoint; failing-closed is the right posture.
try {
  require('./routes/cowork').mount(app, auth)
  console.log('Cowork route mounted: /api/cowork/dispatch-worker')
} catch (e) {
  console.error('Cowork route failed to mount:', e.message)
}

// Phase 8: manual CC chat dispatch with cred rotation.
//
// POST /api/scheduler/manual_chat  { "brief": "...", "preferred_account": "tate"? }
// Picks the healthiest account, atomically rotates ~/.claude/.credentials.json,
// then dispatches a new CC chat tab via cowork.dispatch_worker. Returns
// { ok, account, tab_id }. Use this instead of the IDE keybind when you want
// cred rotation to happen automatically before the new chat reads its tokens.
app.post('/api/scheduler/manual_chat', auth, async (req, res) => {
  try {
    const creds = require('./tools/creds')
    const cowork = require('./tools/cowork')
    const brief = (req.body && req.body.brief) || 'New manual chat (no brief provided).'
    const preferred = req.body && req.body.preferred_account
    const ide = (req.body && req.body.ide) || 'stable'
    const taskId = (req.body && req.body.task_id) || ('manual-' + Date.now())

    const account = await creds.pick_healthiest_account({
      preferred: preferred || null,
      required_headroom_minutes: 15,
    })
    await creds.rotate_to(account)
    const result = await cowork.dispatch_worker({ brief, task_id: taskId, ide, account })
    res.json({ ok: true, account, tab_id: result && result.tab_id, task_id: taskId })
  } catch (err) {
    res.status(500).json({ error: err.message || String(err), name: err.name || 'Error' })
  }
})

app.use((_req, res) => res.status(404).json({ error: 'Not found' }))

// ── caffeinate assertion (darwin, bound to agent lifetime) ───────────────────
//
// 2026-08-13 survival hardening (Tate overseas from ~2026-09-24). The laptop-agent
// plist carries NO caffeinate wrapper, and its KeepAlive only restarts on EXIT,
// not when the OS idle-sleeps and freezes the Node event loop (every setInterval
// in scheduler.start stops ticking; dispatch + recovery go dark). Doctrine:
// launchd-git-push-jobs-need-caffeinate-or-sleep-kills-fleet. Idle-sleep is
// currently disabled on this host, but this makes the no-idle-sleep guarantee
// STANDING and self-documenting, and survives a host settings drift.
//
// We spawn `caffeinate -i -w <this-pid>`: the -i idle assertion is held for as
// long as caffeinate runs, and -w binds it to THIS process's pid so caffeinate
// exits and releases the assertion the moment the agent dies - even on SIGKILL,
// where a JS exit handler would never run. Bound to lifetime by construction, no
// tracking/teardown to get wrong. (Critical-battery hibernate still overrides any
// assertion; keep the host on AC. That residual is unchanged.)
let _caffeinateChild = null
function startCaffeinate() {
  if (process.platform !== 'darwin') return
  try {
    const child = require('child_process').spawn(
      '/usr/bin/caffeinate', ['-i', '-w', String(process.pid)],
      { stdio: 'ignore', detached: false }
    )
    _caffeinateChild = child
    child.on('error', (e) => {
      console.error('[caffeinate] spawn error (sleep guarantee not held):', e && e.message || e)
    })
    child.on('exit', (code, sig) => {
      console.log(`[caffeinate] assertion released (exit code=${code} sig=${sig})`)
    })
    child.unref()
    console.log(`[caffeinate] idle-sleep assertion held for pid ${process.pid} (caffeinate pid ${child.pid})`)
  } catch (e) {
    console.error('[caffeinate] failed to start (sleep guarantee not held):', e && e.message || e)
  }
}

function onListening() {
  console.log(`EcodiaOS Laptop Agent running on :${PORT}`)
  console.log(`Tools loaded: ${Object.keys(tools).join(', ')}`)
  const toolLoadErrors = (toolLoadFailures || []).filter(f => !f.contentSkip)
  const toolContentSkips = (toolLoadFailures || []).filter(f => f.contentSkip)
  if (toolLoadErrors.length) {
    console.error(`Tools SKIPPED (${toolLoadErrors.length}, agent still up): ` +
      toolLoadErrors.map(f => f.file).join(', '))
  }
  if (toolContentSkips.length) {
    // Deliberate, not a fault: these are script-shaped files the content screen
    // refused BEFORE require. Logged at info so a boot-log reader hunting a
    // degraded boot is not sent down a false trail.
    console.log(`Tools content-screened (${toolContentSkips.length}, by design, not a fault): ` +
      toolContentSkips.map(f => `${f.file} [${f.reason}]`).join(', '))
  }
  console.log(`Auth: ${TOKEN ? 'enabled' : 'DISABLED (set AGENT_TOKEN)'}`)

  // Hold the idle-sleep assertion for the agent's whole lifetime (darwin only).
  startCaffeinate()

  // Autonomy substrate scheduler (Phase 3). Off by default - requires explicit
  // SCHEDULER_ENABLED=true + DATABASE_URL. Do NOT flip on until: (a) agent
  // restart picks up signal_bound; (b) code.json + money.json are seeded;
  // (c) Tate explicitly enables.
  if (process.env.SCHEDULER_ENABLED === 'true') {
    try {
      const scheduler = require('./tools/scheduler')
      // 2026-06-08 Mac-day-1: scheduler.js default dispatcher is cowork.js,
      // which is Windows-AHK-coupled. Inject mac-dispatcher when platform=darwin.
      if (process.platform === 'darwin') {
        const macDispatcher = require('./tools/mac-dispatcher')
        scheduler._setDispatcher(macDispatcher)
        console.log('Scheduler dispatcher: mac-dispatcher (darwin)')
      }
      scheduler.start()
      console.log('Scheduler started (autonomy substrate Phase 3)')
    } catch (e) {
      console.error('Scheduler failed to start:', e.message)
    }
  }
}

// ── EADDRINUSE handling (2026-08-13 survival hardening) ──────────────────────
// Without this, a stale/zombie prior instance holding :7456 makes app.listen throw
// an uncaught error -> process exit -> launchd (KeepAlive + ThrottleInterval=10)
// respawns every ~10s FOREVER, silent, same shape as the bad-require crash-loop.
// Handle it: try ONCE to reap a stale prior laptop-agent listener that is OUR node
// index.js, then rebind; if the port is still held (or held by a foreign process we
// must not kill), exit with a DISTINCT code (3) so the plist log + the fleet canary
// can alert, instead of the failure being invisible. Handoff: Hunter 4 #1.
const EXIT_PORT_HELD = 3
let _rebindAttempted = false

// Return the pid we reaped, or null. Only ever kills a process that is clearly OUR
// laptop-agent (a node process running index.js). Never kills a foreign holder.
function reapStaleListener(port) {
  try {
    const { execSync } = require('child_process')
    const out = execSync(`lsof -nP -iTCP:${port} -sTCP:LISTEN -t 2>/dev/null || true`,
      { encoding: 'utf8', timeout: 5000 }).trim()
    if (!out) return null
    const pids = out.split(/\s+/).map(s => parseInt(s, 10)).filter(n => n && n !== process.pid)
    for (const pid of pids) {
      let cmd = ''
      try { cmd = execSync(`ps -o command= -p ${pid} 2>/dev/null || true`, { encoding: 'utf8', timeout: 5000 }).trim() } catch (_e) {}
      const looksLikeAgent = /\bnode\b/.test(cmd) && /index\.js/.test(cmd)
      if (looksLikeAgent) {
        try { process.kill(pid, 'SIGTERM'); return pid } catch (_e) { return null }
      }
      console.error(`[listen] :${port} held by pid ${pid} (${cmd || 'unknown'}) which is NOT our agent; leaving it.`)
    }
    return null
  } catch (e) {
    console.error('[listen] stale-listener reap probe failed:', e && e.message || e)
    return null
  }
}

function startServer() {
  const server = app.listen(PORT, onListening)
  server.on('error', (err) => {
    if (err && err.code === 'EADDRINUSE') {
      console.error(`[listen] :${PORT} already in use (EADDRINUSE).`)
      if (!_rebindAttempted) {
        _rebindAttempted = true
        const reaped = reapStaleListener(PORT)
        if (reaped) {
          console.error(`[listen] reaped stale prior listener (pid ${reaped}); retrying bind in 1s`)
          setTimeout(startServer, 1000)
          return
        }
      }
      console.error(`[listen] could not bind :${PORT}; exiting ${EXIT_PORT_HELD} so launchd/canary can alert (not a silent 10s respawn).`)
      process.exit(EXIT_PORT_HELD)
    } else {
      console.error('[listen] fatal server error:', err && err.message || err)
      process.exit(1)
    }
  })
  return server
}

startServer()
