// chrome.js - portable Chrome / Chromium path resolver.
//
// Collapses the platform-branch logic currently embedded in tools/gui.js
// at lines 542, 667, and 706 (Windows-only User Data path + chrome.exe
// candidate search). Mac-day surfaced this gap on 2026-06-08: CDP work
// from the Mac mini has to hand-roll `~/Library/Application Support/`
// every time, and the existing enable_chrome_cdp helper has only the
// Windows arm.
//
// STUB STATUS: proposed/, not autoloaded. Move to tools/chrome.js +
// restart the agent to activate. Once active, gui.js should be patched
// to import from here rather than duplicate the platform logic.

const fs = require('fs')
const os = require('os')
const path = require('path')
const { spawnSync } = require('child_process')

const PLATFORM = process.platform

// User Data dir candidates by platform. First existing path wins.
function userDataCandidates() {
  if (PLATFORM === 'darwin') {
    return [
      path.join(os.homedir(), 'Library', 'Application Support', 'Google', 'Chrome'),
      path.join(os.homedir(), 'Library', 'Application Support', 'Chromium'),
    ]
  }
  if (PLATFORM === 'win32') {
    return [
      path.join(os.homedir(), 'AppData', 'Local', 'Google', 'Chrome', 'User Data'),
      path.join(os.homedir(), 'AppData', 'Local', 'Chromium', 'User Data'),
    ]
  }
  // linux
  return [
    path.join(os.homedir(), '.config', 'google-chrome'),
    path.join(os.homedir(), '.config', 'chromium'),
  ]
}

function executableCandidates() {
  if (PLATFORM === 'darwin') {
    return [
      '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
      '/Applications/Chromium.app/Contents/MacOS/Chromium',
      path.join(os.homedir(), 'Applications', 'Google Chrome.app', 'Contents', 'MacOS', 'Google Chrome'),
    ]
  }
  if (PLATFORM === 'win32') {
    return [
      'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
      'C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe',
      path.join(os.homedir(), 'AppData', 'Local', 'Google', 'Chrome', 'Application', 'chrome.exe'),
    ]
  }
  return ['/usr/bin/google-chrome', '/usr/bin/chromium-browser', '/usr/bin/chromium']
}

// Public tool: chrome.user_data_dir. Returns the platform-native Chrome
// User Data directory (the parent of `Default/`, `Profile 1/`, etc),
// honouring CHROME_USER_DATA_DIR env override + verifying existence on
// disk. Returns { path, exists, platform, profiles[] }.
exports.user_data_dir = async function ({ override } = {}) {
  const explicit = override || process.env.CHROME_USER_DATA_DIR
  if (explicit) {
    return { path: explicit, exists: fs.existsSync(explicit), platform: PLATFORM, profiles: listProfiles(explicit), source: 'override' }
  }
  for (const cand of userDataCandidates()) {
    if (fs.existsSync(cand)) {
      return { path: cand, exists: true, platform: PLATFORM, profiles: listProfiles(cand), source: 'auto' }
    }
  }
  return { path: null, exists: false, platform: PLATFORM, profiles: [], source: 'not-found', candidates: userDataCandidates() }
}

function listProfiles(userDataDir) {
  try {
    const entries = fs.readdirSync(userDataDir, { withFileTypes: true })
    return entries
      .filter((e) => e.isDirectory() && (e.name === 'Default' || /^Profile \d+$/.test(e.name)))
      .map((e) => e.name)
  } catch (_) {
    return []
  }
}

// Public tool: chrome.executable. Returns the path to the Chrome binary
// on this platform. Resolves via which/where as a final fallback.
exports.executable = async function () {
  for (const cand of executableCandidates()) {
    if (fs.existsSync(cand)) return { path: cand, source: 'candidate', platform: PLATFORM }
  }
  const which = spawnSync(PLATFORM === 'win32' ? 'where' : 'which', [PLATFORM === 'darwin' ? 'Google Chrome' : 'google-chrome'], { encoding: 'utf8' })
  if (which.status === 0 && which.stdout.trim()) {
    return { path: which.stdout.trim().split(/\r?\n/)[0], source: 'PATH', platform: PLATFORM }
  }
  return { path: null, source: 'not-found', platform: PLATFORM, candidates: executableCandidates() }
}

// Public tool: chrome.cdp_launch_args. Returns the canonical arg array
// for launching Chrome with CDP on :9222. Mirrors what
// gui.enable_chrome_cdp uses, but exposed as a primitive so other tools
// can compose it (e.g. a Mac analogue of enable_chrome_cdp).
exports.cdp_launch_args = async function ({ port = 9222, userDataDir, profile = 'Default' } = {}) {
  const ud = userDataDir || (await exports.user_data_dir({})).path
  if (!ud) throw new Error('chrome.cdp_launch_args: no user-data-dir found, pass override')
  return {
    args: [
      `--remote-debugging-port=${port}`,
      '--remote-allow-origins=*',
      `--user-data-dir=${ud}`,
      `--profile-directory=${profile}`,
      '--restore-last-session',
      '--no-first-run',
      '--no-default-browser-check',
    ],
    user_data_dir: ud,
    profile,
    port,
  }
}
