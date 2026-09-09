// ide.js - focusless IDE control via the in-IDE HTTP bridge.
//
// The Chrome-CDP equivalent for VS Code (Stable / Insiders / Cursor). Each
// running IDE registers itself in ~/.ecodia-preview/instances.json with a
// per-process HTTP port. This module finds the right instance and proxies
// requests to its bridge, never touching window focus. Parallel calls into
// different IDE instances are safe because each instance has its own port.
//
// Picking an instance: pass {ide:'stable'|'insiders'|'cursor'} OR {pid:N} OR
// {workspace:'<path-substring>'}. If nothing is passed, defaults to the
// single running instance, or errors with the available list if ambiguous.
//
// Companion to cursor-preview-extension/ide-bridge.js. Keyboard-driven
// vscode.* / cursor.* tools stay as a fallback when the extension is not
// installed or the IDE isn't registered.

const fs = require('fs');
const path = require('path');
const os = require('os');
const http = require('http');

const REGISTRY = path.join(os.homedir(), '.ecodia-preview', 'instances.json');

const IDE_APPNAME_MATCH = {
  stable: /^Visual Studio Code$/i,
  insiders: /Insiders/i,
  cursor: /^Cursor$/i,
};

function readRegistry() {
  try { return JSON.parse(fs.readFileSync(REGISTRY, 'utf8')) || {}; }
  catch { return {}; }
}

function listInstances() {
  const reg = readRegistry();
  return Object.entries(reg).map(([pid, info]) => ({
    pid: Number(pid),
    port: info.port,
    ide: info.ide,
    workspaceRoots: info.workspaceRoots || [],
    startedAt: info.startedAt,
    alive: (() => {
      try { process.kill(Number(pid), 0); return true; } catch { return false; }
    })(),
  })).filter(i => i.alive);
}

function pickInstance(opts) {
  opts = opts || {};
  const instances = listInstances();
  if (instances.length === 0) {
    throw new Error('no IDE instances registered. The ecodia-preview extension must be installed and the IDE running. See backend/laptop-agent/cursor-preview-extension/install.ps1.');
  }
  let pool = instances;
  if (opts.pid) pool = pool.filter(i => i.pid === Number(opts.pid));
  if (opts.ide) {
    const re = IDE_APPNAME_MATCH[String(opts.ide).toLowerCase()];
    if (re) pool = pool.filter(i => re.test(i.ide || ''));
  }
  if (opts.workspace) {
    const needle = String(opts.workspace).toLowerCase().replace(/\\/g, '/');
    pool = pool.filter(i => (i.workspaceRoots || []).some(r => r.toLowerCase().replace(/\\/g, '/').includes(needle)));
  }
  if (pool.length === 0) {
    throw new Error('no IDE instance matched filters. Available: ' + JSON.stringify(instances.map(i => ({ ide: i.ide, pid: i.pid, port: i.port, workspaceRoots: i.workspaceRoots }))));
  }
  return pool[0];
}

function httpCall(port, method, route, body, timeoutMs) {
  return new Promise((resolve, reject) => {
    const data = body == null ? null : Buffer.from(JSON.stringify(body), 'utf8');
    const req = http.request({
      host: '127.0.0.1',
      port,
      path: route,
      method,
      headers: {
        'Content-Type': 'application/json',
        ...(data ? { 'Content-Length': data.length } : {}),
      },
      timeout: timeoutMs || 30000,
    }, res => {
      let chunks = '';
      res.on('data', c => chunks += c);
      res.on('end', () => {
        let parsed;
        try { parsed = JSON.parse(chunks); }
        catch { parsed = { ok: false, raw: chunks, status: res.statusCode }; }
        if (res.statusCode >= 400) {
          return reject(new Error('ide-bridge ' + method + ' ' + route + ' -> ' + res.statusCode + ': ' + (parsed && parsed.error || chunks)));
        }
        resolve(parsed);
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('ide-bridge timeout after ' + (timeoutMs || 30000) + 'ms')); });
    if (data) req.write(data);
    req.end();
  });
}

async function call(opts, method, route, body, timeoutMs) {
  const inst = pickInstance(opts);
  const result = await httpCall(inst.port, method, route, body, timeoutMs);
  return { ide: inst.ide, pid: inst.pid, port: inst.port, ...result };
}

// ===== exported tools ====================================================

// ide.list_instances - registry dump (no IDE call)
async function listAll() {
  const instances = listInstances();
  return { ok: true, count: instances.length, instances };
}

// ide.info {ide?, pid?, workspace?}
async function info(params) {
  return call(params, 'GET', '/ide/info');
}

// ide.routes - what does the bridge support?
async function routes(params) {
  return call(params, 'GET', '/ide/routes');
}

// ----- commands ---------------------------------------------------------

// ide.command {cmd, args?, returnResult?, ...filters}
async function command(params) {
  params = params || {};
  if (!params.cmd) throw new Error('cmd required');
  return call(params, 'POST', '/ide/command', {
    cmd: params.cmd,
    args: params.args,
    returnResult: params.returnResult,
  });
}

// ide.commands_list {filter?, internal?, ...filters}
async function commandsList(params) {
  params = params || {};
  const q = new URLSearchParams();
  if (params.filter) q.set('filter', params.filter);
  if (params.internal) q.set('internal', 'true');
  return call(params, 'GET', '/ide/commands?' + q.toString());
}

// ----- workspace --------------------------------------------------------

async function folders(params) {
  return call(params, 'GET', '/ide/workspace/folders');
}

async function documents(params) {
  return call(params, 'GET', '/ide/workspace/documents');
}

async function findFiles(params) {
  if (!params || !params.pattern) throw new Error('pattern required');
  return call(params, 'POST', '/ide/workspace/find', {
    pattern: params.pattern,
    exclude: params.exclude,
    max: params.max,
  });
}

async function fsRead(params) {
  if (!params || !params.path) throw new Error('path required');
  return call(params, 'POST', '/ide/workspace/fs/read', {
    path: params.path,
    encoding: params.encoding,
  });
}

async function fsWrite(params) {
  if (!params || !params.path || typeof params.content !== 'string') throw new Error('path and content required');
  return call(params, 'POST', '/ide/workspace/fs/write', {
    path: params.path,
    content: params.content,
    encoding: params.encoding,
  });
}

async function fsStat(params) {
  if (!params || !params.path) throw new Error('path required');
  return call(params, 'POST', '/ide/workspace/fs/stat', { path: params.path });
}

async function fsList(params) {
  if (!params || !params.path) throw new Error('path required');
  return call(params, 'POST', '/ide/workspace/fs/list', { path: params.path });
}

async function fsMkdir(params) {
  if (!params || !params.path) throw new Error('path required');
  return call(params, 'POST', '/ide/workspace/fs/mkdir', { path: params.path });
}

async function fsDelete(params) {
  if (!params || !params.path) throw new Error('path required');
  return call(params, 'POST', '/ide/workspace/fs/delete', {
    path: params.path,
    recursive: params.recursive,
    useTrash: params.useTrash,
  });
}

async function fsRename(params) {
  if (!params || !params.oldPath || !params.newPath) throw new Error('oldPath and newPath required');
  return call(params, 'POST', '/ide/workspace/fs/rename', {
    oldPath: params.oldPath,
    newPath: params.newPath,
    overwrite: params.overwrite,
  });
}

// ide.edit {edits: [{path, range, newText, kind?}], save?, ...filters}
async function edit(params) {
  if (!params || !Array.isArray(params.edits) || params.edits.length === 0) throw new Error('edits[] required');
  return call(params, 'POST', '/ide/workspace/edit', {
    edits: params.edits,
    save: params.save,
  });
}

// ----- editor / window --------------------------------------------------

async function activeEditor(params) {
  params = params || {};
  const q = new URLSearchParams();
  if (params.includeText === false) q.set('includeText', 'false');
  if (params.maxBytes) q.set('maxBytes', String(params.maxBytes));
  return call(params, 'GET', '/ide/window/active-editor?' + q.toString());
}

async function visibleEditors(params) {
  return call(params, 'GET', '/ide/window/visible-editors');
}

// ide.show {path, viewColumn?, preview?, preserveFocus?, selection?}
async function show(params) {
  if (!params || !params.path) throw new Error('path required');
  return call(params, 'POST', '/ide/window/show', {
    path: params.path,
    viewColumn: params.viewColumn,
    preview: params.preview,
    preserveFocus: params.preserveFocus,
    selection: params.selection,
  });
}

// ide.edit_active {edits: [{range, newText, kind?}], save?}
async function editActive(params) {
  if (!params || !Array.isArray(params.edits) || params.edits.length === 0) throw new Error('edits[] required');
  return call(params, 'POST', '/ide/window/edit-active', {
    edits: params.edits,
    save: params.save,
  });
}

async function selection(params) {
  if (!params || !Array.isArray(params.selections) || params.selections.length === 0) throw new Error('selections[] required');
  return call(params, 'POST', '/ide/window/selection', {
    selections: params.selections,
    reveal: params.reveal,
  });
}

async function tabs(params) {
  return call(params, 'GET', '/ide/window/tabs');
}

async function tabsClose(params) {
  return call(params, 'POST', '/ide/window/tabs/close', params || {});
}

// ide.chat_send_message {prompt, session?, submit?, ...filters}
//
// Focusless single-call replacement for the dispatch_worker keystroke chain.
// The bridge runs the open + populate + submit + tab-diff sequence on a single
// extension-host tick. Returns the new chat tab's {label, viewColumn, index,
// viewType} which the caller can store as a tab_handle for the close path.
//
// 2026-05-29: replaces clipboard.write + input.shortcut[ctrl,v] + input.key[enter]
// in cowork.dispatch_worker. Collapses the 2.5s focus-dependent paste window
// to ~0ms. See ide-bridge.js /ide/chat/send_message route for full details.
async function chatSendMessage(params) {
  if (!params || !params.prompt) throw new Error('prompt required');
  return call(params, 'POST', '/ide/chat/send_message', {
    prompt: params.prompt,
    session: params.session,
    submit: params.submit,
  });
}

// ----- terminals --------------------------------------------------------

async function terminalsList(params) {
  return call(params, 'GET', '/ide/window/terminals');
}

async function terminalCreate(params) {
  if (!params || !params.name) throw new Error('name required');
  return call(params, 'POST', '/ide/window/terminals/create', {
    name: params.name,
    cwd: params.cwd,
    shellPath: params.shellPath,
    shellArgs: params.shellArgs,
    env: params.env,
    show: params.show,
    preserveFocus: params.preserveFocus,
  });
}

async function terminalSend(params) {
  if (!params || !params.name) throw new Error('name required');
  return call(params, 'POST', '/ide/window/terminals/send', {
    name: params.name,
    text: params.text,
    addNewLine: params.addNewLine,
  });
}

async function terminalShow(params) {
  if (!params || !params.name) throw new Error('name required');
  return call(params, 'POST', '/ide/window/terminals/show', {
    name: params.name,
    preserveFocus: params.preserveFocus,
  });
}

async function terminalDispose(params) {
  if (!params || !params.name) throw new Error('name required');
  return call(params, 'POST', '/ide/window/terminals/dispose', { name: params.name });
}

// ----- diagnostics ------------------------------------------------------

async function diagnostics(params) {
  params = params || {};
  const q = new URLSearchParams();
  if (params.uri) q.set('uri', params.uri);
  return call(params, 'GET', '/ide/diagnostics?' + q.toString());
}

// ----- env --------------------------------------------------------------

async function clipboardRead(params) {
  return call(params, 'GET', '/ide/env/clipboard');
}

async function clipboardWrite(params) {
  return call(params, 'POST', '/ide/env/clipboard', { text: (params && params.text) || '' });
}

async function openExternal(params) {
  if (!params || (!params.uri && !params.path)) throw new Error('uri or path required');
  return call(params, 'POST', '/ide/env/open', { uri: params.uri, path: params.path });
}

// ide.message {message, level?, items?}
async function message(params) {
  if (!params || !params.message) throw new Error('message required');
  return call(params, 'POST', '/ide/window/message', {
    message: params.message,
    level: params.level,
    items: params.items,
  });
}

module.exports = {
  list_instances: listAll,
  info,
  routes,
  command,
  commands_list: commandsList,
  folders,
  documents,
  find_files: findFiles,
  fs_read: fsRead,
  fs_write: fsWrite,
  fs_stat: fsStat,
  fs_list: fsList,
  fs_mkdir: fsMkdir,
  fs_delete: fsDelete,
  fs_rename: fsRename,
  edit,
  active_editor: activeEditor,
  visible_editors: visibleEditors,
  show,
  edit_active: editActive,
  selection,
  tabs,
  tabs_close: tabsClose,
  chat_send_message: chatSendMessage,
  terminals_list: terminalsList,
  terminal_create: terminalCreate,
  terminal_send: terminalSend,
  terminal_show: terminalShow,
  terminal_dispose: terminalDispose,
  diagnostics,
  clipboard_read: clipboardRead,
  clipboard_write: clipboardWrite,
  open_external: openExternal,
  message,
};
