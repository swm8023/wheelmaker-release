import { createHash, randomBytes, randomUUID } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { spawn } from 'node:child_process';
import {
  access,
  chmod,
  cp,
  mkdir,
  open,
  readFile,
  rename,
  rm,
  writeFile,
} from 'node:fs/promises';
import { Readable } from 'node:stream';
import { createGunzip } from 'node:zlib';
import { dirname, isAbsolute, join, resolve, sep } from 'node:path';
import { homedir } from 'node:os';

const BLOCK_SIZE = 512;
const DEFAULT_MAX_ENTRIES = 20_000;
const DEFAULT_MAX_FILE_BYTES = 512 * 1024 * 1024;
const DEFAULT_MAX_CONTENT_BYTES = 2 * 1024 * 1024 * 1024;
const STALE_LEASE_MS = 2 * 60 * 60 * 1000;
const STATUS_STATES = new Set([
  'applying',
  'downloading',
  'failed',
  'queued',
  'restarting',
  'succeeded',
  'verifying',
]);
const TERMINAL_STATES = new Set(['failed', 'succeeded']);

function jsonBytes(value) {
  return Buffer.from(`${JSON.stringify(value, null, 2)}\n`, 'utf8');
}

function sha256Bytes(bytes) {
  return createHash('sha256').update(bytes).digest('hex');
}

function validateJobId(jobId) {
  if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(jobId ?? '')) {
    throw new Error(`invalid update job ID: ${jobId}`);
  }
}

function normalizeTime(value) {
  const milliseconds = Date.parse(value);
  if (!Number.isFinite(milliseconds)) {
    throw new Error(`invalid update timestamp: ${value}`);
  }
  return new Date(milliseconds).toISOString();
}

async function readJsonIfPresent(path) {
  try {
    return JSON.parse(await readFile(path, 'utf8'));
  } catch (error) {
    if (error?.code === 'ENOENT') return null;
    throw error;
  }
}

async function atomicWrite(path, bytes, mode = 0o600) {
  await mkdir(dirname(path), { recursive: true });
  const temporaryPath = join(
    dirname(path),
    `.${randomUUID()}.${process.pid}.tmp`,
  );
  await writeFile(temporaryPath, bytes, { mode });
  try {
    await rename(temporaryPath, path);
  } finally {
    await rm(temporaryPath, { force: true });
  }
}

function statusRecord(status, existing) {
  validateJobId(status.jobId);
  if (!STATUS_STATES.has(status.state)) {
    throw new Error(`invalid update state: ${status.state}`);
  }
  const updatedAt = normalizeTime(status.now ?? new Date().toISOString());
  const startedAt = normalizeTime(
    status.startedAt ??
      (existing?.jobId === status.jobId ? existing.startedAt : updatedAt),
  );
  return {
    schema: 1,
    jobId: status.jobId,
    state: status.state,
    startedAt,
    updatedAt,
    ...(status.version ? { version: status.version } : {}),
    ...(status.errorCode ? { errorCode: status.errorCode } : {}),
  };
}

export async function writeUpdateStatus(stateDirectory, status) {
  const statusPath = join(stateDirectory, 'status.json');
  const existing = await readJsonIfPresent(statusPath);
  const record = statusRecord(status, existing);
  await atomicWrite(statusPath, jsonBytes(record));
  return record;
}

async function tryCreateLease(stateDirectory, lease) {
  const lockPath = join(stateDirectory, 'lock.json');
  let handle;
  try {
    handle = await open(lockPath, 'wx', 0o600);
  } catch (error) {
    if (error?.code === 'EEXIST') return false;
    throw error;
  }

  const startedAt = normalizeTime(lease.now ?? new Date().toISOString());
  const lock = {
    schema: 1,
    jobId: lease.jobId,
    owner: lease.owner,
    state: 'queued',
    startedAt,
    heartbeatAt: startedAt,
  };
  try {
    await handle.writeFile(jsonBytes(lock));
  } finally {
    await handle.close();
  }
  try {
    await writeUpdateStatus(stateDirectory, {
      jobId: lease.jobId,
      now: startedAt,
      startedAt,
      state: 'queued',
    });
  } catch (error) {
    await rm(lockPath, { force: true });
    throw error;
  }
  return true;
}

export async function acquireUpdateLease(
  stateDirectory,
  lease,
  { isUpdaterRunning = async () => false } = {},
) {
  validateJobId(lease.jobId);
  if (lease.owner !== 'web' && lease.owner !== 'timer') {
    throw new Error(`invalid update lease owner: ${lease.owner}`);
  }
  await mkdir(stateDirectory, { recursive: true });
  if (await tryCreateLease(stateDirectory, lease)) {
    return true;
  }

  const lockPath = join(stateDirectory, 'lock.json');
  const existing = await readJsonIfPresent(lockPath);
  if (!existing) {
    return tryCreateLease(stateDirectory, lease);
  }
  const nowMilliseconds = Date.parse(lease.now ?? new Date().toISOString());
  const heartbeatMilliseconds = Date.parse(existing.heartbeatAt ?? '');
  if (
    !Number.isFinite(nowMilliseconds) ||
    !Number.isFinite(heartbeatMilliseconds) ||
    nowMilliseconds - heartbeatMilliseconds <= STALE_LEASE_MS ||
    (await isUpdaterRunning())
  ) {
    return false;
  }

  const claimedPath = join(
    stateDirectory,
    `.stale-lock.${process.pid}.${randomUUID()}.json`,
  );
  try {
    await rename(lockPath, claimedPath);
  } catch (error) {
    if (error?.code === 'ENOENT') {
      return tryCreateLease(stateDirectory, lease);
    }
    return false;
  }
  try {
    return await tryCreateLease(stateDirectory, lease);
  } finally {
    await rm(claimedPath, { force: true });
  }
}

export async function heartbeatUpdateLease(
  stateDirectory,
  jobId,
  now = new Date().toISOString(),
) {
  validateJobId(jobId);
  const lockPath = join(stateDirectory, 'lock.json');
  const lock = await readJsonIfPresent(lockPath);
  if (!lock || lock.jobId !== jobId) {
    return false;
  }
  lock.heartbeatAt = normalizeTime(now);
  await atomicWrite(lockPath, jsonBytes(lock));
  return true;
}

export async function finishUpdate(stateDirectory, status) {
  if (!TERMINAL_STATES.has(status.state)) {
    throw new Error(`update terminal state required: ${status.state}`);
  }
  const record = await writeUpdateStatus(stateDirectory, status);
  const lockPath = join(stateDirectory, 'lock.json');
  const lock = await readJsonIfPresent(lockPath);
  if (lock?.jobId === status.jobId) {
    await rm(lockPath, { force: true });
  }
  return record;
}

class StreamReader {
  constructor(stream) {
    this.iterator = stream[Symbol.asyncIterator]();
    this.buffer = Buffer.alloc(0);
    this.done = false;
  }

  async fill() {
    if (this.buffer.length || this.done) return;
    const next = await this.iterator.next();
    if (next.done) {
      this.done = true;
      return;
    }
    this.buffer = Buffer.from(next.value);
  }

  async readExact(length, allowEnd = false) {
    const output = Buffer.alloc(length);
    let written = 0;
    while (written < length) {
      await this.fill();
      if (!this.buffer.length) {
        if (allowEnd && written === 0) return null;
        throw new Error('unsafe tar entry: truncated archive');
      }
      const count = Math.min(length - written, this.buffer.length);
      this.buffer.copy(output, written, 0, count);
      this.buffer = this.buffer.subarray(count);
      written += count;
    }
    return output;
  }

  async writeExact(length, handle) {
    let remaining = length;
    while (remaining > 0) {
      await this.fill();
      if (!this.buffer.length) {
        throw new Error('unsafe tar entry: truncated file');
      }
      const count = Math.min(remaining, this.buffer.length);
      const chunk = this.buffer.subarray(0, count);
      await handle.writeFile(chunk);
      this.buffer = this.buffer.subarray(count);
      remaining -= count;
    }
  }

  async discard(length) {
    let remaining = length;
    while (remaining > 0) {
      await this.fill();
      if (!this.buffer.length) {
        throw new Error('unsafe tar entry: truncated padding');
      }
      const count = Math.min(remaining, this.buffer.length);
      this.buffer = this.buffer.subarray(count);
      remaining -= count;
    }
  }
}

function readTarText(bytes) {
  const nullIndex = bytes.indexOf(0);
  const value = bytes.subarray(0, nullIndex === -1 ? bytes.length : nullIndex);
  try {
    return new TextDecoder('utf-8', { fatal: true }).decode(value);
  } catch {
    throw new Error('unsafe tar entry: invalid UTF-8');
  }
}

function readTarOctal(bytes, label) {
  if (bytes[0] & 0x80) {
    throw new Error(`unsafe tar entry: unsupported ${label}`);
  }
  const value = readTarText(bytes).trim();
  if (!value) return 0;
  if (!/^[0-7]+$/.test(value)) {
    throw new Error(`unsafe tar entry: invalid ${label}`);
  }
  return Number.parseInt(value, 8);
}

function verifyTarChecksum(header) {
  const expected = readTarOctal(header.subarray(148, 156), 'checksum');
  const copy = Buffer.from(header);
  copy.fill(0x20, 148, 156);
  const actual = copy.reduce((sum, byte) => sum + byte, 0);
  if (actual !== expected) {
    throw new Error('unsafe tar entry: checksum mismatch');
  }
}

function safeTarDestination(root, rawName, type) {
  if (
    !rawName ||
    rawName.includes('\\') ||
    rawName.includes('\0') ||
    rawName.startsWith('/') ||
    /^[A-Za-z]:/.test(rawName) ||
    isAbsolute(rawName)
  ) {
    throw new Error(`unsafe tar entry: ${rawName}`);
  }
  const name = rawName.endsWith('/') ? rawName.slice(0, -1) : rawName;
  const parts = name.split('/');
  if (!name || parts.some((part) => !part || part === '.' || part === '..')) {
    throw new Error(`unsafe tar entry: ${rawName}`);
  }
  if (type === '0' && rawName.endsWith('/')) {
    throw new Error(`unsafe tar entry: ${rawName}`);
  }
  const destination = resolve(root, ...parts);
  const rootPrefix = root.endsWith(sep) ? root : `${root}${sep}`;
  if (destination !== root && !destination.startsWith(rootPrefix)) {
    throw new Error(`unsafe tar entry: ${rawName}`);
  }
  return destination;
}

function inputStream(input) {
  if (Buffer.isBuffer(input) || input instanceof Uint8Array) {
    return Readable.from([Buffer.from(input)]);
  }
  if (typeof input === 'string') {
    return createReadStream(input);
  }
  throw new Error('tar.gz input must be bytes or a file path');
}

export async function extractTarGz(
  input,
  targetDirectory,
  {
    maxContentBytes = DEFAULT_MAX_CONTENT_BYTES,
    maxEntries = DEFAULT_MAX_ENTRIES,
    maxFileBytes = DEFAULT_MAX_FILE_BYTES,
  } = {},
) {
  const root = resolve(targetDirectory);
  await rm(root, { recursive: true, force: true });
  await mkdir(root, { recursive: true });
  const gunzip = createGunzip();
  inputStream(input).pipe(gunzip);
  const reader = new StreamReader(gunzip);
  let entries = 0;
  let contentBytes = 0;

  try {
    for (;;) {
      const header = await reader.readExact(BLOCK_SIZE, true);
      if (!header) {
        throw new Error('unsafe tar entry: missing end marker');
      }
      if (header.every((byte) => byte === 0)) {
        return { contentBytes, entries };
      }
      entries += 1;
      if (entries > maxEntries) {
        throw new Error('archive limit exceeded: too many entries');
      }
      verifyTarChecksum(header);
      if (!readTarText(header.subarray(257, 263)).startsWith('ustar')) {
        throw new Error('unsafe tar entry: unsupported header');
      }
      const typeByte = header[156];
      const type = typeByte === 0 ? '0' : String.fromCharCode(typeByte);
      if (type !== '0' && type !== '5') {
        throw new Error('unsafe tar entry: links and special files are forbidden');
      }
      const name = readTarText(header.subarray(0, 100));
      const prefix = readTarText(header.subarray(345, 500));
      const archiveName = prefix ? `${prefix}/${name}` : name;
      const size = readTarOctal(header.subarray(124, 136), 'size');
      const mode = readTarOctal(header.subarray(100, 108), 'mode') & 0o777;
      if (!Number.isSafeInteger(size) || size < 0) {
        throw new Error('unsafe tar entry: invalid size');
      }
      if (type === '5' && size !== 0) {
        throw new Error('unsafe tar entry: directory has content');
      }
      if (size > maxFileBytes || contentBytes + size > maxContentBytes) {
        throw new Error('archive limit exceeded: uncompressed content');
      }
      contentBytes += size;
      const destination = safeTarDestination(root, archiveName, type);

      if (type === '5') {
        await mkdir(destination, { recursive: true, mode: 0o755 });
        await chmod(destination, mode || 0o755);
      } else {
        await mkdir(dirname(destination), { recursive: true, mode: 0o755 });
        const handle = await open(destination, 'wx', 0o644);
        try {
          await reader.writeExact(size, handle);
          await handle.chmod(mode || 0o644);
        } finally {
          await handle.close();
        }
      }
      const padding = (BLOCK_SIZE - (size % BLOCK_SIZE)) % BLOCK_SIZE;
      await reader.discard(padding);
    }
  } catch (error) {
    gunzip.destroy();
    await rm(root, { recursive: true, force: true });
    throw error;
  }
}

function requireHttps(value, label) {
  let url;
  try {
    url = new URL(value);
  } catch {
    throw new Error(`${label} URL is invalid`);
  }
  if (url.protocol !== 'https:') {
    throw new Error(`${label} URL must use HTTPS`);
  }
  return url.href;
}

export function currentPlatformKey(
  platform = process.platform,
  architecture = process.arch,
) {
  const key = `${platform}/${architecture}`;
  if (key === 'win32/x64') return 'windows-amd64';
  if (key === 'linux/x64') return 'linux-amd64';
  if (key === 'darwin/arm64') return 'darwin-arm64';
  throw new Error(`unsupported deployment platform: ${key}`);
}

export async function stageVerifiedRelease({
  fetchBytes,
  jobId,
  onPhase = async () => {},
  platform = currentPlatformKey(),
  stable,
  stagingDirectory,
}) {
  validateJobId(jobId);
  if (!['windows-amd64', 'linux-amd64', 'darwin-arm64'].includes(platform)) {
    throw new Error(`unsupported deployment platform: ${platform}`);
  }
  const manifestUrl = requireHttps(
    stable?.release?.manifestUrl,
    'release manifest',
  );
  const manifestBytes = await fetchBytes(manifestUrl);
  if (
    sha256Bytes(manifestBytes) !== stable.release.manifestSha256
  ) {
    throw new Error('release manifest SHA-256 verification failed');
  }
  const manifest = JSON.parse(manifestBytes.toString('utf8'));
  if (
    manifest.schema !== 1 ||
    manifest.version !== stable.version ||
    manifest.sourceSha !== stable.sourceSha
  ) {
    throw new Error('release manifest identity is invalid');
  }
  const artifact = manifest.artifacts?.[platform];
  if (
    !artifact ||
    !/^[0-9a-f]{64}$/.test(artifact.sha256 ?? '') ||
    !Number.isSafeInteger(artifact.size) ||
    artifact.size < 0
  ) {
    throw new Error(`release artifact metadata is invalid: ${platform}`);
  }
  const artifactUrl = requireHttps(artifact.url, 'release artifact');
  const archiveBytes = await fetchBytes(artifactUrl);
  await onPhase('verifying');
  if (archiveBytes.length !== artifact.size) {
    throw new Error('release archive size verification failed');
  }
  if (sha256Bytes(archiveBytes) !== artifact.sha256) {
    throw new Error('release archive SHA-256 verification failed');
  }

  const extractionDirectory = join(stagingDirectory, jobId, 'package');
  const extraction = await extractTarGz(archiveBytes, extractionDirectory);
  return {
    artifact,
    extraction,
    extractionDirectory,
    manifest,
    manifestSha256: sha256Bytes(manifestBytes),
  };
}

function psQuote(value) {
  return `'${String(value).replaceAll("'", "''")}'`;
}

function windowsBatchQuote(value) {
  return `"${String(value).replaceAll('%', '%%')}"`;
}

function shellQuote(value) {
  return `'${String(value).replaceAll("'", `'"'"'`)}'`;
}

function systemdQuote(value) {
  return `"${String(value).replaceAll('\\', '\\\\').replaceAll('"', '\\"')}"`;
}

function xmlEscape(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&apos;');
}

export function deploymentRuntimePaths({
  installDirectory,
  nodePath = process.execPath,
  platform = process.platform,
  uid = typeof process.getuid === 'function' ? process.getuid() : 0,
  userHome = homedir(),
}) {
  const home = resolve(installDirectory);
  const bin = join(home, 'bin');
  return {
    bin,
    deploy: join(home, 'deploy.mjs'),
    home,
    hub: join(bin, platform === 'win32' ? 'wheelmaker.exe' : 'wheelmaker'),
    node: nodePath,
    uid,
    userHome,
  };
}

export function windowsRuntimePlan(paths) {
  const names = ['WheelMaker', 'WheelMakerUpdater'];
  const updaterCommand = `& ${psQuote(paths.node)} ${psQuote(paths.deploy)} update\nexit $LASTEXITCODE`;
  const encodedUpdaterCommand = Buffer.from(updaterCommand, 'utf16le').toString('base64');
  const updaterArguments = `-NoProfile -NonInteractive -WindowStyle Hidden -EncodedCommand ${encodedUpdaterCommand}`;
  const script = `$ErrorActionPreference = 'Stop'
$currentUser = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$principal = New-ScheduledTaskPrincipal -UserId $currentUser -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) -ExecutionTimeLimit (New-TimeSpan -Seconds 0) -StartWhenAvailable -MultipleInstances IgnoreNew
$hubAction = New-ScheduledTaskAction -Execute ${psQuote(paths.hub)} -Argument '-d' -WorkingDirectory ${psQuote(paths.home)}
$hubTrigger = New-ScheduledTaskTrigger -AtLogOn -User $currentUser
Register-ScheduledTask -TaskName 'WheelMaker' -Action $hubAction -Trigger $hubTrigger -Principal $principal -Settings $settings -Force | Out-Null
$updaterArguments = ${psQuote(updaterArguments)}
$updaterAction = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument $updaterArguments -WorkingDirectory ${psQuote(paths.home)}
$updaterTrigger = New-ScheduledTaskTrigger -Daily -At '03:00'
Register-ScheduledTask -TaskName 'WheelMakerUpdater' -Action $updaterAction -Trigger $updaterTrigger -Principal $principal -Settings $settings -Force | Out-Null
`;
  return { names, script };
}

export function linuxRuntimeFiles(paths) {
  return {
    'wheelmaker-hub.service': `[Unit]
Description=WheelMaker Hub

[Service]
Type=simple
WorkingDirectory=${systemdQuote(paths.home)}
ExecStart=${systemdQuote(paths.hub)} -d
Restart=always
RestartSec=5
StartLimitIntervalSec=300
StartLimitBurst=5

[Install]
WantedBy=default.target
`,
    'wheelmaker-updater.service': `[Unit]
Description=WheelMaker Updater
After=network-online.target

[Service]
Type=oneshot
WorkingDirectory=${systemdQuote(paths.home)}
ExecStart=${systemdQuote(paths.node)} ${systemdQuote(paths.deploy)} update
`,
    'wheelmaker-updater.timer': `[Unit]
Description=Run WheelMaker Updater daily

[Timer]
OnCalendar=*-*-* 03:00:00
Persistent=true
Unit=wheelmaker-updater.service

[Install]
WantedBy=timers.target
`,
  };
}

function launchAgentPlist({
  arguments: programArguments,
  calendar,
  keepAlive,
  label,
  paths,
}) {
  const argumentsXml = programArguments
    .map((argument) => `    <string>${xmlEscape(argument)}</string>`)
    .join('\n');
  const scheduleXml = calendar
    ? `  <key>StartCalendarInterval</key>
  <dict>
    <key>Hour</key>
    <integer>${calendar.hour}</integer>
    <key>Minute</key>
    <integer>${calendar.minute}</integer>
  </dict>\n`
    : '';
  const keepAliveXml = keepAlive
    ? '  <key>KeepAlive</key>\n  <true/>\n  <key>RunAtLoad</key>\n  <true/>\n'
    : '';
  return `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${xmlEscape(label)}</string>
  <key>WorkingDirectory</key>
  <string>${xmlEscape(paths.home)}</string>
  <key>ProgramArguments</key>
  <array>
${argumentsXml}
  </array>
${keepAliveXml}${scheduleXml}</dict>
</plist>
`;
}

export function darwinRuntimeFiles(paths) {
  return {
    'com.wheelmaker.hub.plist': launchAgentPlist({
      arguments: [paths.hub, '-d'],
      keepAlive: true,
      label: 'com.wheelmaker.hub',
      paths,
    }),
    'com.wheelmaker.updater.plist': launchAgentPlist({
      arguments: [paths.node, paths.deploy, 'update'],
      calendar: { hour: 3, minute: 0 },
      keepAlive: false,
      label: 'com.wheelmaker.updater',
      paths,
    }),
  };
}

export function windowsWrappers(paths) {
  return {
    'deploy.bat': `@echo off\r\nsetlocal\r\n${windowsBatchQuote(paths.node)} ${windowsBatchQuote(paths.deploy)}\r\nset "_EXIT_CODE=%errorlevel%"\r\necho.\r\npause\r\nexit /b %_EXIT_CODE%\r\n`,
    ...Object.fromEntries(
      ['start', 'stop'].map((action) => [
        `${action}.bat`,
        `@echo off\r\nsetlocal\r\n${windowsBatchQuote(paths.node)} ${windowsBatchQuote(paths.deploy)} runtime ${action} %*\r\nexit /b %errorlevel%\r\n`,
      ]),
    ),
    'update_exe.bat': `@echo off\r\nsetlocal\r\n${windowsBatchQuote(paths.node)} ${windowsBatchQuote(paths.deploy)} desktop-update\r\nexit /b %errorlevel%\r\n`,
  };
}

export function unixWrappers(paths) {
  return {
    'deploy.sh': `#!/bin/sh\nset -eu\nexec ${shellQuote(paths.node)} ${shellQuote(paths.deploy)}\n`,
    ...Object.fromEntries(
      ['start', 'stop'].map((action) => [
        `${action}.sh`,
        `#!/bin/sh\nset -eu\nexec ${shellQuote(paths.node)} ${shellQuote(paths.deploy)} runtime ${action} "$@"\n`,
      ]),
    ),
  };
}

const RETIRED_LIFECYCLE_WRAPPERS = [
  'restart.bat',
  'restart.sh',
  'status.bat',
  'status.sh',
];

async function removeRetiredLifecycleWrappers(home) {
  for (const name of RETIRED_LIFECYCLE_WRAPPERS) {
    await rm(join(home, name), { force: true });
  }
}

async function writeRuntimeWrappers(paths, platform) {
  const useWindows = platform === 'win32';
  const active = useWindows ? windowsWrappers(paths) : unixWrappers(paths);
  const staleNames = useWindows
    ? ['deploy.sh', 'start.sh', 'stop.sh']
    : ['deploy.bat', 'start.bat', 'stop.bat', 'update_exe.bat'];
  await mkdir(paths.home, { recursive: true });
  await removeRetiredLifecycleWrappers(paths.home);
  for (const name of staleNames) {
    await rm(join(paths.home, name), { force: true });
  }
  for (const [name, body] of Object.entries(active)) {
    const mode = useWindows ? 0o644 : 0o755;
    await atomicWrite(join(paths.home, name), Buffer.from(body, 'utf8'), mode);
    await chmod(join(paths.home, name), mode);
  }
}

async function runProcess(
  command,
  args,
  { allowFailure = false, cwd, env = process.env } = {},
) {
  return new Promise((resolvePromise, rejectPromise) => {
    const child = spawn(command, args, {
      cwd,
      env,
      shell: false,
      stdio: ['ignore', 'pipe', 'pipe'],
      windowsHide: true,
    });
    const stdout = [];
    const stderr = [];
    child.stdout.on('data', (chunk) => stdout.push(Buffer.from(chunk)));
    child.stderr.on('data', (chunk) => stderr.push(Buffer.from(chunk)));
    child.once('error', rejectPromise);
    child.once('exit', (code, signal) => {
      const result = {
        code: code ?? -1,
        stderr: Buffer.concat(stderr).toString('utf8'),
        stdout: Buffer.concat(stdout).toString('utf8'),
      };
      if (code === 0 || allowFailure) {
        resolvePromise(result);
        return;
      }
      rejectPromise(
        new Error(
          `${command} ${args.join(' ')} failed (${signal ?? `exit ${code}`}): ${result.stderr}`,
        ),
      );
    });
  });
}

export function windowsLegacyMigrationScript(paths) {
  return `$ErrorActionPreference = 'Stop'
$runtimeNames = @('WheelMaker', 'WheelMakerUpdater', 'WheelMakerMonitor')
foreach ($name in $runtimeNames) {
  Stop-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue
  Unregister-ScheduledTask -TaskName $name -Confirm:$false -ErrorAction SilentlyContinue
}

$runKey = 'HKCU:\\Software\\Microsoft\\Windows\\CurrentVersion\\Run'
foreach ($name in $runtimeNames) {
  Remove-ItemProperty -Path $runKey -Name $name -ErrorAction SilentlyContinue
}

$binRoot = ([System.IO.Path]::GetFullPath(${psQuote(paths.bin)}).TrimEnd('\\') + '\\').ToLowerInvariant()
$legacyBinaries = @(
  'wheelmaker.exe',
  'wheelmaker-updater.exe',
  'wheelmaker-deploy.exe',
  'wheelmaker-monitor.exe'
)
Get-CimInstance Win32_Process | Where-Object {
  $path = [string]$_.ExecutablePath
  -not [string]::IsNullOrWhiteSpace($path) -and
  $path.ToLowerInvariant().StartsWith($binRoot) -and
  $legacyBinaries -contains [System.IO.Path]::GetFileName($path).ToLowerInvariant()
} | ForEach-Object {
  Stop-Process -Id $_.ProcessId -Force -ErrorAction Stop
}

$existingServices = @(Get-Service -Name $runtimeNames -ErrorAction SilentlyContinue)
if ($existingServices.Count -gt 0) {
  $serviceRemoval = @'
$ErrorActionPreference = 'Stop'
foreach ($name in @('WheelMaker', 'WheelMakerUpdater', 'WheelMakerMonitor')) {
  $service = Get-Service -Name $name -ErrorAction SilentlyContinue
  if ($null -ne $service) {
    if ($service.Status -ne 'Stopped') {
      Stop-Service -Name $name -Force -ErrorAction Stop
    }
    & sc.exe delete $name | Out-Null
    if ($LASTEXITCODE -ne 0) {
      throw "failed to delete service $name"
    }
  }
}
'@
  $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
  $principal = [Security.Principal.WindowsPrincipal]::new($identity)
  $isAdministrator = $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
  if ($isAdministrator) {
    & ([ScriptBlock]::Create($serviceRemoval))
  } else {
    $encoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($serviceRemoval))
    $process = Start-Process -FilePath 'powershell.exe' -ArgumentList @(
      '-NoProfile',
      '-ExecutionPolicy',
      'Bypass',
      '-EncodedCommand',
      $encoded
    ) -Verb RunAs -Wait -PassThru
    if ($process.ExitCode -ne 0) {
      throw "elevated legacy service removal failed with exit code $($process.ExitCode)"
    }
  }
}
`;
}

export function createLegacyMigrationAdapter({
  paths,
  platform = process.platform,
  runner = runProcess,
}) {
  return {
    removeRuntime: async () => {
      if (platform === 'win32') {
        await runner(
          'powershell',
          [
            '-NoProfile',
            '-ExecutionPolicy',
            'Bypass',
            '-Command',
            windowsLegacyMigrationScript(paths),
          ],
          { cwd: paths.home },
        );
        return;
      }
      if (platform === 'linux') {
        const units = [
          'wheelmaker-hub.service',
          'wheelmaker-updater.service',
          'wheelmaker-updater.timer',
          'wheelmaker-monitor.service',
        ];
        for (const unit of units) {
          await runner('systemctl', ['--user', 'disable', '--now', unit], {
            allowFailure: true,
          });
          await rm(join(paths.userHome, '.config', 'systemd', 'user', unit), {
            force: true,
          });
        }
        await runner('systemctl', ['--user', 'daemon-reload'], {
          allowFailure: true,
        });
        return;
      }
      if (platform === 'darwin') {
        const labels = [
          'com.wheelmaker.hub',
          'com.wheelmaker.updater',
          'com.wheelmaker.monitor',
        ];
        const domain = `gui/${paths.uid}`;
        for (const label of labels) {
          await runner('launchctl', ['bootout', `${domain}/${label}`], {
            allowFailure: true,
          });
          await rm(join(paths.userHome, 'Library', 'LaunchAgents', `${label}.plist`), {
            force: true,
          });
        }
        return;
      }
      throw new Error(`unsupported migration platform: ${platform}`);
    },
  };
}

async function executeLegacyMigration(deps) {
  if (!deps.installDirectory) {
    throw new Error('deployment install directory is required');
  }
  const platform = deps.platform ?? process.platform;
  const paths = deploymentRuntimePaths({
    installDirectory: deps.installDirectory,
    nodePath: deps.nodePath ?? process.execPath,
    platform,
    uid: deps.uid,
    userHome: deps.userHome ?? homedir(),
  });
  const migration =
    deps.legacyMigration ??
    createLegacyMigrationAdapter({ paths, platform, runner: deps.runner });
  await migration.removeRuntime();

  const suffix = platform === 'win32' ? '.exe' : '';
  for (const name of [
    'wheelmaker',
    'wheelmaker-updater',
    'wheelmaker-deploy',
    'wheelmaker-monitor',
  ]) {
    await rm(join(paths.bin, `${name}${suffix}`), { force: true });
  }
  for (const directory of [
    join(paths.home, 'build'),
    join(paths.home, 'cache', 'go-build'),
    join(paths.home, 'mobile'),
    join(paths.home, 'tmp'),
  ]) {
    await rm(directory, { force: true, recursive: true });
  }
  await rm(join(paths.home, 'update-now.signal'), { force: true });
  await removeRetiredLifecycleWrappers(paths.home);
}

async function detectDesktopRunning(platform, runner) {
  if (platform !== 'win32') {
    throw new Error('WheelMaker Desktop update is supported on Windows only');
  }
  const result = await runner(
    'powershell',
    [
      '-NoProfile',
      '-Command',
      "if (Get-Process -Name 'WheelMakerDesktop' -ErrorAction SilentlyContinue) { exit 0 } else { exit 1 }",
    ],
    { allowFailure: true },
  );
  return result.code === 0;
}

async function executeDesktopUpdate(deps) {
  if (!deps.installDirectory) {
    throw new Error('deployment install directory is required');
  }
  const pointer = deps.trustedStable?.desktopExe;
  if (
    !pointer ||
    !/^v1\.(0|[1-9]\d*)$/.test(pointer.version ?? '') ||
    !/^[0-9a-f]{64}$/.test(pointer.sha256 ?? '')
  ) {
    throw new Error('stable release does not contain a valid Desktop executable');
  }
  requireHttps(pointer.url, 'Desktop executable');
  const platform = deps.platform ?? process.platform;
  const runner = deps.runner ?? runProcess;
  const isDesktopRunning =
    deps.isDesktopRunning ?? (() => detectDesktopRunning(platform, runner));
  if (await isDesktopRunning()) {
    throw new Error('Close WheelMaker Desktop before updating it');
  }
  if (typeof deps.fetchBytes !== 'function') {
    throw new Error('Desktop executable downloader is required');
  }

  const home = resolve(deps.installDirectory);
  const desktopDirectory = join(home, 'desktop');
  const targetPath = join(desktopDirectory, 'WheelMakerDesktop.exe');
  const temporaryPath = `${targetPath}.tmp`;
  const bytes = Buffer.from(await deps.fetchBytes(pointer.url));
  await mkdir(desktopDirectory, { recursive: true });
  await rm(temporaryPath, { force: true });
  try {
    await writeFile(temporaryPath, bytes, { mode: 0o755 });
    if (sha256Bytes(bytes) !== pointer.sha256) {
      throw new Error('Desktop executable SHA-256 verification failed');
    }
    await rename(temporaryPath, targetPath);
  } finally {
    await rm(temporaryPath, { force: true });
  }
}

function windowsStopScript(paths) {
  return `$ErrorActionPreference = 'Stop'
Stop-ScheduledTask -TaskName 'WheelMaker' -ErrorAction SilentlyContinue
$bin = (${psQuote(paths.bin)}.TrimEnd('\\') + '\\').ToLowerInvariant()
$hub = ${psQuote(paths.hub)}.ToLowerInvariant()
Get-CimInstance Win32_Process | Where-Object {
  $path = [string]$_.ExecutablePath
  -not [string]::IsNullOrWhiteSpace($path) -and
  $path.ToLowerInvariant().StartsWith($bin) -and
  $path.ToLowerInvariant() -eq $hub
} | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction Stop }
`;
}

async function checkLinuxPrerequisites(runner) {
  await runner('systemctl', ['--user', 'show-environment']);
  let userName = process.env.USER;
  if (!userName) {
    userName = (await runner('id', ['-un'])).stdout.trim();
  }
  const lingering = await runner('loginctl', [
    'show-user',
    userName,
    '-p',
    'Linger',
  ]);
  if (!lingering.stdout.includes('Linger=yes')) {
    throw new Error(
      'linux deploy requires lingering so systemd user services survive logout; run: sudo loginctl enable-linger "$USER"',
    );
  }
}

export function createRuntimeAdapter({
  paths,
  platform = process.platform,
  runner = runProcess,
}) {
  async function configureRuntime() {
    if (platform === 'win32') {
      await runner(
        'powershell',
        ['-NoProfile', '-ExecutionPolicy', 'Bypass', '-Command', windowsRuntimePlan(paths).script],
        { cwd: paths.home },
      );
      return;
    }
    if (platform === 'linux') {
      await checkLinuxPrerequisites(runner);
      const unitDirectory = join(paths.userHome, '.config', 'systemd', 'user');
      for (const [name, body] of Object.entries(linuxRuntimeFiles(paths))) {
        await atomicWrite(join(unitDirectory, name), Buffer.from(body), 0o644);
      }
      await runner('systemctl', ['--user', 'daemon-reload']);
      for (const unit of ['wheelmaker-hub.service', 'wheelmaker-updater.timer']) {
        await runner('systemctl', ['--user', 'enable', unit]);
        await runner('systemctl', ['--user', 'start', unit]);
      }
      return;
    }
    if (platform === 'darwin') {
      const directory = join(paths.userHome, 'Library', 'LaunchAgents');
      const files = darwinRuntimeFiles(paths);
      for (const [name, body] of Object.entries(files)) {
        await atomicWrite(join(directory, name), Buffer.from(body), 0o644);
      }
      const domain = `gui/${paths.uid}`;
      for (const label of ['com.wheelmaker.hub', 'com.wheelmaker.updater']) {
        await runner('launchctl', ['bootout', `${domain}/${label}`], {
          allowFailure: true,
        });
        await runner('launchctl', [
          'bootstrap',
          domain,
          join(directory, `${label}.plist`),
        ]);
      }
      await runner('launchctl', ['kickstart', '-k', `${domain}/com.wheelmaker.hub`]);
      return;
    }
    throw new Error(`unsupported runtime platform: ${platform}`);
  }

  async function action(name) {
    if (!['start', 'stop'].includes(name)) {
      throw new Error(`unknown runtime action: ${name}`);
    }
    if (platform === 'win32') {
      if (name === 'stop') {
        await runner(
          'powershell',
          ['-NoProfile', '-ExecutionPolicy', 'Bypass', '-Command', windowsStopScript(paths)],
          { cwd: paths.home },
        );
      }
      if (name === 'start') {
        await runner('powershell', [
          '-NoProfile',
          '-ExecutionPolicy',
          'Bypass',
          '-Command',
          "Start-ScheduledTask -TaskName 'WheelMaker' -ErrorAction Stop",
        ]);
      }
      return;
    }
    if (platform === 'linux') {
      return runner('systemctl', ['--user', name, 'wheelmaker-hub.service'], {
        allowFailure: name === 'stop',
      });
    }
    if (platform === 'darwin') {
      const target = `gui/${paths.uid}/com.wheelmaker.hub`;
      if (name === 'start') {
        return runner('launchctl', ['kickstart', '-k', target]);
      }
      if (name === 'stop') {
        return runner('launchctl', ['kill', 'SIGTERM', target], {
          allowFailure: true,
        });
      }
    }
    throw new Error(`unsupported runtime platform: ${platform}`);
  }

  return {
    configureRuntime,
    isHubRunning: async () => {
      if (platform === 'win32') {
        const result = await runner(
          'powershell',
          [
            '-NoProfile',
            '-Command',
            "if ((Get-ScheduledTask -TaskName 'WheelMaker' -ErrorAction SilentlyContinue).State -eq 'Running') { exit 0 } else { exit 1 }",
          ],
          { allowFailure: true },
        );
        return result.code === 0;
      }
      if (platform === 'linux') {
        const result = await runner(
          'systemctl',
          ['--user', 'is-active', '--quiet', 'wheelmaker-hub.service'],
          { allowFailure: true },
        );
        return result.code === 0;
      }
      const result = await runner(
        'launchctl',
        ['print', `gui/${paths.uid}/com.wheelmaker.hub`],
        { allowFailure: true },
      );
      return result.code === 0;
    },
    isUpdaterRunning: async () => {
      if (platform === 'win32') {
        const result = await runner(
          'powershell',
          [
            '-NoProfile',
            '-Command',
            "if ((Get-ScheduledTask -TaskName 'WheelMakerUpdater' -ErrorAction SilentlyContinue).State -eq 'Running') { exit 0 } else { exit 1 }",
          ],
          { allowFailure: true },
        );
        return result.code === 0;
      }
      if (platform === 'linux') {
        const result = await runner(
          'systemctl',
          ['--user', 'is-active', '--quiet', 'wheelmaker-updater.service'],
          { allowFailure: true },
        );
        return result.code === 0;
      }
      const result = await runner(
        'launchctl',
        ['print', `gui/${paths.uid}/com.wheelmaker.updater`],
        { allowFailure: true },
      );
      return result.code === 0;
    },
    start: () => action('start'),
    stop: () => action('stop'),
    writeWrappers: () => writeRuntimeWrappers(paths, platform),
  };
}

function resolveRuntime(deps) {
  if (deps.runtime) return deps.runtime;
  if (!deps.installDirectory) {
    throw new Error('deployment install directory is required');
  }
  const platform = deps.platform ?? process.platform;
  const paths = deploymentRuntimePaths({
    installDirectory: deps.installDirectory,
    nodePath: deps.nodePath ?? process.execPath,
    platform,
    uid: deps.uid,
    userHome: deps.userHome ?? homedir(),
  });
  const runtimeFactory = deps.runtimeFactory ?? createRuntimeAdapter;
  return runtimeFactory({ paths, platform, runner: deps.runner });
}

async function ensureRuntimeConfig(home) {
  const configPath = join(home, 'config.json');
  if (await readJsonIfPresent(configPath)) {
    return false;
  }
  const config = {
    projects: [],
    registry: {
      listen: true,
      port: 9630,
      server: '127.0.0.1',
      token: randomBytes(32).toString('base64url'),
      hubId: 'local-hub',
    },
    log: { level: 'warn' },
  };
  await atomicWrite(configPath, jsonBytes(config), 0o600);
  return true;
}

async function applyStagedPackage({
  extractionDirectory,
  home,
  jobId,
  platform,
}) {
  const binaryName = platform === 'win32' ? 'wheelmaker.exe' : 'wheelmaker';
  const sourceBinary = join(extractionDirectory, 'hub', binaryName);
  const sourceWeb = join(extractionDirectory, 'web');
  await access(sourceBinary);
  await access(sourceWeb);

  const binDirectory = join(home, 'bin');
  const targetBinary = join(binDirectory, binaryName);
  const temporaryBinary = join(binDirectory, `.${binaryName}.${jobId}.tmp`);
  const targetWeb = join(home, 'web');
  const temporaryWeb = join(home, `.web.${jobId}.tmp`);
  await mkdir(binDirectory, { recursive: true });
  await rm(temporaryBinary, { force: true });
  await rm(temporaryWeb, { recursive: true, force: true });

  try {
    await cp(sourceBinary, temporaryBinary);
    await chmod(temporaryBinary, 0o755);
    await cp(sourceWeb, temporaryWeb, { recursive: true });
    await rm(targetBinary, { force: true });
    await rename(temporaryBinary, targetBinary);
    await rm(targetWeb, { recursive: true, force: true });
    await rename(temporaryWeb, targetWeb);
  } finally {
    await rm(temporaryBinary, { force: true });
    await rm(temporaryWeb, { recursive: true, force: true });
  }
}

async function writeInstalledRelease(home, stable, manifestSha256, installedAt) {
  const release = {
    schemaVersion: 2,
    version: stable.version,
    publishedAt: stable.publishedAt,
    sourceSha: stable.sourceSha,
    manifestSha256,
    installedAt,
  };
  await atomicWrite(join(home, 'release.json'), jsonBytes(release), 0o644);
  return release;
}

async function confirmHubStarted(runtime, deps) {
  if (typeof runtime.isHubRunning !== 'function') {
    throw new Error('runtime adapter cannot confirm Hub health');
  }
  const timeoutMs = deps.healthTimeoutMs ?? 30_000;
  const pollIntervalMs = deps.healthPollIntervalMs ?? 500;
  const attempts = Math.max(1, Math.ceil(timeoutMs / pollIntervalMs));
  const sleep =
    deps.sleep ?? ((milliseconds) => new Promise((resolveSleep) => setTimeout(resolveSleep, milliseconds)));
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    if (await runtime.isHubRunning()) return;
    if (attempt + 1 < attempts) await sleep(pollIntervalMs);
  }
  const error = new Error('Hub did not start within the health check timeout');
  error.code = 'hub_start_timeout';
  throw error;
}

function updateErrorCode(phase, error) {
  if (error?.code === 'hub_start_timeout') return 'hub_start_timeout';
  return {
    applying: 'apply_failed',
    downloading: 'download_failed',
    restarting: 'restart_failed',
    verifying: 'verification_failed',
  }[phase] ?? 'update_failed';
}

async function resolveUpdateJob({ deps, internalUpdate, runtime, stagingDirectory }) {
  const existing = await readJsonIfPresent(join(stagingDirectory, 'lock.json'));
  if (internalUpdate && existing?.state === 'queued') {
    validateJobId(existing.jobId);
    return { jobId: existing.jobId, startedAt: normalizeTime(existing.startedAt) };
  }
  const jobId = deps.jobIdFactory?.() ?? randomUUID();
  const startedAt = normalizeTime(deps.now?.() ?? new Date().toISOString());
  const acquired = await acquireUpdateLease(
    stagingDirectory,
    { jobId, now: startedAt, owner: 'timer' },
    {
      isUpdaterRunning:
        runtime.isUpdaterRunning?.bind(runtime) ?? (async () => false),
    },
  );
  if (!acquired) {
    throw new Error('another deployment update is already active');
  }
  return { jobId, startedAt };
}

async function executeDeployment(internalUpdate, deps, runtime) {
  if (!deps.trustedStable) {
    throw new Error('trusted stable metadata is required');
  }
  const home = resolve(deps.installDirectory);
  const platform = deps.platform ?? process.platform;
  const stagingDirectory = join(home, 'staging');
  const { jobId, startedAt } = await resolveUpdateJob({
    deps,
    internalUpdate,
    runtime,
    stagingDirectory,
  });
  let phase = 'downloading';
  let runtimeStopped = false;
  let runtimeStarted = false;
  const now = () => deps.now?.() ?? new Date().toISOString();

  const setState = async (state) => {
    phase = state;
    await heartbeatUpdateLease(stagingDirectory, jobId, now());
    await writeUpdateStatus(stagingDirectory, {
      jobId,
      now: now(),
      startedAt,
      state,
      version: deps.trustedStable.version,
    });
  };

  let deploymentError;
  try {
    await setState('downloading');
    const stageRelease =
      deps.stageRelease ??
      ((input) =>
        stageVerifiedRelease({
          ...input,
          fetchBytes: deps.fetchBytes,
          stable: deps.trustedStable,
          stagingDirectory,
        }));
    const staged = await stageRelease({
      jobId,
      onPhase: async (nextPhase) => setState(nextPhase),
      platform:
        deps.platformKey ?? currentPlatformKey(platform, deps.arch ?? process.arch),
    });
    if (phase !== 'verifying') await setState('verifying');
    if (!internalUpdate) await ensureRuntimeConfig(home);

    await setState('applying');
    await runtime.stop();
    runtimeStopped = true;
    await applyStagedPackage({
      extractionDirectory: staged.extractionDirectory,
      home,
      jobId,
      platform,
    });
    await removeRetiredLifecycleWrappers(home);
    await writeInstalledRelease(
      home,
      deps.trustedStable,
      staged.manifestSha256,
      normalizeTime(now()),
    );
    if (!internalUpdate) {
      await runtime.configureRuntime();
      await runtime.writeWrappers();
    }

    await setState('restarting');
    await runtime.start();
    runtimeStarted = true;
    await confirmHubStarted(runtime, deps);
    await finishUpdate(stagingDirectory, {
      jobId,
      now: now(),
      startedAt,
      state: 'succeeded',
      version: deps.trustedStable.version,
    });
  } catch (error) {
    if (runtimeStopped && !runtimeStarted) {
      await runtime.start().catch(() => {});
    }
    await finishUpdate(stagingDirectory, {
      errorCode: updateErrorCode(phase, error),
      jobId,
      now: now(),
      startedAt,
      state: 'failed',
      version: deps.trustedStable.version,
    }).catch(() => {});
    deploymentError = error;
  }

  let cleanupError;
  try {
    validateJobId(jobId);
    await rm(join(stagingDirectory, jobId), {
      force: true,
      recursive: true,
    });
  } catch (error) {
    cleanupError = error;
  }
  if (deploymentError && cleanupError) {
    throw new AggregateError(
      [deploymentError, cleanupError],
      'deployment and staging cleanup both failed',
    );
  }
  if (deploymentError) throw deploymentError;
  if (cleanupError) throw cleanupError;
}

export async function runCore(args, deps = {}) {
  if (args.length === 1 && args[0] === 'migrate-uninstall') {
    return executeLegacyMigration(deps);
  }
  if (args.length === 1 && args[0] === 'desktop-update') {
    return executeDesktopUpdate(deps);
  }
  const runtime = resolveRuntime(deps);
  if (args[0] === 'runtime' && args.length === 2) {
    if (!['start', 'stop'].includes(args[1])) {
      throw new Error(`unknown runtime action: ${args[1]}`);
    }
    if (!runtime || typeof runtime[args[1]] !== 'function') {
      throw new Error(`runtime adapter cannot ${args[1]}`);
    }
    return runtime[args[1]]();
  }
  if (args.length === 1 && args[0] === 'update') {
    if (typeof deps.applyUpdate === 'function') {
      await runtime.stop();
      let updateError;
      try {
        await deps.applyUpdate();
      } catch (error) {
        updateError = error;
      }
      await runtime.start();
      if (updateError) throw updateError;
      return;
    }
    return executeDeployment(true, deps, runtime);
  }
  if (args.length === 0) return executeDeployment(false, deps, runtime);
  throw new Error('deployment application is not implemented yet');
}
