import { randomUUID } from 'node:crypto'
import { link, mkdir, open, readdir, readFile, rename, rm, stat } from 'node:fs/promises'
import { hostname } from 'node:os'
import { basename, dirname, join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import {
  advanceWorkflowStateLock,
  startWorkflowStateLockAcquire,
  startWorkflowStateLockRelease,
  type WorkflowStateLockContext,
  type WorkflowStateLockHeld,
  type WorkflowStateLockMachine,
  type WorkflowStateLockOutcome,
  workflowStateLockOperation,
} from './workflow-state-lock.mjs'

export const WORKFLOW_STATE_VERSION = 1
export const WORKFLOW_TASK_STATUSES = ['queued', 'running', 'blocked', 'done', 'failed'] as const

export type WorkflowStateVersion = typeof WORKFLOW_STATE_VERSION
export type WorkflowTaskStatus = (typeof WORKFLOW_TASK_STATUSES)[number]

export interface WorkflowLease {
  readonly workerId: string
  readonly expiresAt: string
  readonly renewedAt: string
}

export interface WorkflowTask {
  readonly id: string
  readonly status: WorkflowTaskStatus
  readonly updatedAt: string
  readonly prompt: string
  readonly priority: number
  readonly attempts: number
  readonly maxAttempts: number
  readonly lease: WorkflowLease | null
  readonly blockedReason: string | null
  readonly lastError: string | null
}

export interface WorkflowState {
  readonly version: WorkflowStateVersion
  readonly tasks: readonly WorkflowTask[]
}

export class WorkflowStateParseError extends Error {
  override readonly name = 'WorkflowStateParseError'
  readonly path: readonly string[]

  constructor(path: readonly string[], reason: string) {
    super(`invalid workflow state at ${formatPath(path)}: ${reason}`)
    this.path = path
  }
}

export class WorkflowStateSerializeError extends Error {
  override readonly name = 'WorkflowStateSerializeError'

  constructor() {
    super('workflow state could not be serialized')
  }
}

export interface WorkflowStateLockRecord {
  readonly pid: number
  readonly hostname: string
  readonly acquiredAt: string
  readonly token: string
}

export interface WorkflowStateStoreOptions {
  readonly lockTimeoutMs?: number
  readonly lockStaleMs?: number
}

export class WorkflowStateLockTimeoutError extends Error {
  override readonly name = 'WorkflowStateLockTimeoutError'

  constructor(path: string, timeoutMs: number, holder: WorkflowStateLockRecord | null) {
    super(
      `workflow state lock acquisition timed out after ${timeoutMs}ms for ${path}; ` +
        `holder pid ${holder?.pid ?? 'unknown'} on ${holder?.hostname ?? 'unknown host'} ` +
        `since ${holder?.acquiredAt ?? 'unknown time'}`,
    )
  }
}

export class WorkflowStateLockFormatError extends Error {
  override readonly name = 'WorkflowStateLockFormatError'

  constructor(path: string, holder: WorkflowStateLockRecord | null) {
    super(
      `${path} is a legacy plain-file lock from an earlier workflow CLI build ` +
        `(holder pid ${holder?.pid ?? 'unknown'} on ${holder?.hostname ?? 'unknown host'} ` +
        `since ${holder?.acquiredAt ?? 'unknown time'}); this build locks with a directory ` +
        'at that path. Stop any older workflow CLI using this state, then remove the file.',
    )
  }
}

export class WorkflowStateStore {
  readonly path: string
  private readonly lockTimeoutMs: number
  private readonly lockStaleMs: number

  constructor(path: string, options: WorkflowStateStoreOptions = {}) {
    this.path = path
    this.lockTimeoutMs = options.lockTimeoutMs ?? 30_000
    this.lockStaleMs = options.lockStaleMs ?? 5 * 60 * 1000
  }

  async read(): Promise<WorkflowState | null> {
    try {
      return parseWorkflowStateJson(await readFile(this.path, 'utf8'))
    } catch (error) {
      if (hasErrorCode(error, 'ENOENT')) return null
      throw error
    }
  }

  async transact<T>(
    fallback: WorkflowState,
    transition: (state: WorkflowState) => { state: WorkflowState; value: T },
  ): Promise<T> {
    const release = await acquireWorkflowStateLock(this.path, {
      timeoutMs: this.lockTimeoutMs,
      staleMs: this.lockStaleMs,
    })
    let next: { state: WorkflowState; value: T }
    try {
      const current = (await this.read()) ?? fallback
      next = transition(current)
      await this.write(next.state)
    } catch (error) {
      await release().catch((releaseError: unknown) => warnWorkflowStateLockRelease(releaseError))
      throw error
    }
    // The transition is committed; a failed release must not report it as failed.
    await release().catch((releaseError: unknown) => warnWorkflowStateLockRelease(releaseError))
    return next.value
  }

  async write(state: WorkflowState): Promise<void> {
    await writeWorkflowStateFile(this.path, state)
  }
}

// The lock algorithm lives in `workflow-state-lock.mts` as a pure state machine (with its
// safety argument); this module performs its filesystem operations. It needs a local
// filesystem where creating a hard link is atomic and exclusive, records are immutable once
// linked, and one listing of the small lock directory is a consistent snapshot; network
// filesystems such as NFS are not supported.
type WorkflowStateLockEntry =
  | ({ readonly state: 'held' } & WorkflowStateLockRecord)
  | { readonly state: 'free'; readonly token: string }

interface WorkflowStateLockSlot {
  readonly generation: number
  readonly entry: WorkflowStateLockEntry | null
  readonly mtimeMs: number
}

const localHostname = hostname()
const lockRetryMs = 10
const lockGenerationPattern = /^\d+$/
// Temporary records exist only between writing and linking them, so one this old was left
// by a crashed process. Fixed, so a short stale-holder limit never removes a live one.
const abandonedLockTempMs = 10 * 60 * 1000

async function acquireWorkflowStateLock(
  path: string,
  options: { readonly timeoutMs: number; readonly staleMs: number },
): Promise<() => Promise<void>> {
  const lockDir = `${path}.lock`
  await ensureWorkflowStateLockDir(lockDir)
  const created: { held: WorkflowStateLockHeld | null } = { held: null }
  let machine: WorkflowStateLockMachine
  try {
    machine = await runWorkflowStateLock(
      lockDir,
      startWorkflowStateLockAcquire(),
      options,
      Date.now(),
      created,
    )
    if (machine.step !== 'acquired') throw new Error(`workflow state lock ended ${machine.step}`)
  } catch (error) {
    // A filesystem error after creating a held slot must not leave it looking held by this
    // live process. Releasing a slot is safe whether or not the claim was confirmed.
    if (created.held) {
      const held = created.held
      await runWorkflowStateLock(lockDir, startWorkflowStateLockRelease(held), options, null).catch(
        (releaseError: unknown) => warnWorkflowStateLockRelease(releaseError),
      )
    }
    throw error
  }
  const held = { generation: machine.generation, token: machine.token }
  await removeAbandonedWorkflowStateLockTemps(lockDir).catch(() => {})
  return async () => {
    await runWorkflowStateLock(lockDir, startWorkflowStateLockRelease(held), options, null)
  }
}

async function runWorkflowStateLock(
  lockDir: string,
  start: WorkflowStateLockMachine,
  options: { readonly timeoutMs: number; readonly staleMs: number },
  startedAt: number | null,
  created: { held: WorkflowStateLockHeld | null } = { held: null },
): Promise<WorkflowStateLockMachine> {
  const context: WorkflowStateLockContext<WorkflowStateLockSlot> = {
    isAvailable: (slot) => isWorkflowStateLockAvailable(slot, options.staleMs),
    tokenOf: (slot) => slot.entry?.token ?? null,
  }
  let machine = start
  let holder: WorkflowStateLockRecord | null = null
  while (true) {
    const operation = workflowStateLockOperation(machine)
    if (!operation) return machine
    let outcome: WorkflowStateLockOutcome<WorkflowStateLockSlot>
    switch (operation.type) {
      case 'list':
        outcome = { type: 'list', generations: await listWorkflowStateLockGenerations(lockDir) }
        break
      case 'read': {
        const record = await readWorkflowStateLockGeneration(lockDir, operation.generation)
        if (record?.entry?.state === 'held') holder = record.entry
        outcome = { type: 'read', record }
        break
      }
      case 'wait': {
        const elapsedMs = startedAt === null ? 0 : Date.now() - startedAt
        if (elapsedMs >= options.timeoutMs) {
          throw new WorkflowStateLockTimeoutError(lockDir, options.timeoutMs, holder)
        }
        await delay(Math.min(lockRetryMs, options.timeoutMs - elapsedMs))
        outcome = { type: 'wait' }
        break
      }
      case 'create': {
        const token = randomUUID()
        const entry: WorkflowStateLockEntry =
          operation.kind === 'held'
            ? {
                state: 'held',
                pid: process.pid,
                hostname: localHostname,
                acquiredAt: new Date().toISOString(),
                token,
              }
            : { state: 'free', token }
        const linked = await linkWorkflowStateLockGeneration(
          lockDir,
          operation.generation,
          entry,
          () => {
            if (operation.kind === 'held')
              created.held = { generation: operation.generation, token }
          },
        )
        outcome = { type: 'create', token: linked ? token : null }
        break
      }
      case 'remove':
        await rm(join(lockDir, String(operation.generation)), { force: true })
        outcome = { type: 'remove' }
        break
    }
    machine = advanceWorkflowStateLock(machine, outcome, context)
  }
}

// Best effort: temporary records and staging directories left by crashed processes.
async function removeAbandonedWorkflowStateLockTemps(lockDir: string): Promise<void> {
  const prefix = `${basename(lockDir)}.`
  const candidates = [
    ...(await readdir(lockDir))
      .filter((name) => name.endsWith('.tmp'))
      .map((name) => join(lockDir, name)),
    ...(await readdir(dirname(lockDir)))
      .filter((name) => name.startsWith(prefix) && name.endsWith('.tmp'))
      .map((name) => join(dirname(lockDir), name)),
  ]
  for (const candidate of candidates) {
    try {
      if (Date.now() - (await stat(candidate)).mtimeMs >= abandonedLockTempMs) {
        await rm(candidate, { recursive: true, force: true })
      }
    } catch (error) {
      if (!hasErrorCode(error, 'ENOENT')) throw error
    }
  }
}

async function ensureWorkflowStateLockDir(lockDir: string): Promise<void> {
  await mkdir(dirname(lockDir), { recursive: true, mode: 0o700 })
  try {
    if ((await stat(lockDir)).isDirectory()) return
    await rejectLegacyWorkflowStateLock(lockDir)
  } catch (error) {
    if (!hasErrorCode(error, 'ENOENT')) throw error
  }
  // Publish the directory together with its first (free) generation, so a lock directory
  // is never observed without a current generation.
  const staging = `${lockDir}.${process.pid}.${randomUUID()}.tmp`
  await mkdir(staging, { mode: 0o700 })
  try {
    await writeWorkflowStateLockEntry(join(staging, '0'), { state: 'free', token: randomUUID() })
    await rename(staging, lockDir)
  } catch (error) {
    if (hasErrorCode(error, 'ENOTDIR')) await rejectLegacyWorkflowStateLock(lockDir)
    if (!hasErrorCode(error, 'EEXIST') && !hasErrorCode(error, 'ENOTEMPTY')) throw error
  } finally {
    await rm(staging, { recursive: true, force: true })
  }
}

// An earlier build of this lock used a plain file at the same path. Its holder may still be
// running that build, which cannot see this directory lock, so removing the file is left to
// the operator instead of being guessed here.
async function rejectLegacyWorkflowStateLock(lockDir: string): Promise<never> {
  let holder: WorkflowStateLockRecord | null = null
  try {
    const entry = parseWorkflowStateLockEntry(
      JSON.stringify({ state: 'held', ...JSON.parse(await readFile(lockDir, 'utf8')) }),
    )
    if (entry?.state === 'held') holder = entry
  } catch {
    // Report the legacy lock even when its record cannot be read.
  }
  throw new WorkflowStateLockFormatError(lockDir, holder)
}

async function linkWorkflowStateLockGeneration(
  lockDir: string,
  generation: number,
  entry: WorkflowStateLockEntry,
  onLinked: () => void = () => {},
): Promise<boolean> {
  const tempPath = join(lockDir, `${generation}.${process.pid}.${randomUUID()}.tmp`)
  try {
    await writeWorkflowStateLockEntry(tempPath, entry)
    // Hard-linking a complete record keeps exclusive-create semantics without ever
    // exposing a partially written generation.
    await link(tempPath, join(lockDir, String(generation)))
    onLinked()
    return true
  } catch (error) {
    if (hasErrorCode(error, 'EEXIST')) return false
    throw error
  } finally {
    // Best effort: a leftover temporary record is harmless and removed later by age.
    await rm(tempPath, { force: true }).catch(() => {})
  }
}

// Releasing writes no state, so a failure to release is reported without discarding the
// transaction's outcome. The lock is then taken over once this process exits (or after the
// stale limit).
function warnWorkflowStateLockRelease(error: unknown): void {
  process.emitWarning(
    `workflow state lock release failed: ${error instanceof Error ? error.message : String(error)}`,
    { code: 'CLAUDE_CODEX_WORKFLOW_LOCK_RELEASE' },
  )
}

async function writeWorkflowStateLockEntry(
  path: string,
  entry: WorkflowStateLockEntry,
): Promise<void> {
  const handle = await open(path, 'wx', 0o600)
  try {
    await handle.writeFile(JSON.stringify(entry), 'utf8')
    await handle.sync()
  } finally {
    await handle.close()
  }
}

async function listWorkflowStateLockGenerations(lockDir: string): Promise<number[]> {
  return (await readdir(lockDir))
    .filter((name) => lockGenerationPattern.test(name))
    .map(Number)
    .filter(Number.isSafeInteger)
}

async function readWorkflowStateLockGeneration(
  lockDir: string,
  generation: number,
): Promise<WorkflowStateLockSlot | null> {
  const path = join(lockDir, String(generation))
  try {
    const info = await stat(path)
    const text = await readFile(path, 'utf8')
    return { generation, entry: parseWorkflowStateLockEntry(text), mtimeMs: info.mtimeMs }
  } catch (error) {
    if (hasErrorCode(error, 'ENOENT')) return null
    throw error
  }
}

function parseWorkflowStateLockEntry(text: string): WorkflowStateLockEntry | null {
  let value: unknown
  try {
    value = JSON.parse(text)
  } catch {
    return null
  }
  if (!isRecord(value) || typeof value.token !== 'string') return null
  if (value.state === 'free') return { state: 'free', token: value.token }
  if (
    value.state !== 'held' ||
    typeof value.pid !== 'number' ||
    !Number.isInteger(value.pid) ||
    value.pid <= 0 ||
    typeof value.hostname !== 'string' ||
    typeof value.acquiredAt !== 'string'
  ) {
    return null
  }
  return {
    state: 'held',
    pid: value.pid,
    hostname: value.hostname,
    acquiredAt: value.acquiredAt,
    token: value.token,
  }
}

function isWorkflowStateLockAvailable(head: WorkflowStateLockSlot, staleMs: number): boolean {
  const entry = head.entry
  if (entry?.state === 'free') return true
  if (entry?.hostname === localHostname) {
    try {
      process.kill(entry.pid, 0)
    } catch (error) {
      if (hasErrorCode(error, 'ESRCH')) return true
    }
  }
  // A record without a readable acquisition time goes stale by its file age.
  const parsedMs = entry ? Date.parse(entry.acquiredAt) : Number.NaN
  const acquiredAtMs = Number.isFinite(parsedMs) ? parsedMs : head.mtimeMs
  return Number.isFinite(acquiredAtMs) && Date.now() - acquiredAtMs >= staleMs
}

export function parseWorkflowStateJson(text: string): WorkflowState {
  let value: unknown
  try {
    value = JSON.parse(text)
  } catch (error) {
    if (error instanceof SyntaxError) {
      throw new WorkflowStateParseError([], `invalid JSON: ${error.message}`)
    }
    throw error
  }
  return parseWorkflowState(value, [])
}

export async function writeWorkflowStateFile(path: string, state: WorkflowState): Promise<void> {
  const directory = dirname(path)
  await mkdir(directory, { recursive: true, mode: 0o700 })
  const tempPath = join(directory, `.${basename(path)}.${process.pid}.${randomUUID()}.tmp`)
  try {
    const handle = await open(tempPath, 'w', 0o600)
    try {
      await handle.writeFile(formatWorkflowStateJson(state), 'utf8')
      await handle.sync()
    } finally {
      await handle.close()
    }
    await rename(tempPath, path)
  } catch (error) {
    await rm(tempPath, { force: true })
    throw error
  }
}

function parseWorkflowState(value: unknown, path: readonly string[]): WorkflowState {
  const record = requireRecord(value, path)
  if (record.version !== WORKFLOW_STATE_VERSION) {
    throw new WorkflowStateParseError(pathFor(path, 'version'), 'version must be 1')
  }
  const tasks = requireArray(record, 'tasks', path)
  return {
    version: WORKFLOW_STATE_VERSION,
    tasks: tasks.map((task, index) =>
      parseWorkflowTask(task, pathFor(pathFor(path, 'tasks'), index)),
    ),
  }
}

function parseWorkflowTask(value: unknown, path: readonly string[]): WorkflowTask {
  const record = requireRecord(value, path)
  return {
    id: requireNonEmptyString(record, 'id', path),
    status: requireWorkflowTaskStatus(record, path),
    updatedAt: requireIsoString(record, 'updatedAt', path),
    prompt: requireNonEmptyString(record, 'prompt', path),
    priority: requireInteger(record, 'priority', path),
    attempts: requireInteger(record, 'attempts', path),
    maxAttempts: requirePositiveInteger(record, 'maxAttempts', path),
    lease: parseNullableLease(record.lease, pathFor(path, 'lease')),
    blockedReason: requireNullableString(record, 'blockedReason', path),
    lastError: requireNullableString(record, 'lastError', path),
  }
}

function parseNullableLease(value: unknown, path: readonly string[]): WorkflowLease | null {
  if (value === null) return null
  const record = requireRecord(value, path)
  return {
    workerId: requireNonEmptyString(record, 'workerId', path),
    expiresAt: requireIsoString(record, 'expiresAt', path),
    renewedAt: requireIsoString(record, 'renewedAt', path),
  }
}

function formatWorkflowStateJson(state: WorkflowState): string {
  const text = JSON.stringify(state, null, 2)
  if (text === undefined) throw new WorkflowStateSerializeError()
  return `${text}\n`
}

function requireRecord(value: unknown, path: readonly string[]): Record<string, unknown> {
  if (isRecord(value)) return value
  throw new WorkflowStateParseError(path, 'expected an object')
}

function requireArray(
  record: Record<string, unknown>,
  field: string,
  path: readonly string[],
): readonly unknown[] {
  const value = record[field]
  if (Array.isArray(value)) return value
  throw new WorkflowStateParseError(pathFor(path, field), 'expected an array')
}

function requireNonEmptyString(
  record: Record<string, unknown>,
  field: string,
  path: readonly string[],
): string {
  const value = record[field]
  if (typeof value === 'string' && value.trim() !== '') return value
  throw new WorkflowStateParseError(pathFor(path, field), 'expected a non-empty string')
}

function requireNullableString(
  record: Record<string, unknown>,
  field: string,
  path: readonly string[],
): string | null {
  const value = record[field]
  if (value === null || typeof value === 'string') return value
  throw new WorkflowStateParseError(pathFor(path, field), 'expected a string or null')
}

function requireIsoString(
  record: Record<string, unknown>,
  field: string,
  path: readonly string[],
): string {
  const value = requireNonEmptyString(record, field, path)
  if (!Number.isNaN(Date.parse(value))) return value
  throw new WorkflowStateParseError(pathFor(path, field), 'expected an ISO timestamp')
}

function requireInteger(
  record: Record<string, unknown>,
  field: string,
  path: readonly string[],
): number {
  const value = record[field]
  if (typeof value === 'number' && Number.isInteger(value)) return value
  throw new WorkflowStateParseError(pathFor(path, field), 'expected an integer')
}

function requirePositiveInteger(
  record: Record<string, unknown>,
  field: string,
  path: readonly string[],
): number {
  const value = requireInteger(record, field, path)
  if (value > 0) return value
  throw new WorkflowStateParseError(pathFor(path, field), 'expected a positive integer')
}

function requireWorkflowTaskStatus(
  record: Record<string, unknown>,
  path: readonly string[],
): WorkflowTaskStatus {
  const value = record.status
  if (isWorkflowTaskStatus(value)) return value
  throw new WorkflowStateParseError(
    pathFor(path, 'status'),
    `status must be one of ${WORKFLOW_TASK_STATUSES.join(', ')}`,
  )
}

function hasErrorCode(value: unknown, code: string): boolean {
  return isRecord(value) && value.code === code
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function isWorkflowTaskStatus(value: unknown): value is WorkflowTaskStatus {
  return typeof value === 'string' && workflowTaskStatusSet.has(value)
}

function pathFor(path: readonly string[], segment: string | number): readonly string[] {
  return [...path, String(segment)]
}

function formatPath(path: readonly string[]): string {
  if (path.length === 0) return '$'
  return `$.${path.join('.')}`
}

const workflowTaskStatusSet: ReadonlySet<string> = new Set(WORKFLOW_TASK_STATUSES)
