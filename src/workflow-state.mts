import { randomUUID } from 'node:crypto'
import { link, mkdir, open, readdir, readFile, rename, rm, stat } from 'node:fs/promises'
import { hostname } from 'node:os'
import { basename, dirname, join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'

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
    try {
      const current = (await this.read()) ?? fallback
      const next = transition(current)
      await this.write(next.state)
      return next.value
    } finally {
      await release()
    }
  }

  async write(state: WorkflowState): Promise<void> {
    await writeWorkflowStateFile(this.path, state)
  }
}

// The lock is a directory of numbered generations; the highest number is the current one.
// Each generation is either held (a holder record) or free. Acquiring and releasing both
// work by exclusively creating the next generation, so two waiters acting on the same
// observation race for one slot and only one can win. A waiter with a stale observation
// can therefore only fail to create an already-taken slot; nothing ever removes the
// current generation, so a stale-lock takeover cannot displace a live holder. Old
// generations are compacted away, so a lagging process can create a missing old slot
// number again; `publishWorkflowStateLockGeneration` makes sure such a slot is never
// treated as current.
type WorkflowStateLockEntry =
  | ({ readonly state: 'held' } & WorkflowStateLockRecord)
  | { readonly state: 'free'; readonly token: string }

interface WorkflowStateLockHead {
  readonly generation: number
  readonly text: string
  readonly entry: WorkflowStateLockEntry | null
  readonly mtimeMs: number
}

interface HeldWorkflowStateLock {
  readonly generation: number
  readonly token: string
}

const localHostname = hostname()
const lockRetryMs = 10
const lockGenerationPattern = /^\d+$/

async function acquireWorkflowStateLock(
  path: string,
  options: { readonly timeoutMs: number; readonly staleMs: number },
): Promise<() => Promise<void>> {
  const lockDir = `${path}.lock`
  const startedAt = Date.now()
  await ensureWorkflowStateLockDir(lockDir)

  while (true) {
    const head = await readWorkflowStateLockHead(lockDir)
    if (head && isWorkflowStateLockAvailable(head, options.staleMs)) {
      const held = await claimWorkflowStateLockGeneration(lockDir, head)
      if (held) return () => releaseWorkflowStateLock(lockDir, held)
    }

    const elapsedMs = Date.now() - startedAt
    if (elapsedMs >= options.timeoutMs) {
      const holder = head?.entry?.state === 'held' ? head.entry : null
      throw new WorkflowStateLockTimeoutError(lockDir, options.timeoutMs, holder)
    }
    await delay(Math.min(lockRetryMs, options.timeoutMs - elapsedMs))
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

async function claimWorkflowStateLockGeneration(
  lockDir: string,
  predecessor: WorkflowStateLockHead,
): Promise<HeldWorkflowStateLock | null> {
  const generation = predecessor.generation + 1
  const entry: WorkflowStateLockEntry = {
    state: 'held',
    pid: process.pid,
    hostname: localHostname,
    acquiredAt: new Date().toISOString(),
    token: randomUUID(),
  }
  if (!(await publishWorkflowStateLockGeneration(lockDir, predecessor, entry))) return null
  return { generation, token: entry.token }
}

async function releaseWorkflowStateLock(
  lockDir: string,
  { generation, token }: HeldWorkflowStateLock,
): Promise<void> {
  const holder = await readWorkflowStateLockGeneration(lockDir, generation)
  if (holder?.entry?.token !== token) return
  // If a stale takeover already superseded this generation, publishing fails and the newer
  // holder is left alone.
  await publishWorkflowStateLockGeneration(lockDir, holder, { state: 'free', token: randomUUID() })
}

// Creates `predecessor.generation + 1` and keeps it only if it is the current generation.
//
// Let L be the newest generation that passed these checks. Nothing removes slot L while
// its holder relies on it: compaction only removes generations below the compacting
// process's own one, and a process removes its own slot only when it backs off here,
// before relying on it. A slot created from a stale observation (the observed generation
// was compacted or superseded and the next number was free again) is therefore numbered
// below L, and L, or a newer generation, is in the directory when that process lists it
// again. So a new slot that sees a higher generation backs off, and one that sees none
// was created from the current generation. Checking the predecessor first lets the
// common superseded case back off without another listing.
async function publishWorkflowStateLockGeneration(
  lockDir: string,
  predecessor: WorkflowStateLockHead,
  entry: WorkflowStateLockEntry,
): Promise<boolean> {
  const generation = predecessor.generation + 1
  if (!(await linkWorkflowStateLockGeneration(lockDir, generation, entry))) return false
  if (
    !(await isWorkflowStateLockPredecessorIntact(lockDir, predecessor)) ||
    Math.max(...(await listWorkflowStateLockGenerations(lockDir))) > generation
  ) {
    await removeOwnWorkflowStateLockGeneration(lockDir, generation, entry.token)
    return false
  }
  await compactWorkflowStateLock(lockDir, generation)
  return true
}

async function removeOwnWorkflowStateLockGeneration(
  lockDir: string,
  generation: number,
  token: string,
): Promise<void> {
  const current = await readWorkflowStateLockGeneration(lockDir, generation)
  if (current?.entry?.token === token) await rm(join(lockDir, String(generation)), { force: true })
}

async function isWorkflowStateLockPredecessorIntact(
  lockDir: string,
  predecessor: WorkflowStateLockHead,
): Promise<boolean> {
  const current = await readWorkflowStateLockGeneration(lockDir, predecessor.generation)
  return current?.text === predecessor.text
}

async function compactWorkflowStateLock(lockDir: string, current: number): Promise<void> {
  const old = (await listWorkflowStateLockGenerations(lockDir))
    .filter((generation) => generation < current)
    .sort((left, right) => left - right)
  for (const generation of old) await rm(join(lockDir, String(generation)), { force: true })
}

async function linkWorkflowStateLockGeneration(
  lockDir: string,
  generation: number,
  entry: WorkflowStateLockEntry,
): Promise<boolean> {
  const tempPath = join(lockDir, `${generation}.${process.pid}.${randomUUID()}.tmp`)
  try {
    await writeWorkflowStateLockEntry(tempPath, entry)
    // Hard-linking a complete record keeps exclusive-create semantics without ever
    // exposing a partially written generation.
    await link(tempPath, join(lockDir, String(generation)))
    return true
  } catch (error) {
    if (hasErrorCode(error, 'EEXIST')) return false
    throw error
  } finally {
    await rm(tempPath, { force: true })
  }
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

async function readWorkflowStateLockHead(lockDir: string): Promise<WorkflowStateLockHead | null> {
  const generations = await listWorkflowStateLockGenerations(lockDir)
  if (generations.length === 0) return null
  // A missing head was compacted by a newer generation; the caller retries.
  return readWorkflowStateLockGeneration(lockDir, Math.max(...generations))
}

async function readWorkflowStateLockGeneration(
  lockDir: string,
  generation: number,
): Promise<WorkflowStateLockHead | null> {
  const path = join(lockDir, String(generation))
  try {
    const info = await stat(path)
    const text = await readFile(path, 'utf8')
    return { generation, text, entry: parseWorkflowStateLockEntry(text), mtimeMs: info.mtimeMs }
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

function isWorkflowStateLockAvailable(head: WorkflowStateLockHead, staleMs: number): boolean {
  const entry = head.entry
  if (entry?.state === 'free') return true
  if (entry?.hostname === localHostname) {
    try {
      process.kill(entry.pid, 0)
    } catch (error) {
      if (hasErrorCode(error, 'ESRCH')) return true
    }
  }
  const acquiredAtMs = entry ? Date.parse(entry.acquiredAt) : head.mtimeMs
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
