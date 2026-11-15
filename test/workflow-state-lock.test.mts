import assert from 'node:assert/strict'
import { AsyncLocalStorage } from 'node:async_hooks'
import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { mkdtemp } from 'node:fs/promises'
import { createRequire, syncBuiltinESMExports } from 'node:module'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import test from 'node:test'
import { setTimeout as delay } from 'node:timers/promises'
import {
  type WorkflowState,
  WorkflowStateStore,
  type WorkflowTask,
} from '../src/workflow-state.mjs'

// Three waiters contend for a lock abandoned by a killed process:
//   1. W1 observes the dead generation and is paused before its first lock mutation.
//   2. W2 takes over the dead generation and is paused inside its transaction.
//   3. W1 resumes and is paused again before its second lock mutation, while W3 tries
//      to acquire the lock.
// A takeover that acts on W1's stale observation lets W3 in while W2 still holds the lock.
test('stale lock takeover keeps mutual exclusion when a delayed stale observer interleaves', {
  timeout: 20_000,
}, async () => {
  const statePath = await tempStatePath()
  const deadPid = await abandonLockInKilledProcess(statePath)
  const hooks = installLockFsHooks(deadPid)
  try {
    const active = new Set<string>()
    const overlaps: string[] = []
    let w2Write: () => void = () => {}
    const w2InTransaction = deferred()
    const w3InTransaction = deferred()

    const run = (name: string, onEnter: () => void, beforeWrite?: () => Promise<void>) => {
      const store = new WorkflowStateStore(statePath, {
        lockTimeoutMs: 10_000,
        lockStaleMs: 60_000,
      })
      const write = store.write.bind(store)
      store.write = async (state) => {
        await beforeWrite?.()
        await write(state)
        active.delete(name)
      }
      return hooks.as(name, () =>
        store.transact(emptyState(), (current) => {
          if (active.size > 0) overlaps.push(`${name} entered while ${[...active].join(', ')} held`)
          active.add(name)
          onEnter()
          return { state: { ...current, tasks: [...current.tasks, task(name)] }, value: name }
        }),
      )
    }

    const w1Gate1 = hooks.gate('w1', 1)
    const w1Gate2 = hooks.gate('w1', 2)
    const w1 = run('w1', () => {})
    await w1Gate1.reached

    const w2 = run(
      'w2',
      () => w2InTransaction.resolve(),
      () =>
        new Promise<void>((resolve) => {
          w2Write = resolve
        }),
    )
    await w2InTransaction.promise

    w1Gate1.release()
    await w1Gate2.reached

    const w3 = run('w3', () => w3InTransaction.resolve())
    // A correct lock keeps W3 out while W2 holds it; give a broken one time to let W3 in.
    await Promise.race([w3InTransaction.promise, delay(500)])

    w1Gate2.release()
    w2Write()
    assert.deepEqual((await Promise.all([w1, w2, w3])).sort(), ['w1', 'w2', 'w3'])

    assert.deepEqual(overlaps, [])
    const state = await new WorkflowStateStore(statePath).read()
    assert.deepEqual(state?.tasks.map((entry) => entry.id).sort(), ['w1', 'w2', 'w3'])
  } finally {
    hooks.uninstall()
  }
})

interface Deferred {
  readonly promise: Promise<void>
  readonly resolve: () => void
}

function deferred(): Deferred {
  let resolve: () => void = () => {}
  const promise = new Promise<void>((done) => {
    resolve = done
  })
  return { promise, resolve }
}

interface LockGate {
  readonly reached: Promise<void>
  readonly release: () => void
}

interface LockFsHooks {
  as<T>(name: string, run: () => Promise<T>): Promise<T>
  gate(name: string, mutation: number): LockGate
  uninstall(): void
}

// Pauses a named waiter before its Nth filesystem mutation after it first read the dead
// holder's lock record. It hooks fs/promises rather than lock internals so the same test
// drives any file-based lock implementation.
function installLockFsHooks(deadPid: number): LockFsHooks {
  const fsp = createRequire(import.meta.url)('node:fs/promises') as Record<
    string,
    (...args: unknown[]) => Promise<unknown>
  >
  const waiter = new AsyncLocalStorage<string>()
  const sawDeadHolder = new Set<string>()
  const mutations = new Map<string, number>()
  const gates = new Map<string, { reached: () => void; released: Promise<void> }>()
  const originals = new Map<string, (...args: unknown[]) => Promise<unknown>>()

  const hook = (name: string, mutating: boolean) => {
    const original = fsp[name]
    if (!original) throw new Error(`fs/promises.${name} is missing`)
    originals.set(name, original)
    fsp[name] = async (...args: unknown[]) => {
      const current = waiter.getStore()
      if (current && mutating && sawDeadHolder.has(current)) {
        const count = (mutations.get(current) ?? 0) + 1
        mutations.set(current, count)
        const gate = gates.get(`${current}:${count}`)
        if (gate) {
          gate.reached()
          await gate.released
        }
      }
      const result = await original(...args)
      if (current && typeof result === 'string' && result.includes(`"pid":${deadPid}`)) {
        sawDeadHolder.add(current)
      }
      return result
    }
  }
  for (const name of ['readFile']) hook(name, false)
  for (const name of ['open', 'link', 'rename', 'rm', 'unlink', 'writeFile']) hook(name, true)
  syncBuiltinESMExports()

  return {
    as: (name, run) => waiter.run(name, run),
    gate(name, mutation) {
      const reached = deferred()
      const released = deferred()
      gates.set(`${name}:${mutation}`, { reached: reached.resolve, released: released.promise })
      return { reached: reached.promise, release: released.resolve }
    },
    uninstall() {
      for (const gate of gates.values()) gate.reached()
      for (const [name, original] of originals) fsp[name] = original
      syncBuiltinESMExports()
    },
  }
}

// Acquires the lock in a child process and SIGKILLs it inside the transaction, leaving a
// lock generation whose holder is a dead local process.
async function abandonLockInKilledProcess(statePath: string): Promise<number> {
  const script = `
import { WorkflowStateStore } from ${JSON.stringify(new URL('../src/workflow-state.mjs', import.meta.url).href)}
const store = new WorkflowStateStore(process.env.WORKFLOW_STATE_PATH)
store.write = () => {
  process.send('locked')
  return new Promise(() => {})
}
await store.transact({ version: 1, tasks: [] }, (state) => ({ state, value: null }))
`
  const child = spawn(process.execPath, ['--input-type=module', '--eval', script], {
    env: { ...process.env, WORKFLOW_STATE_PATH: statePath },
    stdio: ['ignore', 'ignore', 'pipe', 'ipc'],
  })
  let stderr = ''
  child.stderr?.setEncoding('utf8')
  child.stderr?.on('data', (chunk: string) => {
    stderr += chunk
  })
  const [message] = await Promise.race([
    once(child, 'message'),
    once(child, 'close').then(([code]) => {
      throw new Error(`lock holder exited before locking (${code}): ${stderr}`)
    }),
  ])
  assert.equal(message, 'locked')
  const pid = child.pid
  assert.ok(pid)
  child.kill('SIGKILL')
  await once(child, 'close')
  return pid
}

function emptyState(): WorkflowState {
  return { version: 1, tasks: [] }
}

function task(id: string): WorkflowTask {
  return {
    id,
    status: 'queued',
    updatedAt: '2026-07-04T00:00:00.000Z',
    prompt: `Task ${id}.`,
    priority: 0,
    attempts: 0,
    maxAttempts: 3,
    lease: null,
    blockedReason: null,
    lastError: null,
  }
}

async function tempStatePath(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), 'claude-codex-workflow-lock-'))
  return join(dir, 'workflow-state.json')
}
