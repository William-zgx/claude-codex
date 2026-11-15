import assert from 'node:assert/strict'
import { AsyncLocalStorage } from 'node:async_hooks'
import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { mkdtemp, readdir, readFile, writeFile } from 'node:fs/promises'
import { createRequire, syncBuiltinESMExports } from 'node:module'
import { hostname, tmpdir } from 'node:os'
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
    const { run, overlaps } = transactionRunner(statePath, hooks)
    let w2Write: () => void = () => {}
    const w2InTransaction = deferred()
    const w3InTransaction = deferred()

    const w1Gate1 = hooks.gateMutation('w1', 1)
    const w1Gate2 = hooks.gateMutation('w1', 2)
    const w1 = run('w1')
    await w1Gate1.reached

    const w2 = run('w2', {
      onEnter: () => w2InTransaction.resolve(),
      beforeWrite: () =>
        new Promise<void>((resolve) => {
          w2Write = resolve
        }),
    })
    await w2InTransaction.promise

    w1Gate1.release()
    await w1Gate2.reached

    const w3 = run('w3', { onEnter: () => w3InTransaction.resolve() })
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

// A holder that is slow to release re-creates an old, already compacted generation as free.
// A waiter that listed the directory earlier reads that re-created slot as the current one:
//   R holds [1] and stalls in release; T takes over [2] (R looks stale) and pauses inside;
//   V lists the directory, sees [2] and stalls before reading it; T releases to [3] free;
//   B claims [4] and pauses inside; R resumes and re-creates [2] as free, stalling before
//   its own check; V reads that [2] and creates [3], whose predecessor check passes.
test('a waiter cannot build on an old generation re-created by a lagging release', {
  timeout: 20_000,
}, async () => {
  const statePath = await tempStatePath()
  const hooks = installLockFsHooks()
  try {
    const { run, overlaps } = transactionRunner(statePath, hooks)

    const rReleasing = hooks.gateCall('r', 'open', slotTemp(2))
    const r = run('r')
    await rReleasing.reached

    const tInside = deferred()
    let tWrite: () => void = () => {}
    await delay(5)
    const t = run('t', {
      lockStaleMs: 1,
      onEnter: () => tInside.resolve(),
      beforeWrite: () =>
        new Promise<void>((resolve) => {
          tWrite = resolve
        }),
    })
    await tInside.promise

    const vListed = hooks.gateCall('v', 'stat', slot(2))
    const vInside = deferred()
    const v = run('v', { onEnter: () => vInside.resolve() })
    await vListed.reached

    tWrite()
    assert.equal(await t, 't')

    const bInside = deferred()
    let bWrite: () => void = () => {}
    const b = run('b', {
      onEnter: () => bInside.resolve(),
      beforeWrite: () =>
        new Promise<void>((resolve) => {
          bWrite = resolve
        }),
    })
    await bInside.promise

    // R has just re-created [2]; its own check (whatever it is) has not run yet.
    const rChecking = hooks.gateCall('r', 'rm', slotTemp(2))
    rReleasing.release()
    await rChecking.reached

    vListed.release()
    // A correct lock keeps V out while B holds it; give a broken one time to let V in.
    await Promise.race([vInside.promise, delay(500)])

    rChecking.release()
    bWrite()
    assert.deepEqual((await Promise.all([r, b, v])).sort(), ['b', 'r', 'v'])

    assert.deepEqual(overlaps, [])
    const state = await new WorkflowStateStore(statePath).read()
    assert.deepEqual(state?.tasks.map((entry) => entry.id).sort(), ['b', 'r', 't', 'v'])
  } finally {
    hooks.uninstall()
  }
})

// A lagging waiter re-creates an old, already compacted generation as held and crashes
// before its own check, leaving a dead holder there. A waiter that listed the directory
// earlier reads that slot, sees a dead holder and takes over the next (compacted) slot.
test('a waiter cannot take over an old generation left by a crashed lagging claimant', {
  timeout: 20_000,
}, async () => {
  const statePath = await tempStatePath()
  const lockDir = `${statePath}.lock`
  const hooks = installLockFsHooks()
  try {
    const { run, overlaps } = transactionRunner(statePath, hooks)
    assert.equal(await run('setup'), 'setup')
    assert.deepEqual(await readdir(lockDir), ['2'])

    const vListed = hooks.gateCall('v', 'stat', /\.lock\/2$/)
    const vInside = deferred()
    const v = run('v', { onEnter: () => vInside.resolve() })
    await vListed.reached

    assert.equal(await run('c'), 'c')
    const bInside = deferred()
    let bWrite: () => void = () => {}
    const b = run('b', {
      onEnter: () => bInside.resolve(),
      beforeWrite: () =>
        new Promise<void>((resolve) => {
          bWrite = resolve
        }),
    })
    await bInside.promise
    assert.deepEqual(await readdir(lockDir), ['5'])

    // What a lagging claimant leaves behind after re-creating [2] and crashing.
    await writeFile(
      join(lockDir, '2'),
      JSON.stringify({
        state: 'held',
        pid: await deadPid(),
        hostname: hostname(),
        acquiredAt: new Date().toISOString(),
        token: 'crashed-lagging-claimant',
      }),
    )

    vListed.release()
    await Promise.race([vInside.promise, delay(500)])

    bWrite()
    assert.deepEqual((await Promise.all([b, v])).sort(), ['b', 'v'])

    assert.deepEqual(overlaps, [])
    const state = await new WorkflowStateStore(statePath).read()
    assert.deepEqual(state?.tasks.map((entry) => entry.id).sort(), ['b', 'c', 'setup', 'v'])
  } finally {
    hooks.uninstall()
  }
})

test('workflow state store refuses a legacy plain-file lock with a clear error', async () => {
  const statePath = await tempStatePath()
  const legacy = JSON.stringify({
    pid: process.pid,
    hostname: hostname(),
    acquiredAt: '2026-07-04T00:00:00.000Z',
    token: 'legacy',
  })
  await writeFile(`${statePath}.lock`, legacy)
  const store = new WorkflowStateStore(statePath, { lockTimeoutMs: 1_000 })

  await assert.rejects(
    store.transact(emptyState(), (current) => ({ state: current, value: null })),
    (error: unknown) => {
      assert.ok(error instanceof Error)
      assert.equal(error.name, 'WorkflowStateLockFormatError')
      assert.match(error.message, /legacy plain-file lock/)
      assert.ok(error.message.includes(`${statePath}.lock`))
      assert.ok(error.message.includes(`pid ${process.pid}`))
      return true
    },
  )
  assert.equal(await readFile(`${statePath}.lock`, 'utf8'), legacy)
  assert.equal(await store.read(), null)
})

// Finding A on 2eea8f66: R releases [1] by creating free [2]; W reads [2] and creates [3].
// If R then sees [3] above its free slot and removes [2], W's check finds its predecessor
// gone and removes [3] too, leaving [1] held by R, a live process that believes it
// released. Every later transaction, including R's own next one, waits for the age limit.
test('a release that a newer claim overtakes does not strand the lock', {
  timeout: 20_000,
}, async () => {
  const statePath = await tempStatePath()
  const hooks = installLockFsHooks()
  try {
    const { run, overlaps } = transactionRunner(statePath, hooks)
    const rLinked = hooks.gateAfterCall('r', 'link', slotTemp(2))
    const r = run('r')
    await rLinked.reached

    const wLinked = hooks.gateAfterCall('w', 'link', slotTemp(3))
    const w = run('w', { lockTimeoutMs: 2_000 })
    await wLinked.reached
    assert.ok(wLinked.succeeded())

    rLinked.release()
    assert.equal(await r, 'r')
    wLinked.release()
    assert.equal(await w, 'w')
    // The releasing process can take the lock again without waiting for its own record.
    assert.equal(await run('r-again', { lockTimeoutMs: 2_000 }), 'r-again')

    assert.deepEqual(overlaps, [])
    const state = await new WorkflowStateStore(statePath).read()
    assert.deepEqual(state?.tasks.map((entry) => entry.id).sort(), ['r', 'r-again', 'w'])
  } finally {
    hooks.uninstall()
  }
})

// Finding B on 2eea8f66: the double back-off of finding A lowers the highest generation
// from [3] to [1] while D holds an old observation of free [2]. R's process exits, W takes
// over [1] and holds [2]; D creates [3] from its old observation and crashes before its
// check; X then sees a dead holder at [3], creates [4], and enters while W is inside.
test('an overtaken release cannot let a crashed stale claimant open the lock', {
  timeout: 20_000,
}, async () => {
  const statePath = await tempStatePath()
  const lockDir = `${statePath}.lock`
  const hooks = installLockFsHooks()
  try {
    const { run, overlaps } = transactionRunner(statePath, hooks)
    const rLinked = hooks.gateAfterCall('r', 'link', slotTemp(2))
    const r = run('r')
    await rLinked.reached

    const dObserved = hooks.gateCall('d', 'open', slotTemp(3))
    const d = run('d')
    await dObserved.reached

    const wLinked = hooks.gateAfterCall('w', 'link', slotTemp(3))
    const wInside = deferred()
    let wWrite: () => void = () => {}
    const w = run('w', {
      onEnter: () => wInside.resolve(),
      beforeWrite: () =>
        new Promise<void>((resolve) => {
          wWrite = resolve
        }),
    })
    await wLinked.reached

    rLinked.release()
    assert.equal(await r, 'r')
    // R's process exits; its records now belong to a dead process.
    const exited = await deadPid()
    await rewriteHolderPid(join(lockDir, '1'), exited)

    wLinked.release()
    await wInside.promise

    const dLinked = hooks.gateAfterCall('d', 'link', slotTemp(3))
    dObserved.release()
    await dLinked.reached
    // D crashes right after creating its slot, before any check of its own.
    if (dLinked.succeeded()) await rewriteHolderPid(join(lockDir, '3'), await deadPid())

    const xInside = deferred()
    const x = run('x', { onEnter: () => xInside.resolve() })
    // A correct lock keeps X out while W holds it; give a broken one time to let X in.
    await Promise.race([xInside.promise, delay(500)])
    assert.deepEqual(overlaps, [])

    wWrite()
    dLinked.release()
    assert.deepEqual((await Promise.all([w, d, x])).sort(), ['d', 'w', 'x'])
    assert.deepEqual(overlaps, [])
    const state = await new WorkflowStateStore(statePath).read()
    assert.deepEqual(state?.tasks.map((entry) => entry.id).sort(), ['d', 'r', 'w', 'x'])
  } finally {
    hooks.uninstall()
  }
})

test('a filesystem error right after claiming does not leave the lock held', {
  timeout: 20_000,
}, async () => {
  const statePath = await tempStatePath()
  const hooks = installLockFsHooks()
  try {
    const { run, overlaps } = transactionRunner(statePath, hooks)
    const linked = hooks.gateAfterCall('f', 'link', slotTemp(1))
    const failing = run('f')
    await linked.reached
    hooks.failCall('f', 'readdir', /\.lock$/)
    linked.release()
    await assert.rejects(failing, /injected EIO in readdir/)

    // The same process can take the lock again without waiting for its own record.
    assert.equal(await run('next', { lockTimeoutMs: 1_000 }), 'next')
    assert.deepEqual(overlaps, [])
  } finally {
    hooks.uninstall()
  }
})

async function rewriteHolderPid(path: string, pid: number): Promise<void> {
  let text: string
  try {
    text = await readFile(path, 'utf8')
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return
    throw error
  }
  await writeFile(path, JSON.stringify({ ...JSON.parse(text), pid }))
}

interface RunOptions {
  readonly lockStaleMs?: number
  readonly lockTimeoutMs?: number
  readonly onEnter?: () => void
  readonly beforeWrite?: () => Promise<void>
}

// Runs named transactions that each append a task, recording any transaction that starts
// while another one is still between its transition and its write.
function transactionRunner(
  statePath: string,
  hooks: LockFsHooks,
): {
  run(name: string, options?: RunOptions): Promise<string>
  readonly overlaps: string[]
} {
  const active = new Set<string>()
  const overlaps: string[] = []
  const run = (name: string, options: RunOptions = {}) => {
    const store = new WorkflowStateStore(statePath, {
      lockTimeoutMs: options.lockTimeoutMs ?? 10_000,
      lockStaleMs: options.lockStaleMs ?? 60_000,
    })
    const write = store.write.bind(store)
    store.write = async (state) => {
      await options.beforeWrite?.()
      await write(state)
      active.delete(name)
    }
    return hooks.as(name, () =>
      store.transact(emptyState(), (current) => {
        if (active.size > 0) overlaps.push(`${name} entered while ${[...active].join(', ')} held`)
        active.add(name)
        options.onEnter?.()
        return { state: { ...current, tasks: [...current.tasks, task(name)] }, value: name }
      }),
    )
  }
  return { run, overlaps }
}

async function deadPid(): Promise<number> {
  const child = spawn(process.execPath, ['-e', ''])
  await once(child, 'close')
  assert.ok(child.pid)
  return child.pid
}

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
  gateMutation(name: string, mutation: number): LockGate
  gateCall(name: string, operation: string, path: RegExp): LockGate
  gateAfterCall(
    name: string,
    operation: string,
    path: RegExp,
  ): LockGate & { readonly succeeded: () => boolean }
  // Makes the named waiter's next matching call fail with EIO.
  failCall(name: string, operation: string, path: RegExp): void
  uninstall(): void
}

const slot = (generation: number) => new RegExp(`\\.lock/${generation}$`)
const slotTemp = (generation: number) => new RegExp(`\\.lock/${generation}\\.\\d+\\..*\\.tmp$`)

// Hooks fs/promises to pause a named waiter at a chosen point:
// - gateMutation: before its Nth filesystem mutation after it first read a lock record of
//   `deadPid`, which does not depend on the lock's internal layout;
// - gateCall: at its next call of `operation` on a path matching `path`, armed from the
//   moment the gate is created;
// - gateAfterCall: the same, but after that call returned or failed.
function installLockFsHooks(deadPid?: number): LockFsHooks {
  const fsp = createRequire(import.meta.url)('node:fs/promises') as Record<
    string,
    (...args: unknown[]) => Promise<unknown>
  >
  const waiter = new AsyncLocalStorage<string>()
  const sawDeadHolder = new Set<string>()
  const mutations = new Map<string, number>()
  const mutationGates = new Map<string, { reached: () => void; released: Promise<void> }>()
  const callGates: {
    name: string
    operation: string
    path: RegExp
    after: boolean
    reached: () => void
    released: Promise<void>
    result?: (succeeded: boolean) => void
  }[] = []
  const originals = new Map<string, (...args: unknown[]) => Promise<unknown>>()
  const failures: { name: string; operation: string; path: RegExp }[] = []

  const hook = (operation: string, mutating: boolean) => {
    const original = fsp[operation]
    if (!original) throw new Error(`fs/promises.${operation} is missing`)
    originals.set(operation, original)
    fsp[operation] = async (...args: unknown[]) => {
      const current = waiter.getStore()
      if (current && mutating && sawDeadHolder.has(current)) {
        const count = (mutations.get(current) ?? 0) + 1
        mutations.set(current, count)
        const gate = mutationGates.get(`${current}:${count}`)
        if (gate) {
          gate.reached()
          await gate.released
        }
      }
      const target = String(args[0])
      const take = (after: boolean) => {
        const index = callGates.findIndex(
          (gate) =>
            gate.name === current &&
            gate.operation === operation &&
            gate.after === after &&
            gate.path.test(target),
        )
        return index >= 0 ? callGates.splice(index, 1)[0] : undefined
      }
      const failure = failures.findIndex(
        (entry) =>
          entry.name === current && entry.operation === operation && entry.path.test(target),
      )
      if (failure >= 0) {
        failures.splice(failure, 1)
        throw Object.assign(new Error(`injected EIO in ${operation}`), { code: 'EIO' })
      }
      const before = take(false)
      if (before) {
        before.reached()
        await before.released
      }
      const after = take(true)
      let result: unknown
      try {
        result = await original(...args)
      } catch (error) {
        if (after) {
          after.result?.(false)
          after.reached()
          await after.released
        }
        throw error
      }
      if (after) {
        after.result?.(true)
        after.reached()
        await after.released
      }
      if (
        current &&
        deadPid !== undefined &&
        typeof result === 'string' &&
        result.includes(`"pid":${deadPid}`)
      ) {
        sawDeadHolder.add(current)
      }
      return result
    }
  }
  for (const operation of ['readFile', 'readdir', 'stat']) hook(operation, false)
  for (const operation of ['open', 'link', 'rename', 'rm', 'unlink', 'writeFile']) {
    hook(operation, true)
  }
  syncBuiltinESMExports()

  const releases: (() => void)[] = []
  const newGate = () => {
    const reached = deferred()
    const released = deferred()
    releases.push(released.resolve)
    return { reached, released }
  }
  return {
    as: (name, run) => waiter.run(name, run),
    gateMutation(name, mutation) {
      const { reached, released } = newGate()
      mutationGates.set(`${name}:${mutation}`, {
        reached: reached.resolve,
        released: released.promise,
      })
      return { reached: reached.promise, release: released.resolve }
    },
    gateCall(name, operation, path) {
      const { reached, released } = newGate()
      callGates.push({
        name,
        operation,
        path,
        after: false,
        reached: reached.resolve,
        released: released.promise,
      })
      return { reached: reached.promise, release: released.resolve }
    },
    gateAfterCall(name, operation, path) {
      const { reached, released } = newGate()
      let succeeded = false
      callGates.push({
        name,
        operation,
        path,
        after: true,
        reached: reached.resolve,
        released: released.promise,
        result: (value) => {
          succeeded = value
        },
      })
      return { reached: reached.promise, release: released.resolve, succeeded: () => succeeded }
    },
    failCall(name, operation, path) {
      failures.push({ name, operation, path })
    },
    uninstall() {
      for (const release of releases) release()
      for (const [operation, original] of originals) fsp[operation] = original
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
