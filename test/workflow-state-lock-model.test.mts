import assert from 'node:assert/strict'
import test from 'node:test'
import {
  advanceWorkflowStateLock,
  startWorkflowStateLockAcquire,
  startWorkflowStateLockRelease,
  type WorkflowStateLockContext,
  type WorkflowStateLockEntryKind,
  type WorkflowStateLockHeld,
  type WorkflowStateLockMachine,
  type WorkflowStateLockOperation,
  type WorkflowStateLockOutcome,
  workflowStateLockOperation,
} from '../src/workflow-state-lock.mjs'

// An exhaustive explorer for the workflow state lock. Each actor is a process that runs a
// number of transactions (acquire, critical section, release). Every lock step is one
// atomic operation on an in-memory directory, and the explorer tries every interleaving of
// those steps. At every point any live actor may also crash (its records then belong to a
// dead holder), and a bounded number of held records may become stale by age.
//
// Properties, checked on every reachable state:
// - mutual exclusion: at most one live actor is in its critical section, unless the
//   other's held record became stale by age (a documented limit of age-based takeover);
// - no permanent wedge: from every reachable state, the live actors can all finish their
//   transactions without any further crash or staleness. Waiting on a live process that
//   no longer holds the lock until the age limit expires counts as a wedge;
// - no livelock: no cycle of ordinary steps changes the directory. Every transaction
//   count is part of the state, so such a cycle is an endless run in which the lock keeps
//   changing hands or slots while no transaction completes. (Generation numbers are
//   normalized, so endless claiming shows up as a cycle.) Cycles that only read are
//   processes polling a holder that the scheduler does not run.

interface ModelRecord {
  readonly generation: number
  readonly kind: WorkflowStateLockEntryKind
  readonly owner: number
  readonly token: string
}

interface LockAlgorithm<M> {
  startAcquire(): M
  startRelease(held: WorkflowStateLockHeld): M
  operation(machine: M): WorkflowStateLockOperation | null
  advance(
    machine: M,
    outcome: WorkflowStateLockOutcome<ModelRecord>,
    context: WorkflowStateLockContext<ModelRecord>,
  ): M
  acquired(machine: M): WorkflowStateLockHeld | null
  released(machine: M): boolean
}

// A crashed actor keeps no local state, so all crashes of one actor reach the same state.
interface ModelActor<M> {
  readonly alive: boolean
  readonly phase: 'acquire' | 'critical' | 'release' | 'done' | 'dead'
  readonly machine: M | null
  readonly held: WorkflowStateLockHeld | null
  readonly transactions: number
}

interface ModelState<M> {
  readonly slots: readonly ModelRecord[]
  readonly actors: readonly ModelActor<M>[]
  readonly aged: readonly string[]
}

const deadActor = {
  alive: false,
  phase: 'dead',
  machine: null,
  held: null,
  transactions: 0,
} as const

interface ExploreBounds {
  readonly transactions: readonly number[]
  readonly crashes: boolean
  readonly staleTakeovers: number
  readonly maxGeneration: number
  readonly maxStates: number
  // Stop at the first overlap instead of building the whole state graph (no wedge check).
  readonly stopAtOverlap?: boolean
}

interface ExploreResult {
  readonly states: number
  readonly edges: number
  readonly truncated: number
  readonly overlap: readonly string[] | null
  readonly wedge: readonly string[] | null
  readonly livelock: readonly string[] | null
}

type EdgeKind = 'step' | 'crash' | 'stale'

interface Successor<M> {
  readonly kind: EdgeKind
  readonly label: string
  readonly next: ModelState<M>
  readonly mutates: boolean
}

function explore<M>(algorithm: LockAlgorithm<M>, bounds: ExploreBounds): ExploreResult {
  const initial: ModelState<M> = {
    slots: [{ generation: 0, kind: 'free', owner: -1, token: '#0' }],
    actors: bounds.transactions.map((transactions) => ({
      alive: true,
      phase: 'acquire',
      machine: algorithm.startAcquire(),
      held: null,
      transactions,
    })),
    aged: [],
  }
  const ids = new Map<string, number>()
  const states: ModelState<M>[] = []
  const parents: { parent: number; label: string }[] = []
  const stepEdges: number[][] = []
  const mutatingEdges: Set<number>[] = []
  const truncated = new Set<number>()
  let edges = 0
  let overlapAt: number | null = null

  const intern = (state: ModelState<M>, parent: number, label: string): number => {
    const key = canonical(state)
    const known = ids.get(key)
    if (known !== undefined) return known
    const id = states.length
    ids.set(key, id)
    states.push(state)
    parents.push({ parent, label })
    stepEdges.push([])
    mutatingEdges.push(new Set())
    return id
  }

  intern(initial, -1, 'start')
  for (let id = 0; id < states.length; id += 1) {
    if (states.length > bounds.maxStates) throw new Error(`more than ${bounds.maxStates} states`)
    const state = states[id] as ModelState<M>
    if (overlapAt === null && hasOverlap(state)) {
      overlapAt = id
      if (bounds.stopAtOverlap) break
    }
    const successors = successorsOf(algorithm, state, bounds)
    if (successors === null) {
      truncated.add(id)
      continue
    }
    for (const { kind, label, next, mutates } of successors) {
      const nextId = intern(next, id, label)
      edges += 1
      if (kind === 'step') stepEdges[id]?.push(nextId)
      if (kind === 'step' && mutates) mutatingEdges[id]?.add(nextId)
    }
  }

  // Backward reachability of "every live actor is done" over ordinary steps only.
  // Truncated states are optimistically assumed to finish.
  const reverse: number[][] = states.map(() => [])
  stepEdges.forEach((targets, from) => {
    for (const to of targets) reverse[to]?.push(from)
  })
  const canFinish = new Set<number>()
  const queue: number[] = []
  states.forEach((state, id) => {
    if (
      truncated.has(id) ||
      state.actors.every((actor) => !actor.alive || actor.phase === 'done')
    ) {
      canFinish.add(id)
      queue.push(id)
    }
  })
  while (queue.length > 0) {
    const id = queue.pop() as number
    for (const from of reverse[id] ?? []) {
      if (!canFinish.has(from)) {
        canFinish.add(from)
        queue.push(from)
      }
    }
  }
  const wedgeAt = bounds.stopAtOverlap ? -1 : states.findIndex((_, id) => !canFinish.has(id))

  const trace = (id: number): string[] => {
    const labels: string[] = []
    for (let at = id; at > 0; at = parents[at]?.parent ?? 0) {
      labels.unshift(parents[at]?.label ?? '?')
    }
    labels.push(`=> ${describe(states[id] as ModelState<M>)}`)
    return labels
  }
  const livelockAt = bounds.stopAtOverlap ? -1 : findMutatingCycle(stepEdges, mutatingEdges)
  return {
    states: states.length,
    edges,
    truncated: truncated.size,
    overlap: overlapAt === null ? null : trace(overlapAt),
    wedge: wedgeAt < 0 ? null : trace(wedgeAt),
    livelock: livelockAt < 0 ? null : trace(livelockAt),
  }
}

// A state on a cycle of step edges that contains a directory-changing edge, or -1.
// Uses Tarjan's strongly connected components, iteratively.
function findMutatingCycle(edges: readonly number[][], mutating: readonly Set<number>[]): number {
  const count = edges.length
  const index = new Int32Array(count).fill(-1)
  const low = new Int32Array(count)
  const component = new Int32Array(count).fill(-1)
  const onStack = new Uint8Array(count)
  const stack: number[] = []
  let nextIndex = 0
  let components = 0
  for (let root = 0; root < count; root += 1) {
    if (index[root] !== -1) continue
    const work: [number, number][] = [[root, 0]]
    while (work.length > 0) {
      const frame = work[work.length - 1] as [number, number]
      const [node, edge] = frame
      if (edge === 0) {
        index[node] = nextIndex
        low[node] = nextIndex
        nextIndex += 1
        stack.push(node)
        onStack[node] = 1
      }
      const targets = edges[node] ?? []
      if (edge < targets.length) {
        frame[1] = edge + 1
        const target = targets[edge] as number
        if (index[target] === -1) work.push([target, 0])
        else if (onStack[target]) low[node] = Math.min(low[node] ?? 0, index[target] ?? 0)
        continue
      }
      work.pop()
      const parent = work[work.length - 1]
      if (parent) low[parent[0]] = Math.min(low[parent[0]] ?? 0, low[node] ?? 0)
      if (low[node] === index[node]) {
        let member: number
        do {
          member = stack.pop() as number
          onStack[member] = 0
          component[member] = components
        } while (member !== node)
        components += 1
      }
    }
  }
  for (let from = 0; from < count; from += 1) {
    for (const to of mutating[from] ?? []) {
      if (component[from] === component[to]) return from
    }
  }
  return -1
}

function hasOverlap<M>(state: ModelState<M>): boolean {
  const inside = state.actors.filter(
    (actor) =>
      actor.alive && actor.phase === 'critical' && !state.aged.includes(actor.held?.token ?? ''),
  )
  return inside.length > 1
}

function successorsOf<M>(
  algorithm: LockAlgorithm<M>,
  state: ModelState<M>,
  bounds: ExploreBounds,
): Successor<M>[] | null {
  const successors: Successor<M>[] = []
  const context: WorkflowStateLockContext<ModelRecord> = {
    isAvailable: (record) =>
      record.kind === 'free' ||
      !(state.actors[record.owner]?.alive ?? false) ||
      state.aged.includes(record.token),
    tokenOf: (record) => record.token,
  }

  for (const [index, actor] of state.actors.entries()) {
    if (!actor.alive) continue
    const name = `p${index}`
    if (bounds.crashes) {
      successors.push({
        kind: 'crash',
        label: `${name} crashes`,
        next: withActor(state, index, deadActor),
        mutates: false,
      })
    }
    if (actor.phase === 'done') continue
    // Leaving the critical section and waiting touch nothing shared, so they are merged
    // into the next filesystem step.
    const releasing =
      actor.phase === 'critical'
        ? {
            ...actor,
            phase: 'release' as const,
            machine: algorithm.startRelease(actor.held as WorkflowStateLockHeld),
          }
        : actor
    let machine = releasing.machine as M
    let operation = algorithm.operation(machine)
    if (operation?.type === 'wait') {
      machine = algorithm.advance(machine, { type: 'wait' }, context)
      operation = algorithm.operation(machine)
    }
    if (!operation || operation.type === 'wait') {
      throw new Error(`${name} has no filesystem operation in ${actor.phase}`)
    }
    let slots = state.slots
    let outcome: WorkflowStateLockOutcome<ModelRecord>
    switch (operation.type) {
      case 'list':
        outcome = { type: 'list', generations: slots.map((slot) => slot.generation) }
        break
      case 'read':
        outcome = {
          type: 'read',
          record: slots.find((slot) => slot.generation === operation.generation) ?? null,
        }
        break
      case 'create': {
        if (operation.generation > bounds.maxGeneration + lowestGeneration(state)) return null
        if (slots.some((slot) => slot.generation === operation.generation)) {
          outcome = { type: 'create', token: null }
          break
        }
        const token = freshToken(state)
        slots = [
          ...slots,
          { generation: operation.generation, kind: operation.kind, owner: index, token },
        ].sort((left, right) => left.generation - right.generation)
        outcome = { type: 'create', token }
        break
      }
      case 'remove':
        slots = slots.filter((slot) => slot.generation !== operation.generation)
        outcome = { type: 'remove' }
        break
    }
    const advanced = algorithm.advance(machine, outcome, context)
    let next: ModelActor<M> = { ...releasing, machine: advanced }
    const acquired = releasing.phase === 'acquire' ? algorithm.acquired(advanced) : null
    if (acquired) next = { ...next, phase: 'critical', machine: null, held: acquired }
    if (releasing.phase === 'release' && algorithm.released(advanced)) {
      const transactions = releasing.transactions - 1
      next =
        transactions > 0
          ? {
              ...next,
              phase: 'acquire',
              machine: algorithm.startAcquire(),
              held: null,
              transactions,
            }
          : { ...next, phase: 'done', machine: null, held: null, transactions }
    }
    successors.push({
      kind: 'step',
      label: `${name} ${actor.phase === 'critical' ? 'leaves its transaction, ' : ''}${describeOperation(operation, outcome)}`,
      next: { ...withActor(state, index, next), slots },
      mutates: slots !== state.slots,
    })
  }

  if (state.aged.length < bounds.staleTakeovers) {
    for (const record of state.slots) {
      const owner = state.actors[record.owner]
      if (record.kind !== 'held' || !owner?.alive || state.aged.includes(record.token)) continue
      successors.push({
        kind: 'stale',
        label: `[${record.generation}] of p${record.owner} becomes stale by age`,
        next: { ...state, aged: [...state.aged, record.token].sort() },
        mutates: false,
      })
    }
  }
  return successors
}

function withActor<M>(state: ModelState<M>, index: number, actor: ModelActor<M>): ModelState<M> {
  return { ...state, actors: state.actors.map((current, at) => (at === index ? actor : current)) }
}

function describeOperation(
  operation: WorkflowStateLockOperation,
  outcome: WorkflowStateLockOutcome<ModelRecord>,
): string {
  switch (outcome.type) {
    case 'list':
      return `lists [${outcome.generations.join(',')}]`
    case 'read':
      return `reads [${'generation' in operation ? operation.generation : '?'}] ${
        outcome.record ? `${outcome.record.kind} ${outcome.record.token}` : 'missing'
      }`
    case 'create':
      return `creates [${'generation' in operation ? operation.generation : '?'}] ${
        'kind' in operation ? operation.kind : ''
      }: ${outcome.token ?? 'taken'}`
    case 'remove':
      return `removes [${'generation' in operation ? operation.generation : '?'}]`
    case 'wait':
      return 'waits'
  }
}

function describe<M>(state: ModelState<M>): string {
  const slots = state.slots
    .map((record) => `[${record.generation}] ${record.kind} ${record.token}`)
    .join(', ')
  const actors = state.actors
    .map((actor, index) => `p${index} ${actor.alive ? actor.phase : 'dead'}`)
    .join(', ')
  return `${slots} | ${actors}`
}

// Generation numbers are only compared with each other and incremented, and tokens are
// only compared for equality, so states that differ by a shift of all generation numbers
// or by a renaming of tokens behave identically and share one canonical key.
const generationKeys = new Set(['generation', 'g', 'remaining'])

function canonical<M>(state: ModelState<M>): string {
  const lowest = lowestGeneration(state)
  const names = new Map<string, string>()
  const rename = (token: string) => {
    let name = names.get(token)
    if (name === undefined) {
      name = `#${names.size}`
      names.set(token, name)
    }
    return name
  }
  const body = JSON.stringify(
    { slots: state.slots, actors: state.actors },
    (key: string, value: unknown) => {
      if (key === 'remaining' && Array.isArray(value)) {
        return value.map((generation: number) => generation - lowest)
      }
      if (typeof value === 'number' && generationKeys.has(key)) return value - lowest
      if (typeof value === 'string' && value.startsWith('#')) return rename(value)
      return value
    },
  )
  // Aged tokens no longer referenced anywhere else cannot affect any future step; the
  // number of stale takeovers used so far still bounds what is reachable.
  const aged = state.aged
    .filter((token) => names.has(token))
    .map((token) => names.get(token))
    .sort()
  return `${body}|${aged.join(',')}|${state.aged.length}`
}

function lowestGeneration<M>(state: ModelState<M>): number {
  let lowest = Number.POSITIVE_INFINITY
  JSON.stringify({ slots: state.slots, actors: state.actors }, (key: string, value: unknown) => {
    if (key === 'remaining' && Array.isArray(value)) {
      for (const generation of value as number[]) lowest = Math.min(lowest, generation)
    } else if (typeof value === 'number' && generationKeys.has(key)) {
      lowest = Math.min(lowest, value)
    }
    return value
  })
  return Number.isFinite(lowest) ? lowest : 0
}

function freshToken<M>(state: ModelState<M>): string {
  let highest = 0
  for (const match of JSON.stringify(state).matchAll(/"#(\d+)"/g)) {
    highest = Math.max(highest, Number(match[1]))
  }
  return `#${highest + 1}`
}

const productionLock: LockAlgorithm<WorkflowStateLockMachine> = {
  startAcquire: startWorkflowStateLockAcquire,
  startRelease: startWorkflowStateLockRelease,
  operation: workflowStateLockOperation,
  advance: advanceWorkflowStateLock,
  acquired: (machine) =>
    machine.step === 'acquired' ? { generation: machine.generation, token: machine.token } : null,
  released: (machine) => machine.step === 'released',
}

// The algorithm at PR head 2eea8f66, transcribed step for step: after creating a slot it
// checked the predecessor, listed again, and removed its own slot if either check failed.
type RoundTwoMachine =
  | { readonly s: 'list' }
  | { readonly s: 'read'; readonly g: number }
  | { readonly s: 'wait' }
  | {
      readonly s: 'link' | 'pred'
      readonly g: number
      readonly pred: string
      readonly kind: WorkflowStateLockEntryKind
      readonly token?: string
    }
  | {
      readonly s: 'relist' | 'removeOwnRead' | 'removeOwn' | 'compactList'
      readonly g: number
      readonly kind: WorkflowStateLockEntryKind
      readonly token: string
    }
  | {
      readonly s: 'compact'
      readonly g: number
      readonly kind: WorkflowStateLockEntryKind
      readonly token: string
      readonly remaining: readonly number[]
    }
  | { readonly s: 'checkOwn'; readonly g: number; readonly token: string }
  | { readonly s: 'acquired'; readonly g: number; readonly token: string }
  | { readonly s: 'released' }

const roundTwoLock: LockAlgorithm<RoundTwoMachine> = {
  startAcquire: () => ({ s: 'list' }),
  startRelease: (held) => ({ s: 'checkOwn', g: held.generation, token: held.token }),
  operation(machine) {
    switch (machine.s) {
      case 'list':
      case 'relist':
      case 'compactList':
        return { type: 'list' }
      case 'read':
      case 'removeOwnRead':
      case 'checkOwn':
        return { type: 'read', generation: machine.g }
      case 'pred':
        return { type: 'read', generation: machine.g - 1 }
      case 'wait':
        return { type: 'wait' }
      case 'link':
        return { type: 'create', generation: machine.g, kind: machine.kind }
      case 'removeOwn':
        return { type: 'remove', generation: machine.g }
      case 'compact':
        return { type: 'remove', generation: machine.remaining[0] ?? machine.g }
      case 'acquired':
      case 'released':
        return null
    }
  },
  advance(machine, outcome, context) {
    const gaveUp = (kind: WorkflowStateLockEntryKind): RoundTwoMachine =>
      kind === 'held' ? { s: 'wait' } : { s: 'released' }
    const done = (m: Extract<RoundTwoMachine, { s: 'compact' }>): RoundTwoMachine => {
      if (m.remaining.length > 0) return m
      return m.kind === 'held' ? { s: 'acquired', g: m.g, token: m.token } : { s: 'released' }
    }
    switch (machine.s) {
      case 'list': {
        const generations = outcome.type === 'list' ? outcome.generations : []
        if (generations.length === 0) return { s: 'wait' }
        return { s: 'read', g: Math.max(...generations) }
      }
      case 'read': {
        const record = outcome.type === 'read' ? outcome.record : null
        if (!record || !context.isAvailable(record)) return { s: 'wait' }
        return { s: 'link', g: machine.g + 1, pred: record.token, kind: 'held' }
      }
      case 'wait':
        return { s: 'list' }
      case 'link': {
        const token = outcome.type === 'create' ? outcome.token : null
        if (token === null) return gaveUp(machine.kind)
        return { ...machine, s: 'pred', token }
      }
      case 'pred': {
        const record = outcome.type === 'read' ? outcome.record : null
        const next = { g: machine.g, kind: machine.kind, token: machine.token ?? '' }
        return record?.token === machine.pred
          ? { s: 'relist', ...next }
          : { s: 'removeOwnRead', ...next }
      }
      case 'relist': {
        const generations = outcome.type === 'list' ? outcome.generations : []
        if (Math.max(...generations) > machine.g) return { ...machine, s: 'removeOwnRead' }
        return { ...machine, s: 'compactList' }
      }
      case 'removeOwnRead': {
        const record = outcome.type === 'read' ? outcome.record : null
        if (record?.token === machine.token) return { ...machine, s: 'removeOwn' }
        return gaveUp(machine.kind)
      }
      case 'removeOwn':
        return gaveUp(machine.kind)
      case 'compactList': {
        const generations = outcome.type === 'list' ? outcome.generations : []
        const remaining = generations
          .filter((generation) => generation < machine.g)
          .sort((left, right) => left - right)
        return done({ ...machine, s: 'compact', remaining })
      }
      case 'compact':
        return done({ ...machine, remaining: machine.remaining.slice(1) })
      case 'checkOwn': {
        const record = outcome.type === 'read' ? outcome.record : null
        if (record?.token !== machine.token) return { s: 'released' }
        return { s: 'link', g: machine.g + 1, pred: machine.token, kind: 'free' }
      }
      case 'acquired':
      case 'released':
        throw new Error('finished')
    }
  },
  acquired: (machine) =>
    machine.s === 'acquired' ? { generation: machine.g, token: machine.token } : null,
  released: (machine) => machine.s === 'released',
}

const generous = { maxGeneration: 24, maxStates: 6_000_000 }

test('lock model explorer finds the wedge (A) and overlap (B) in the 2eea8f66 algorithm', () => {
  // A: a release that backs off removes its own free slot while the claimant above it
  // backs off too, leaving the lock held by a live process that believes it released.
  const wedge = explore(roundTwoLock, {
    transactions: [1, 1],
    crashes: false,
    staleTakeovers: 0,
    ...generous,
  })
  assert.ok(wedge.wedge, 'expected a wedge')
  console.log(`2eea8f66 wedge (A), ${wedge.states} states:\n  ${wedge.wedge.join('\n  ')}`)
  console.log(
    `2eea8f66 livelock (C) with 2 processes: ${
      wedge.livelock ? `\n  ${wedge.livelock.join('\n  ')}` : 'none found'
    }`,
  )

  // B: the double back-off lowers the highest generation, so a stale observer's slot that
  // its crash leaves behind is later taken as current while another holder is inside.
  const overlap = explore(roundTwoLock, {
    transactions: [2, 1, 1],
    crashes: true,
    staleTakeovers: 1,
    stopAtOverlap: true,
    ...generous,
  })
  assert.ok(overlap.overlap, 'expected an overlap')
  console.log(`2eea8f66 overlap (B), ${overlap.states} states:\n  ${overlap.overlap.join('\n  ')}`)
})

// The default bounds keep `npm test` fast. WORKFLOW_LOCK_MODEL=deep adds larger ones, which
// take minutes and need a large heap (2/2/2 explores about 2.6 million states):
//   WORKFLOW_LOCK_MODEL=deep node --max-old-space-size=24000 --test \
//     dist/test/workflow-state-lock-model.test.mjs
const productionBounds: { transactions: number[]; staleTakeovers: number }[] =
  process.env.WORKFLOW_LOCK_MODEL === 'deep'
    ? [
        { transactions: [1, 1, 1], staleTakeovers: 1 },
        { transactions: [2, 1, 1], staleTakeovers: 1 },
        { transactions: [2, 2, 1], staleTakeovers: 1 },
        { transactions: [2, 2, 2], staleTakeovers: 1 },
        { transactions: [1, 1, 1], staleTakeovers: 2 },
        { transactions: [1, 1, 1, 1], staleTakeovers: 0 },
      ]
    : [
        { transactions: [1, 1, 1], staleTakeovers: 1 },
        { transactions: [2, 1, 1], staleTakeovers: 1 },
      ]

for (const { transactions, staleTakeovers } of productionBounds) {
  const bound = `transactions ${transactions.join('/')}, ${staleTakeovers} stale`
  test(`lock model: production lock is exclusive and cannot wedge, ${bound}`, () => {
    const result = explore(productionLock, {
      transactions,
      crashes: true,
      staleTakeovers,
      ...generous,
    })
    console.log(
      `production lock ${bound}: ${result.states} states, ${result.edges} edges, ` +
        `${result.truncated} truncated`,
    )
    assert.ok(result.overlap === null, result.overlap?.join('\n') ?? '')
    assert.ok(result.wedge === null, result.wedge?.join('\n') ?? '')
    assert.ok(result.livelock === null, result.livelock?.join('\n') ?? '')
    assert.equal(result.truncated, 0)
  })
}
