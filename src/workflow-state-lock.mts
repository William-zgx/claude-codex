// The workflow state lock as a pure state machine. `workflow-state.mts` drives it against
// the filesystem; the lock model test drives the same functions against an in-memory
// directory and explores every interleaving of a few processes, including crashes.
//
// The lock is a directory of numbered generations. The highest number present is the
// current generation; it is either held (a holder record) or free. Each step below
// performs one atomic filesystem operation:
//   - list:   read the generation numbers in the directory (one directory listing);
//   - read:   read one generation's record (records are immutable once created);
//   - create: exclusively create a generation with a complete record (hard link);
//   - remove: delete one generation.
//
// Acquire: list, read the highest generation H. If it is held by a live, fresh holder,
// wait and start over. Otherwise create H + 1 as held, then list again. The claim is
// valid only if H + 1 is still the highest generation; if not, the process leaves its
// slot in place and starts over. A valid holder removes the generations below its own.
//
// Release: create the holder's generation + 1 as free (if it is already taken, a newer
// generation took over and nothing is left to do), then compact below it the same way.
//
// Why this is safe. Only compaction removes anything, and only a process whose own
// generation was the highest when it listed removes, and then only generations below its
// own. So the highest generation is never removed and the highest number never
// decreases. A slot created from an out-of-date observation (its number was compacted
// away and is free again) is therefore below the highest generation when its creator
// lists again, so it is never taken as valid and never becomes the highest. While a
// valid holder G is in its transaction, the first generation above G can only be created
// by someone who read G as available: G's own release, or a takeover of a dead or
// stale holder. So two valid holders can only overlap through a stale-holder takeover.
//
// Why it cannot wedge. The highest generation is free (claimable), held by a dead or
// stale holder (claimable), or held by a live process whose claim is valid and which
// will release it. A process never gives up on a slot that is the highest, and a
// release that loses its slot means a newer generation is already above it.

export type WorkflowStateLockEntryKind = 'held' | 'free'

export interface WorkflowStateLockHeld {
  readonly generation: number
  readonly token: string
}

export type WorkflowStateLockMachine =
  | { readonly step: 'list' }
  | { readonly step: 'read'; readonly generation: number }
  | { readonly step: 'wait' }
  | {
      readonly step: 'create'
      readonly generation: number
      readonly kind: WorkflowStateLockEntryKind
    }
  | {
      readonly step: 'confirm'
      readonly generation: number
      readonly kind: WorkflowStateLockEntryKind
      readonly token: string
    }
  | {
      readonly step: 'compact'
      readonly generation: number
      readonly kind: WorkflowStateLockEntryKind
      readonly token: string
      readonly remaining: readonly number[]
    }
  | { readonly step: 'check-own'; readonly generation: number; readonly token: string }
  | { readonly step: 'acquired'; readonly generation: number; readonly token: string }
  | { readonly step: 'released' }

export type WorkflowStateLockOperation =
  | { readonly type: 'list' }
  | { readonly type: 'read'; readonly generation: number }
  | { readonly type: 'wait' }
  | {
      readonly type: 'create'
      readonly generation: number
      readonly kind: WorkflowStateLockEntryKind
    }
  | { readonly type: 'remove'; readonly generation: number }

export type WorkflowStateLockOutcome<R> =
  | { readonly type: 'list'; readonly generations: readonly number[] }
  // `record` is null when the generation does not exist.
  | { readonly type: 'read'; readonly record: R | null }
  | { readonly type: 'wait' }
  // `token` is the created record's token, or null when the generation already existed.
  | { readonly type: 'create'; readonly token: string | null }
  | { readonly type: 'remove' }

export interface WorkflowStateLockContext<R> {
  // Free, or held by a holder that is dead or older than the stale limit.
  isAvailable(record: R): boolean
  tokenOf(record: R): string | null
}

export function startWorkflowStateLockAcquire(): WorkflowStateLockMachine {
  return { step: 'list' }
}

export function startWorkflowStateLockRelease(
  held: WorkflowStateLockHeld,
): WorkflowStateLockMachine {
  return { step: 'check-own', generation: held.generation, token: held.token }
}

// The next filesystem operation, or null once the machine has acquired or released.
export function workflowStateLockOperation(
  machine: WorkflowStateLockMachine,
): WorkflowStateLockOperation | null {
  switch (machine.step) {
    case 'list':
    case 'confirm':
      return { type: 'list' }
    case 'read':
    case 'check-own':
      return { type: 'read', generation: machine.generation }
    case 'wait':
      return { type: 'wait' }
    case 'create':
      return { type: 'create', generation: machine.generation, kind: machine.kind }
    case 'compact':
      return { type: 'remove', generation: machine.remaining[0] ?? machine.generation }
    case 'acquired':
    case 'released':
      return null
  }
}

export function advanceWorkflowStateLock<R>(
  machine: WorkflowStateLockMachine,
  outcome: WorkflowStateLockOutcome<R>,
  context: WorkflowStateLockContext<R>,
): WorkflowStateLockMachine {
  switch (machine.step) {
    case 'list': {
      const generations = expectOutcome(outcome, 'list').generations
      if (generations.length === 0) return { step: 'wait' }
      return { step: 'read', generation: Math.max(...generations) }
    }
    case 'read': {
      const record = expectOutcome(outcome, 'read').record
      // A missing generation was compacted by a newer one; look again.
      if (record === null) return { step: 'list' }
      if (!context.isAvailable(record)) return { step: 'wait' }
      return { step: 'create', generation: machine.generation + 1, kind: 'held' }
    }
    case 'wait':
      expectOutcome(outcome, 'wait')
      return { step: 'list' }
    case 'create': {
      const token = expectOutcome(outcome, 'create').token
      if (token === null) return machine.kind === 'held' ? { step: 'list' } : { step: 'released' }
      return { step: 'confirm', generation: machine.generation, kind: machine.kind, token }
    }
    case 'confirm': {
      const generations = expectOutcome(outcome, 'list').generations
      // Not the highest: either the slot number was reused below the current generation,
      // or (for a release) someone already claimed above it. Leave the slot in place:
      // removing it could lower the highest generation another process relies on.
      if (Math.max(...generations) !== machine.generation) {
        return machine.kind === 'held' ? { step: 'list' } : { step: 'released' }
      }
      const remaining = generations
        .filter((generation) => generation < machine.generation)
        .sort((left, right) => left - right)
      return finishCompaction({ ...machine, step: 'compact', remaining })
    }
    case 'compact':
      expectOutcome(outcome, 'remove')
      return finishCompaction({ ...machine, remaining: machine.remaining.slice(1) })
    case 'check-own': {
      const record = expectOutcome(outcome, 'read').record
      // A takeover of this generation already removed it.
      if (record === null || context.tokenOf(record) !== machine.token) return { step: 'released' }
      return { step: 'create', generation: machine.generation + 1, kind: 'free' }
    }
    case 'acquired':
    case 'released':
      throw new Error(`workflow state lock machine is already ${machine.step}`)
  }
}

function finishCompaction(
  machine: Extract<WorkflowStateLockMachine, { step: 'compact' }>,
): WorkflowStateLockMachine {
  if (machine.remaining.length > 0) return machine
  if (machine.kind === 'free') return { step: 'released' }
  return { step: 'acquired', generation: machine.generation, token: machine.token }
}

function expectOutcome<R, T extends WorkflowStateLockOutcome<R>['type']>(
  outcome: WorkflowStateLockOutcome<R>,
  type: T,
): Extract<WorkflowStateLockOutcome<R>, { type: T }> {
  if (outcome.type !== type) {
    throw new Error(`workflow state lock expected a ${type} outcome, got ${outcome.type}`)
  }
  return outcome as Extract<WorkflowStateLockOutcome<R>, { type: T }>
}
