// Copied temporarily into the pinned upstream core tests by run.py.
import { readFileSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { describe, expect, it } from 'vitest'
import { Context, FiberState } from '../src/index.ts'

interface Provider {
  version: number
  open: boolean
}

declare module '../src/context.ts' {
  interface Context {
    parityProvider: Provider
    parityRecovery: number
  }
}

function gate() {
  return Promise.withResolvers<void>()
}

function assertTrace(name: string, trace: string[]) {
  const actual = trace.join('\n') + '\n'
  const fixtures = process.env.CORDIS_PARITY_FIXTURES
  const output = process.env.CORDIS_PARITY_OUTPUT
  if (!fixtures || !output) throw new Error('run this fixture through tests/parity/run.py')
  expect(actual).toBe(readFileSync(join(fixtures, `${name}.txt`), 'utf8'))
  writeFileSync(join(output, `${name}.txt`), actual)
}

function assertDiagnostic(root: Context, failure: Error) {
  const records = root.logger.buffer
  expect(records).toHaveLength(1)
  expect(records[0].type).toBe('error')
  expect(records[0].args).toEqual([failure])
  expect(records[0].args[0]).toBe(failure)
}

describe('cordis-rust executable lifecycle parity', () => {
  for (const mode of ['combined', 'coalesced', 'removed'] as const) {
    it(mode, async () => {
      const root = new Context()
      const baseline = root.fiber.getEffects()
      expect(root.registry.size).toBe(0)
      const trace: string[] = []
      const entered = gate()
      const release = gate()
      const latestEntered = gate()
      const latestRelease = gate()
      let firstCleanup = mode !== 'combined'
      const providerPlugin = (ctx: Context, version: number) => {
        const value = { version, open: true }
        trace.push(`provider-open ${version}`)
        ctx.provide('parityProvider', value)
        ctx.effect(() => () => {
          expect(value.open).toBe(true)
          value.open = false
          trace.push(`provider-close ${version}`)
        })
      }
      const consumerPlugin = {
        inject: ['parityProvider'],
        async apply(ctx: Context, config: string) {
          const version = ctx.parityProvider.version
          trace.push(`apply ${version} ${config}`)
          ctx.effect(() => async () => {
            if (firstCleanup) {
              firstCleanup = false
              trace.push(`cleanup-gated ${version} ${config}`)
              entered.resolve()
              await release.promise
            }
            const old = ctx.parityProvider
            expect(old.version).toBe(version)
            expect(old.open).toBe(true)
            trace.push(`read ${version} ${config}`)
          })
          if (mode === 'coalesced' && config === 'new') {
            latestEntered.resolve()
            await latestRelease.promise
          }
        },
      }
      const provider = await root.plugin(providerPlugin, 1)
      const oldProvider = root.parityProvider
      const consumer = await root.plugin(consumerPlugin, 'old')
      const tasks: { settled: boolean; promise: Promise<void> }[] = []
      const watch = (role: 'provider' | 'consumer', task: void | PromiseLike<void>) => {
        const status = { settled: false, promise: Promise.resolve(task) }
        status.promise = status.promise.then(() => {
          expect(role === 'provider' ? provider.state : consumer.state)
            .toBe(role === 'provider' ? FiberState.ACTIVE
              : mode === 'removed' ? FiberState.DISPOSED : FiberState.PENDING)
          status.settled = true
        })
        tasks.push(status)
      }
      try {
        watch('provider', provider.update(2))
        watch('consumer', consumer.update(mode === 'combined' ? 'new' : 'middle'))
        if (mode !== 'combined') {
          await entered.promise
          watch('provider', provider.update(3))
          watch('consumer', consumer.update('new'))
          if (mode === 'removed') {
            root.registry.delete(consumerPlugin)
            watch('consumer', consumer.await().then(() => {}))
            expect(root.registry.has(consumerPlugin)).toBe(false)
            trace.push('registry consumer=removed')
          }
          expect(provider.state).toBe(FiberState.UNLOADING)
          expect(consumer.state).toBe(FiberState.UNLOADING)
          expect(oldProvider.open).toBe(true)
          expect(tasks.every(task => !task.settled)).toBe(true)
          trace.push('blocked provider=open waiters=pending')
          release.resolve()
        }
        if (mode === 'coalesced') {
          await latestEntered.promise
        }
        await Promise.all(tasks.map(task => task.promise))
        expect(tasks.every(task => task.settled)).toBe(true)
        let readySettled = false
        const ready = consumer.await().then(() => { readySettled = true })
        if (mode === 'coalesced') {
          expect(consumer.state).toBe(FiberState.LOADING)
          expect(readySettled).toBe(false)
          trace.push('latest-load consumer=Loading ready=pending')
          latestRelease.resolve()
        }
        await ready
        trace.push(`update-waiters provider=Active consumer=${mode === 'removed' ? 'Disposed' : 'Pending'}`)
        expect(provider.state).toBe(FiberState.ACTIVE)
        const consumerState = mode === 'removed' ? 'Disposed' : 'Active'
        expect(consumer.state).toBe(mode === 'removed' ? FiberState.DISPOSED : FiberState.ACTIVE)
        trace.push(`settled provider=Active consumer=${consumerState}`)
      } finally {
        release.resolve()
        latestRelease.resolve()
        await root.fiber.dispose()
      }
      expect(root.registry.size).toBe(0)
      expect(root.get('parityProvider', false)).toBeUndefined()
      expect(root.fiber.getEffects()).toEqual(baseline)
      expect(root.logger.buffer.filter(record => record.type === 'error')).toEqual([])
      trace.push('baseline restored')
      assertTrace(mode, trace)
    }, 5000)
  }

  for (const mode of ['superseded-failure', 'latest-failure'] as const) {
    it(mode, async () => {
      const root = new Context()
      const baseline = root.fiber.getEffects()
      const trace: string[] = []
      const latestFails = mode === 'latest-failure'
      const failure = new Error(latestFails ? 'latest load failed' : 'superseded load')
      const failingConfig = latestFails ? 2 : 1
      const loadEntered = gate()
      const loadRelease = gate()
      const rollbackEntered = gate()
      const rollbackRelease = gate()
      const fiber = root.plugin(async (ctx: Context, config: number) => {
        trace.push(`apply ${config}`)
        ctx.effect(() => async () => {
          if (config === failingConfig) {
            trace.push(`rollback-gated ${config}`)
            rollbackEntered.resolve()
            await rollbackRelease.promise
          }
          trace.push(`dispose ${config}`)
        })
        if (config === 1) {
          loadEntered.resolve()
          await loadRelease.promise
        }
        if (config === failingConfig) {
          trace.push(`fail ${config}`)
          throw failure
        }
      }, latestFails ? 0 : 1)
      let settled = 0
      const watch = (task: void | PromiseLike<unknown>) => Promise.resolve(task).then(
        () => { settled++ },
        (error: unknown) => { settled++; return error },
      )
      try {
        if (latestFails) await fiber
        const first = watch(latestFails ? fiber.update(1) : fiber.await())
        await loadEntered.promise
        const tasks = [first, watch(fiber.update(2))]
        if (latestFails) {
          fiber.update(2)
          trace.push('update-waiter abandoned')
        }
        loadRelease.resolve()
        await rollbackEntered.promise
        expect(fiber.state).toBe(FiberState.UNLOADING)
        expect(settled).toBe(0)
        trace.push('blocked rollback waiters=pending')
        rollbackRelease.resolve()
        for (const result of await Promise.all(tasks)) {
          expect(result).toBe(latestFails ? failure : undefined)
        }
        expect(settled).toBe(2)
        expect(fiber.state).toBe(latestFails ? FiberState.FAILED : FiberState.ACTIVE)
        expect(fiber.config).toBe(2)
        expect(fiber.getEffects()).toHaveLength(latestFails ? 0 : 1)
        trace.push(`waiters=${latestFails ? 'Failed' : 'Active'}`)
        assertDiagnostic(root, failure)
        trace.push('diagnostics=1')
      } finally {
        loadRelease.resolve()
        rollbackRelease.resolve()
        await root.fiber.dispose()
      }
      expect(root.registry.size).toBe(0)
      expect(root.fiber.getEffects()).toEqual(baseline)
      assertDiagnostic(root, failure)
      trace.push('baseline restored')
      assertTrace(mode, trace)
    }, 5000)
  }

  it('explicit-recovery', async () => {
    const root = new Context()
    const baseline = root.fiber.getEffects()
    const trace: string[] = []
    const old = root.provide('parityRecovery', 1)
    trace.push('provider 1')
    const failure = new Error('initial failure')
    const entered = gate()
    const release = gate()
    let attempts = 0
    const fiber = root.plugin({
      inject: ['parityRecovery'],
      async apply(ctx: Context, config: number) {
        attempts++
        const version = ctx.parityRecovery
        trace.push(`apply ${version} ${config}`)
        ctx.effect(() => () => {
          expect(ctx.parityRecovery).toBe(version)
          trace.push(`dispose ${version} ${config}`)
        })
        if (config === 0) {
          trace.push('fail initial')
          throw failure
        }
        entered.resolve()
        await release.promise
      },
    }, 0)
    try {
      await expect(fiber.await()).rejects.toBe(failure)
      expect(fiber.state).toBe(FiberState.FAILED)
      expect(fiber.getEffects()).toEqual([])
      trace.push('failed')
      await old()
      root.provide('parityRecovery', 2)
      trace.push('provider 2')
      await expect(fiber.await()).rejects.toBe(failure)
      expect(fiber.state).toBe(FiberState.FAILED)
      expect(attempts).toBe(1)
      trace.push('refresh consumer=Failed attempts=1')
      let settled = false
      const updating = Promise.resolve(fiber.update(1)).then(() => { settled = true })
      await entered.promise
      expect(fiber.state).toBe(FiberState.LOADING)
      expect(settled).toBe(false)
      trace.push('recovery consumer=Loading waiter=pending')
      release.resolve()
      await updating
      expect(fiber.state).toBe(FiberState.ACTIVE)
      expect(attempts).toBe(2)
      expect(fiber.config).toBe(1)
      trace.push('waiter=Active')
      assertDiagnostic(root, failure)
      trace.push('diagnostics=1')
    } finally {
      release.resolve()
      await root.fiber.dispose()
    }
    expect(root.registry.size).toBe(0)
    expect(root.get('parityRecovery', false)).toBeUndefined()
    expect(root.fiber.getEffects()).toEqual(baseline)
    assertDiagnostic(root, failure)
    trace.push('baseline restored')
    assertTrace('explicit-recovery', trace)
  }, 5000)
})
