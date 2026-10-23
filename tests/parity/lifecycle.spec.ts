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
  }
}

function gate() {
  return Promise.withResolvers<void>()
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
      const actual = trace.join('\n') + '\n'
      const fixtures = process.env.CORDIS_PARITY_FIXTURES
      const output = process.env.CORDIS_PARITY_OUTPUT
      if (!fixtures || !output) throw new Error('run this fixture through tests/parity/run.py')
      expect(actual).toBe(readFileSync(join(fixtures, `${mode}.txt`), 'utf8'))
      writeFileSync(join(output, `${mode}.txt`), actual)
    }, 5000)
  }
})
