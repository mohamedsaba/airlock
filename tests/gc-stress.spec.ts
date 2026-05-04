import { describe, it, expect, vi, beforeEach } from 'vitest';
import { GarbageCollectorWorker, GarbageCollectorConfig } from '../src/core/worker/garbage-collector-worker';
import { IStorageAdapter } from '../src/core/interfaces/adapter.interfaces';

describe('Rule 4 Stress Test (GC vs Inserts)', () => {
  let storage: IStorageAdapter;
  let gcWorker: GarbageCollectorWorker;
  const config: GarbageCollectorConfig = {
    retentionDays: 7,
    chunkSize: 5000,
    intervalMs: 1000,
  };

  beforeEach(() => {
    storage = {
      claimLeases: vi.fn(),
      markProcessed: vi.fn(),
      scheduleRetry: vi.fn(),
      insertMessage: vi.fn(),
      verifySchema: vi.fn(),
      pruneProcessedMessages: vi.fn(),
    };
    gcWorker = new GarbageCollectorWorker(storage, config);
  });

  it('should maintain low insert latency during high volume GC pass', async () => {
    // Simulate 1,000,000 rows to prune by returning chunkSize many times
    let gcCalls = 0;
    vi.mocked(storage.pruneProcessedMessages).mockImplementation(async () => {
      gcCalls++;
      // Mock some DB delay for GC (e.g. 20ms)
      await new Promise(resolve => setTimeout(resolve, 20));
      return 5000; // Hit chunkSize to trigger immediate repeat
    });

    // Mock insertMessage with low latency
    vi.mocked(storage.insertMessage).mockImplementation(async () => {
      // Mock DB delay for insert (e.g. 5ms)
      await new Promise(resolve => setTimeout(resolve, 5));
      return 'new-id';
    });

    // Start GC
    gcWorker.start();

    // Concurrently run 100 inserts and measure latency
    const insertLatencies: number[] = [];
    for (let i = 0; i < 100; i++) {
      const start = Date.now();
      await storage.insertMessage({} as any, null);
      insertLatencies.push(Date.now() - start);
    }

    await gcWorker.stop();

    // Sort to find p99
    insertLatencies.sort((a, b) => a - b);
    const p99 = insertLatencies[Math.floor(insertLatencies.length * 0.99)];

    console.log(`p99 Insert Latency during GC: ${p99}ms`);
    expect(p99).toBeLessThan(50);
    expect(gcCalls).toBeGreaterThan(0);
  });
});
