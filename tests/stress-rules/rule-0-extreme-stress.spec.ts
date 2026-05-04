import { describe, it, expect, vi } from 'vitest';
import { RelayWorker } from '../../src/core/worker/relay-worker';
import { MockStorageAdapter } from '../mocks/storage.mock';
import { IBrokerAdapter, ILogger } from '../../src/core/interfaces/adapter.interfaces';

describe('Extreme Stress Test', () => {
  it('should maintain invariants under high load and failure conditions', async () => {
    const totalMessages = 500;
    const storage = new MockStorageAdapter();
    
    // Mixture of valid and poison pill messages
    for (let i = 0; i < totalMessages; i++) {
      const isPoison = i % 50 === 0; // 10 poison pills
      await storage.insertMessage({
        id: `msg-${i}`,
        payload: isPoison ? "INVALID_JSON_CORRUPTED" : { i },
        payloadSize: 10,
        aggregateType: 'StressTest',
        aggregateId: `${i}`,
        eventType: 'TestEvent',
        partitionKey: `p-${i % 10}`
      }, null);
    }

    // Unreliable storage: 15% failure rate, jittery latency
    storage.errorRate = 0.15;
    storage.latencyMs = 5;

    let publishedCount = 0;
    const broker: IBrokerAdapter = {
      publish: vi.fn().mockImplementation(async () => {
        // Random broker failures
        if (Math.random() < 0.1) throw new Error('Broker Flaky');
        // Random latency
        await new Promise(resolve => setTimeout(resolve, Math.random() * 30));
        publishedCount++;
      }),
    };

    const logger: ILogger = {
      log: vi.fn(),
      error: vi.fn(),
      warn: vi.fn(),
    };

    const worker = new RelayWorker(storage, broker, {
      pollIntervalMs: 10,
      batchSize: 50,
      concurrency: 30,
      leaseTtlMs: 1000, // Short lease for rapid recovery testing
      maxBatchBytes: 10 * 1024 * 1024,
      maxRetries: 3,
    }, logger);

    console.log('Starting Chaos Engine...');
    worker.start();

    // Chaos Phase 1: Heavy load + sudden stop
    await new Promise(resolve => setTimeout(resolve, 500));
    console.log(`Chaos Phase 1: Worker CRASH at ${publishedCount}/${totalMessages}`);
    await worker.stop(100); // Very short timeout to force dirty shutdown

    // Chaos Phase 2: Resume + wait for lease expiration
    console.log('Chaos Phase 2: RECOVERING...');
    worker.start();

    // Wait for full completion (Processed + Failed)
    let finished = false;
    for (let i = 0; i < 60; i++) {
      const done = storage.messages.filter(m => m.status === 'PROCESSED' || m.status === 'FAILED').length;
      if (done >= totalMessages) {
        finished = true;
        break;
      }
      await new Promise(resolve => setTimeout(resolve, 500));
    }

    await worker.stop(500);

    const processed = storage.messages.filter(m => m.status === 'PROCESSED').length;
    const failed = storage.messages.filter(m => m.status === 'FAILED').length;
    const pending = storage.messages.filter(m => m.status === 'PENDING').length;
    const inFlight = storage.messages.filter(m => m.status === 'IN_FLIGHT').length;

    console.log(`
--- Chaos Engine Final Report ---
Processed: ${processed}
Failed: ${failed}
Pending: ${pending}
In-Flight: ${inFlight}
Total: ${totalMessages}
    `);

    // Verification of At-Least-Once
    expect(finished).toBe(true);
    expect(processed + failed).toBe(totalMessages);
    expect(pending).toBe(0);
    expect(inFlight).toBe(0);

    // Rule 1: Poison Pills Verification
    const poisonPills = storage.messages.filter(m => typeof m.payload === 'string');
    expect(poisonPills.length).toBe(10);
    for (const pill of poisonPills) {
      expect(pill.status).toBe('FAILED');
      expect(pill.retryCount).toBe(1); 
    }
  }, { timeout: 45000 });
});
