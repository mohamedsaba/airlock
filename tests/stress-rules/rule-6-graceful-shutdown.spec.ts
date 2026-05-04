import { describe, it, expect, vi } from 'vitest';
import { RelayWorker } from '../../src/core/worker/relay-worker';
import { MockStorageAdapter } from '../mocks/storage.mock';
import { IBrokerAdapter, ILogger } from '../../src/core/interfaces/adapter.interfaces';

describe('Rule 6: Graceful Shutdown', () => {
  it('should stop polling and wait for in-flight messages on stop', async () => {
    const storage = new MockStorageAdapter();
    await storage.insertMessage({ id: '1', payload: { data: 'msg1' }, payloadSize: 10 }, null);
    await storage.insertMessage({ id: '2', payload: { data: 'msg2' }, payloadSize: 10 }, null);

    let publishCount = 0;
    const broker: IBrokerAdapter = {
      publish: vi.fn().mockImplementation(async () => {
        publishCount++;
        // Simulate slow publish
        await new Promise(resolve => setTimeout(resolve, 500));
      }),
    };

    const logger: ILogger = {
      log: vi.fn(),
      error: vi.fn(),
      warn: vi.fn(),
    };

    const worker = new RelayWorker(storage, broker, {
      pollIntervalMs: 100,
      batchSize: 10,
      concurrency: 2,
      leaseTtlMs: 30000,
      maxBatchBytes: 1024,
      maxRetries: 3,
    }, logger);

    // Start worker
    worker.start();

    // Wait for the first message to start processing
    await new Promise(resolve => setTimeout(resolve, 150));
    
    expect(publishCount).toBeGreaterThan(0);

    // Trigger shutdown
    const stopPromise = worker.stop(2000);

    // At this point, new messages should not be picked up if we add them now
    await storage.insertMessage({ id: '3', payload: { data: 'msg3' }, payloadSize: 10 }, null);

    await stopPromise;

    // Verify msg3 was NOT processed
    const msg3 = storage.messages.find(m => m.id === '3');
    expect(msg3?.status).toBe('PENDING');

    // Verify msg1 and msg2 WERE processed (they were in flight)
    expect(broker.publish).toHaveBeenCalledTimes(2);
  });
});
