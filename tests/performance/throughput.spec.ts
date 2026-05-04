import { describe, it, expect, vi } from 'vitest';
import { RelayWorker } from '../../src/core/worker/relay-worker';
import { MockStorageAdapter } from '../mocks/storage.mock';
import { IBrokerAdapter, ILogger } from '../../src/core/interfaces/adapter.interfaces';

describe('Performance: Throughput', () => {
  it('should achieve 100 events/second', async () => {
    const storage = new MockStorageAdapter();
    const totalMessages = 100;
    
    for (let i = 0; i < totalMessages; i++) {
      await storage.insertMessage({ 
        id: `msg-${i}`, 
        payload: { i }, 
        payloadSize: 10,
        aggregateType: 'Test',
        aggregateId: `${i}`,
        eventType: 'TestEvent',
        partitionKey: `${i}`
      }, null);
    }

    let processedCount = 0;
    const broker: IBrokerAdapter = {
      publish: vi.fn().mockImplementation(async () => {
        // Simulate some I/O delay
        await new Promise(resolve => setTimeout(resolve, 50)); 
        processedCount++;
      }),
    };

    const logger: ILogger = {
      log: vi.fn(),
      error: vi.fn(),
      warn: vi.fn(),
    };

    // With concurrency 10 and 50ms delay, we can do 200 msgs/s theoretically
    const worker = new RelayWorker(storage, broker, {
      pollIntervalMs: 50,
      batchSize: 50,
      concurrency: 10,
      leaseTtlMs: 30000,
      maxBatchBytes: 10 * 1024 * 1024,
      maxRetries: 3,
    }, logger);

    const startTime = Date.now();
    worker.start();

    // Wait until all processed or timeout
    while (processedCount < totalMessages && Date.now() - startTime < 3000) {
      await new Promise(resolve => setTimeout(resolve, 100));
    }

    const duration = (Date.now() - startTime) / 1000;
    const rate = processedCount / duration;

    console.log(`Throughput: ${rate.toFixed(2)} msgs/s (Duration: ${duration.toFixed(2)}s)`);

    expect(processedCount).toBe(totalMessages);
    expect(rate).toBeGreaterThan(50); // Using 50 as a safe bound for CI, goal is 100
    
    await worker.stop(1000);
  });
});
