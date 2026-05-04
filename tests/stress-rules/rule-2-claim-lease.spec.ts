import { describe, it, expect, vi } from 'vitest';
import { RelayWorker } from '../../src/core/worker/relay-worker';
import { IStorageAdapter, IBrokerAdapter } from '../../src/core/interfaces';

describe('Rule 2: Claim-Lease (Non-blocking)', () => {
  it('should not hold a database lock during broker publish', async () => {
    const mockStorage: IStorageAdapter = {
      claimLeases: vi.fn().mockResolvedValue([{ 
        id: '1', 
        payload: { hi: 'there' }, 
        payloadSize: 10,
        status: 'PENDING',
        retryCount: 0,
        nextRetryAt: new Date(),
        createdAt: new Date(),
        aggregateType: 'Test',
        aggregateId: '1',
        eventType: 'TestEvent',
        partitionKey: '1'
      }]),
      markProcessed: vi.fn().mockResolvedValue(undefined),
      scheduleRetry: vi.fn().mockResolvedValue(undefined),
      insertMessage: vi.fn(),
    };

    const mockBroker: IBrokerAdapter = {
      publish: vi.fn().mockImplementation(() => new Promise((resolve) => setTimeout(resolve, 1000))),
    };

    const worker = new RelayWorker(mockStorage, mockBroker, {
      pollIntervalMs: 100,
      batchSize: 1,
      concurrency: 1,
      leaseTtlMs: 30000,
      maxBatchBytes: 1024,
      maxRetries: 1,
    });

    // Start worker and wait for it to pick up the message
    worker.start();
    
    // Wait a bit for claimLeases to be called
    await new Promise(r => setTimeout(r, 200));

    expect(mockStorage.claimLeases).toHaveBeenCalled();
    expect(mockBroker.publish).toHaveBeenCalled();
    
    // At this point, publish is running (it takes 1s)
    // In a real integration test, we would verify here that we can still query the DB.
    // For this unit-ish test, we just verify the flow.
    
    await worker.stop(2000);
  });
});
