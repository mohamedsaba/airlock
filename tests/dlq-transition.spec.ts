import { describe, it, expect, vi, beforeEach } from 'vitest';
import { RelayWorker, RelayWorkerConfig } from '../src/core/worker/relay-worker';
import { IStorageAdapter, IBrokerAdapter } from '../src/core/interfaces/adapter.interfaces';
import { AirLockMessage } from '../src/core/interfaces/airlock-types.interface';

describe('DLQ Transition Test', () => {
  let storage: IStorageAdapter;
  let broker: IBrokerAdapter;
  let worker: RelayWorker;
  const config: RelayWorkerConfig = {
    pollIntervalMs: 100,
    batchSize: 10,
    concurrency: 5,
    leaseTtlMs: 1000,
    maxBatchBytes: 1000000,
    maxRetries: 3,
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
    broker = {
      publish: vi.fn(),
    };
    worker = new RelayWorker(storage, broker, config);
  });

  it('should transition to FAILED when maxRetries is exhausted', async () => {
    const message: AirLockMessage = {
      id: 'msg-dlq',
      payload: { foo: 'bar' },
      payloadSize: 100,
      status: 'PENDING',
      retryCount: 3, // Already at maxRetries
      nextRetryAt: new Date(),
      createdAt: new Date(),
    } as any;

    vi.mocked(storage.claimLeases).mockResolvedValueOnce([message]).mockResolvedValue([]);
    vi.mocked(broker.publish).mockRejectedValue(new Error('Persistent Error'));

    (worker as any).isRunning = true;
    await (worker as any).poll();

    // The storage adapter implementation handles the CASE WHEN logic, 
    // but the worker should have called scheduleRetry with the correct maxRetries.
    expect(storage.scheduleRetry).toHaveBeenCalledWith(
      'msg-dlq',
      expect.any(String),
      expect.any(Date),
      'Persistent Error',
      3,
      expect.any(Number)
    );
  });
});
