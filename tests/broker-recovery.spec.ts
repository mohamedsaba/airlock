import { describe, it, expect, vi, beforeEach } from 'vitest';
import { RelayWorker, RelayWorkerConfig } from '../src/core/worker/relay-worker';
import { IStorageAdapter, IBrokerAdapter } from '../src/core/interfaces/adapter.interfaces';
import { AirLockMessage } from '../src/core/interfaces/airlock-types.interface';

describe('Broker Recovery Test', () => {
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

  it('should transition to PENDING with exponential backoff on broker failure and then PROCESSED on recovery', async () => {
    const message: AirLockMessage = {
      id: 'msg-1',
      payload: { foo: 'bar' },
      payloadSize: 100,
      status: 'PENDING',
      retryCount: 0,
      nextRetryAt: new Date(),
      createdAt: new Date(),
    } as any;

    // First attempt: Broker fails
    vi.mocked(storage.claimLeases).mockResolvedValueOnce([message]).mockResolvedValue([]);
    vi.mocked(broker.publish).mockRejectedValueOnce(new Error('Broker Down'));

    (worker as any).isRunning = true;
    await (worker as any).poll();

    expect(storage.scheduleRetry).toHaveBeenCalledWith(
      'msg-1',
      expect.any(String),
      expect.any(Date),
      'Broker Down',
      3,
      expect.any(Number)
    );

    // Check that retryDelayMs is within reasonable range for retryCount 0 (approx 0-1000ms)
    const retryDelayMs = vi.mocked(storage.scheduleRetry).mock.calls[0][5];
    expect(retryDelayMs).toBeGreaterThanOrEqual(0);
    expect(retryDelayMs).toBeLessThanOrEqual(1000);

    // Second attempt: Broker recovered
    const messageRetry = { ...message, retryCount: 1 };
    vi.mocked(storage.claimLeases).mockResolvedValueOnce([messageRetry]).mockResolvedValue([]);
    vi.mocked(broker.publish).mockResolvedValueOnce();

    await (worker as any).poll();

    expect(storage.markProcessed).toHaveBeenCalledWith('msg-1', expect.any(String));
  });
});
