import { randomUUID } from 'crypto';
import { IStorageAdapter, IBrokerAdapter, ILogger } from '../interfaces/adapter.interfaces';
import { AirLockMessage } from '../interfaces/airlock-types.interface';
import { PoisonPillError } from '../utils/errors';

export interface RelayWorkerConfig {
  pollIntervalMs: number;
  batchSize: number;
  concurrency: number;
  leaseTtlMs: number;
  maxBatchBytes: number;
  maxRetries: number;
}

export class RelayWorker {
  private readonly abortController = new AbortController();
  private readonly workerId: string;
  private isRunning = false;
  private timeout?: NodeJS.Timeout;
  private inFlightCount = 0;

  constructor(
    private readonly storage: IStorageAdapter,
    private readonly broker: IBrokerAdapter,
    private readonly config: RelayWorkerConfig,
    private readonly logger: ILogger = console,
    workerId?: string,
  ) {
    this.workerId = workerId || randomUUID();
  }

  async start(): Promise<void> {
    if (this.isRunning) return;
    this.isRunning = true;
    this.poll();
  }

  async stop(timeoutMs: number): Promise<void> {
    this.isRunning = false;
    this.abortController.abort();

    if (this.timeout) {
      clearTimeout(this.timeout);
    }

    const start = Date.now();
    while (this.inFlightCount > 0 && Date.now() - start < timeoutMs) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }

  private async poll(): Promise<void> {
    if (!this.isRunning) return;

    try {
      const messages = await this.storage.claimLeases(
        this.config.batchSize,
        this.config.leaseTtlMs,
      );

      if (messages.length > 0) {
        await this.processBatch(messages);
      }
    } catch (error) {
      this.logger.error('RelayWorker poll error:', error);
    }

    if (this.isRunning) {
      this.timeout = setTimeout(() => this.poll(), this.config.pollIntervalMs);
    }
  }

  private async processBatch(messages: AirLockMessage[]): Promise<void> {
    let currentBatchBytes = 0;
    const approvedMessages: AirLockMessage[] = [];

    for (const msg of messages) {
      if (currentBatchBytes + msg.payloadSize > this.config.maxBatchBytes) {
        // Leave the rest for next poll or another worker
        break;
      }
      approvedMessages.push(msg);
      currentBatchBytes += msg.payloadSize;
    }

    // Process approved messages in parallel with concurrency limit
    const chunks = this.chunkArray(approvedMessages, this.config.concurrency);
    for (const chunk of chunks) {
      await Promise.all(chunk.map((msg) => this.processMessage(msg)));
    }
  }

  private async processMessage(msg: AirLockMessage): Promise<void> {
    this.inFlightCount++;
    try {
      // Poison Pill Check (Rule 1)
      let event;
      try {
        event = typeof msg.payload === 'string' ? JSON.parse(msg.payload) : msg.payload;
      } catch (e) {
        throw new PoisonPillError(`Failed to parse payload for message ${msg.id}`);
      }

      await this.broker.publish(event);
      await this.storage.markProcessed(msg.id, this.workerId);
    } catch (error) {
      const isPoisonPill = error instanceof PoisonPillError;
      const nextRetryAt = new Date(Date.now() + 10000); // Phase 0: fixed 10s backoff

      await this.storage.scheduleRetry(
        msg.id,
        this.workerId,
        nextRetryAt,
        error instanceof Error ? error.message : String(error),
        isPoisonPill ? 0 : this.config.maxRetries,
      );
    } finally {
      this.inFlightCount--;
    }
  }

  private chunkArray<T>(array: T[], size: number): T[][] {
    const result: T[][] = [];
    for (let i = 0; i < array.length; i += size) {
      result.push(array.slice(i, i + size));
    }
    return result;
  }
}
