import { IStorageAdapter, ILogger } from '../interfaces/adapter.interfaces';

export interface GarbageCollectorConfig {
  retentionDays: number;
  chunkSize: number;
  intervalMs: number;
}

export class GarbageCollectorWorker {
  private isRunning = false;
  private timeout?: NodeJS.Timeout;

  constructor(
    private readonly storage: IStorageAdapter,
    private readonly config: GarbageCollectorConfig,
    private readonly logger: ILogger = console,
  ) {}

  async start(): Promise<void> {
    if (this.isRunning) return;
    this.isRunning = true;
    this.run();
  }

  async stop(): Promise<void> {
    this.isRunning = false;
    if (this.timeout) {
      clearTimeout(this.timeout);
    }
  }

  private async run(): Promise<void> {
    if (!this.isRunning) return;

    try {
      const deletedCount = await this.storage.pruneProcessedMessages(
        this.config.retentionDays,
        this.config.chunkSize,
      );

      if (deletedCount === this.config.chunkSize) {
        // If we hit the chunk limit, there might be more to prune.
        // Sleep 100ms and repeat immediately to avoid holding locks too long but keep making progress.
        this.timeout = setTimeout(() => this.run(), 100);
        return;
      }
    } catch (error) {
      this.logger.error('GarbageCollectorWorker error:', error);
    }

    if (this.isRunning) {
      this.timeout = setTimeout(() => this.run(), this.config.intervalMs);
    }
  }
}
