import { AirLockMessage, AirLockEvent } from './airlock-types.interface';

export interface IStorageAdapter {
  claimLeases(batchSize: number, leaseTtlMs: number): Promise<AirLockMessage[]>;
  markProcessed(id: string, workerId: string): Promise<void>;
  scheduleRetry(
    id: string,
    workerId: string,
    nextRetryAt: Date, // Kept for legacy support if needed, but retryDelayMs is preferred
    errorReason: string,
    maxRetries: number,
    retryDelayMs?: number,
  ): Promise<void>;
  pruneProcessedMessages(retentionDays: number, chunkSize: number): Promise<number>;
  insertMessage(
    message: Partial<AirLockMessage>,
    transactionManager: any,
  ): Promise<string>;
  verifySchema(): Promise<void>;
}

export interface IBrokerAdapter {
  publish(event: AirLockEvent): Promise<void>;
}

export interface ILogger {
  log(message: string, ...optionalParams: any[]): any;
  error(message: string, ...optionalParams: any[]): any;
  warn(message: string, ...optionalParams: any[]): any;
  debug?(message: string, ...optionalParams: any[]): any;
}
