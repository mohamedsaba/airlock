export interface AirLockMessage {
  id: string;
  aggregateType: string;
  aggregateId: string;
  eventType: string;
  partitionKey: string;
  payload: any;
  payloadSize: number;
  idempotencyKey?: string;
  status: 'PENDING' | 'IN_FLIGHT' | 'PROCESSED' | 'FAILED';
  retryCount: number;
  nextRetryAt: Date;
  lockedBy?: string;
  lockedUntil?: Date;
  errorReason?: string;
  createdAt: Date;
  firstAttemptAt?: Date;
  lastAttemptAt?: Date;
  processedAt?: Date;
}

export interface AirLockEvent {
  id: string;
  source: string;
  specversion: "1.0";
  type: string;
  time: string;
  data: Record<string, any>;
  subject?: string;
  datacontenttype: "application/json";
  traceparent?: string;
}
