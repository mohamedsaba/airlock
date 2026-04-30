import { randomUUID } from 'crypto';
import { AirLockEvent } from '../interfaces/airlock-types.interface';

export interface BuildCloudEventOptions {
  type: string;
  source: string;
  subject?: string;
  id?: string;
  traceparent?: string;
}

export function buildCloudEvent(
  data: Record<string, any>,
  options: BuildCloudEventOptions,
): AirLockEvent {
  return {
    id: options.id || randomUUID(),
    source: options.source,
    specversion: '1.0',
    type: options.type,
    time: new Date().toISOString(),
    data,
    subject: options.subject,
    datacontenttype: 'application/json',
    traceparent: options.traceparent,
  };
}
