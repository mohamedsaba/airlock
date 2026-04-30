import { SerializationError } from './errors';

export function assertSerializable(payload: any, seen = new WeakSet()): void {
  if (payload === null || typeof payload !== 'object') {
    return;
  }

  if (seen.has(payload)) {
    throw new SerializationError('Circular reference detected');
  }

  seen.add(payload);

  if (payload instanceof Date) {
    throw new SerializationError('Date objects are not allowed in payload. Convert to ISO string.');
  }

  if (payload instanceof Buffer) {
    throw new SerializationError('Buffer objects are not allowed in payload.');
  }

  if (payload.constructor.name !== 'Object' && payload.constructor.name !== 'Array') {
    throw new SerializationError(
      `Custom class instance (${payload.constructor.name}) detected. Convert to plain object.`,
    );
  }

  for (const key in payload) {
    if (Object.prototype.hasOwnProperty.call(payload, key)) {
      assertSerializable(payload[key], seen);
    }
  }
}
