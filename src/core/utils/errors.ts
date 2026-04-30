export class AirLockError extends Error {
  constructor(message: string) {
    super(message);
    this.name = this.constructor.name;
  }
}

export class SerializationError extends AirLockError {
  constructor(message: string) {
    super(message);
  }
}

export class PoisonPillError extends AirLockError {
  constructor(message: string) {
    super(message);
  }
}

export class PayloadTooLargeError extends AirLockError {
  constructor(size: number, limit: number) {
    super(`Payload size ${size} exceeds limit of ${limit} bytes`);
  }
}

