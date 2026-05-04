import { describe, it, expect } from 'vitest';
import { assertSerializable } from '../src/core/utils/assert-serializable';
import { SerializationError } from '../src/core/utils/errors';

describe('assertSerializable', () => {
  it('should allow plain objects and arrays', () => {
    expect(() => assertSerializable({ a: 1, b: [2, 3] })).not.toThrow();
  });

  it('should throw on Date objects', () => {
    expect(() => assertSerializable({ date: new Date() })).toThrow(SerializationError);
  });

  it('should throw on Buffer objects', () => {
    expect(() => assertSerializable({ buf: Buffer.from('hi') })).toThrow(SerializationError);
  });

  it('should throw on custom class instances', () => {
    class MyClass {
      a = 1;
    }
    expect(() => assertSerializable({ obj: new MyClass() })).toThrow(SerializationError);
  });

  it('should throw on circular references', () => {
    const obj: any = { a: 1 };
    obj.self = obj;
    expect(() => assertSerializable(obj)).toThrow(SerializationError);
  });
});
