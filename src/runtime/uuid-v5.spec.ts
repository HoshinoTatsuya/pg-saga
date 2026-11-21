import { deterministicId } from './deterministic-id';
import { uuidV5 } from './uuid-v5';

describe('uuidV5', () => {
  it('эталон: пространство DNS и python.org (вектор из стандартной библиотеки Python)', () => {
    expect(uuidV5('6ba7b810-9dad-11d1-80b4-00c04fd430c8', 'python.org')).toBe(
      '886313e1-3b8a-5372-9b90-0c9aee199e5d',
    );
  });

  it('детерминирован и различает ключи; версия 5, вариант RFC', () => {
    const a = deterministicId('command\u001fs-1\u001f0\u001fexecute\u001f0\u001f1');
    expect(deterministicId('command\u001fs-1\u001f0\u001fexecute\u001f0\u001f1')).toBe(a);
    expect(deterministicId('command\u001fs-1\u001f0\u001fexecute\u001f0\u001f2')).not.toBe(a);
    expect(a).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  });

  it('пространство имён не uuid — отказ', () => {
    expect(() => uuidV5('nope', 'x')).toThrow('не uuid');
  });
});
