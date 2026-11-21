import { SagaStepRejectedError } from './saga-step-rejected.error';

describe('SagaStepRejectedError.is', () => {
  it('узнаёт отказ из другой копии пакета (метка Symbol.for), а не только instanceof', () => {
    // Класс «другой копии»: тот же контракт метки, другой конструктор.
    const foreign = Object.assign(new Error('карта отклонена'), {
      [Symbol.for('@hoshinotatsuya/pg-saga:step-rejected')]: true,
    });
    expect(foreign instanceof SagaStepRejectedError).toBe(false);
    expect([
      SagaStepRejectedError.is(foreign),
      SagaStepRejectedError.is(new SagaStepRejectedError('нет на складе')),
      SagaStepRejectedError.is(new Error('обычный')),
      SagaStepRejectedError.is(null),
      SagaStepRejectedError.is('строка'),
    ]).toEqual([true, true, false, false, false]);
  });

  it('обёрнутый транспортом окончательный отказ (цепочка cause) — тоже окончательный', () => {
    const wrapped = new Error('транспорт: участник ответил отказом', {
      cause: new Error('клиент', { cause: new SagaStepRejectedError('карта отклонена') }),
    });
    expect([
      SagaStepRejectedError.is(wrapped),
      SagaStepRejectedError.is(new Error('x', { cause: 'y' })),
    ]).toEqual([true, false]);
  });
});
