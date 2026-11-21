import { SagaPayloadError } from '../errors/saga-payload.error';
import type { SagaCommandMeta } from '../types/saga-command-meta.type';
import { parseSagaCommand } from './parse-saga-command';
import { parseSagaReply } from './parse-saga-reply';
import { sagaReplyMessage } from './saga-reply-message';
import { sagaReplyOf } from './saga-reply-of';

const META: SagaCommandMeta = {
  commandId: '6f1d3b52-a8e4-5c0f-9b7e-2d5a91c3e7f4',
  sagaId: 's-1',
  saga: 'order',
  version: 2,
  generation: 0,
  phase: 'execute',
  stepIndex: 1,
  step: 'charge',
  attempt: 1,
  stepKey: 's-1:execute:1',
};

describe('команда участнику и ответ', () => {
  it('разбор команды отдаёт служебную часть и данные; ответ строится из неё и разбирается обратно', () => {
    const { meta, data } = parseSagaCommand({ sagaCommand: META, data: { amount: 5 } });
    const ok = sagaReplyOf(meta, { ok: true, data: { chargeId: 'c' } });
    const failed = sagaReplyOf(meta, { ok: false, error: new Error('нет денег'), retryable: false });

    expect({ meta, data }).toEqual({ meta: META, data: { amount: 5 } });
    expect(parseSagaReply(JSON.parse(JSON.stringify(ok)))).toEqual(ok);
    expect(parseSagaReply(JSON.parse(JSON.stringify(failed)))).toEqual({
      commandId: META.commandId,
      sagaId: 's-1',
      generation: 0,
      phase: 'execute',
      stepIndex: 1,
      attempt: 1,
      outcome: 'failed',
      error: 'Error: нет денег',
      retryable: false,
    });
  });

  it('событие ответа: id детерминирован от команды, тип по умолчанию saga.reply', () => {
    const reply = sagaReplyOf(META, { ok: true });
    expect(sagaReplyMessage(reply)).toEqual(sagaReplyMessage({ ...reply }));
    expect(sagaReplyMessage(reply)).toEqual(
      expect.objectContaining({ eventType: 'saga.reply', aggregateType: 'saga-reply', payload: reply }),
    );
    // Свой агрегат у ответа: при FIFO по агрегату он не ждёт отложенных команд экземпляра.
    expect(sagaReplyMessage(reply).aggregateId).toBe(sagaReplyMessage(reply).id);
    expect(sagaReplyMessage(reply).aggregateId).not.toBe('s-1');

    expect(sagaReplyMessage(reply).id).not.toBe(META.commandId);
  });

  it('commandId в верхнем регистре принимается и приводится к нижнему (ключ inbox — как выдал оркестратор)', () => {
    const upper = META.commandId.toUpperCase();
    expect(parseSagaCommand({ sagaCommand: { ...META, commandId: upper } }).meta.commandId).toBe(
      META.commandId,
    );
  });

  it('ключ формата 2: с воплощением принимается; чужое воплощение и ключ выполнения не того шага — отказ', () => {
    const meta = {
      ...META,
      incarnation: 1_700_000_000_000,
      stepKey: `${META.sagaId}#1700000000000:${META.phase}:${META.stepIndex}`,
    };
    expect(parseSagaCommand({ sagaCommand: meta }).meta.incarnation).toBe(1_700_000_000_000);
    expect(() => parseSagaCommand({ sagaCommand: { ...meta, incarnation: 1 } })).toThrow(SagaPayloadError);
    expect(() =>
      parseSagaCommand({
        sagaCommand: { ...meta, executeStepKey: `${META.sagaId}#1:execute:${META.stepIndex}` },
      }),
    ).toThrow(SagaPayloadError);
    expect(
      parseSagaCommand({
        sagaCommand: { ...meta, executeStepKey: `${META.sagaId}#1700000000000:execute:${META.stepIndex}` },
      }).meta.executeStepKey,
    ).toBe(`${META.sagaId}#1700000000000:execute:${META.stepIndex}`);
  });

  it.each([
    ['не объект', null],
    ['нет sagaCommand', { data: 1 }],
    ['stepKey не сходится с адресом', { sagaCommand: { ...META, stepKey: 's-1:execute:2' } }],
    ['commandId не uuid', { sagaCommand: { ...META, commandId: 'x' } }],
    ['попытка 0', { sagaCommand: { ...META, attempt: 0 } }],
    ['чужая фаза', { sagaCommand: { ...META, phase: 'undo' } }],
  ])('команда: %s — отказ', (_label, payload) => {
    expect(() => parseSagaCommand(payload)).toThrow(SagaPayloadError);
  });

  it.each([
    ['исход не из словаря', { ...sagaReplyOf(META, { ok: true }), outcome: 'maybe' }],
    ['data — массив', { ...sagaReplyOf(META, { ok: true }), data: [1] }],
    ['retryable — строка', { ...sagaReplyOf(META, { ok: true }), retryable: 'no' }],
    ['нет sagaId', { ...sagaReplyOf(META, { ok: true }), sagaId: '' }],
  ])('ответ: %s — отказ', (_label, payload) => {
    expect(() => parseSagaReply(payload)).toThrow(SagaPayloadError);
  });
});
