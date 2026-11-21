import type { SagaCommandPayload } from '../types/saga-command-payload.type';
import { sagaMessageBelongs } from './saga-message-belongs';
import { sagaMessageId } from './saga-message-id';

const INCARNATION = 140_737_488_355_327;
const address = { sagaId: 'order:1', generation: 0, phase: 'execute', stepIndex: 1, attempt: 1 } as const;

const message = (commandId: string): SagaCommandPayload => ({ commandId, ...address });

/**
 * id, которые версия 0.1.0 выдавала для адреса `order:1`/0/execute/1/1 —
 * ЛИТЕРАЛАМИ: посчитаны отдельно (uuid v5 по SHA-1 пространства имён пакета и
 * строки `вид␟sagaId␟поколение␟фаза␟шаг␟попытка` её формулы), а не той же
 * функцией, что проверяется. Иначе правка формулы без воплощения прошла бы
 * спеку, а летящие сообщения 0.1.0 перестали бы приниматься.
 */
const V010 = {
  timer: '5c5c3921-6adc-550a-a304-d5efc1c14cb6',
  command: '4d508536-6093-58c1-b51c-6420bc6ad454',
  remote: '9fd96c7c-7340-5478-a25f-0d3a6d8096db',
} as const;

describe('sagaMessageBelongs: сообщение выдано этим воплощением', () => {
  const current = sagaMessageId.timer({ ...address, incarnation: INCARNATION });
  const previous = sagaMessageId.timer({ ...address, incarnation: INCARNATION - 1 });

  it('id формата 0.1.0 (без воплощения) — ровно те, что выдавала 0.1.0', () => {
    expect({
      timer: sagaMessageId.timer({ ...address, incarnation: null }),
      command: sagaMessageId.command({ ...address, incarnation: null }),
      remote: sagaMessageId.remote({ ...address, incarnation: null }),
    }).toEqual(V010);
  });

  it.each([
    ['id этого воплощения', INCARNATION, current, true],
    ['id прежнего воплощения того же id саги (до prune)', INCARNATION, previous, false],
    ['id формата 0.1.0 у экземпляра 0.2.0', INCARNATION, V010.timer, false],
    ['id формата 0.1.0 у экземпляра, начатого 0.1.0 (летящее сообщение)', null, V010.timer, true],
    ['id с воплощением у экземпляра, начатого 0.1.0', null, current, false],
    ['произвольный uuid у экземпляра, начатого 0.1.0', null, '00000000-0000-4000-8000-000000000000', false],
    [
      'id другого вида сообщения (команда вместо таймера)',
      INCARNATION,
      sagaMessageId.command({ ...address, incarnation: INCARNATION }),
      false,
    ],
    ['id другого вида у экземпляра 0.1.0 (команда вместо таймера)', null, V010.command, false],
  ] as const)('%s → %s', (_name, incarnation, id, expected) => {
    expect(sagaMessageBelongs('timer', { id: 'order:1', incarnation }, message(id))).toBe(expected);
  });

  it('поля адреса участвуют в id: другая попытка с тем же id — не своя', () => {
    expect(
      sagaMessageBelongs(
        'timer',
        { id: 'order:1', incarnation: INCARNATION },
        { ...message(current), attempt: 2 },
      ),
    ).toBe(false);
    expect(
      sagaMessageBelongs(
        'timer',
        { id: 'order:1', incarnation: null },
        { ...message(V010.timer), attempt: 2 },
      ),
    ).toBe(false);
  });
});
