import { SAGA_STATUS } from '../constants/saga-status.constant';
import type { SagaInstance } from '../types/saga-instance.type';
import type { SagaReply } from '../types/saga-reply.type';
import { sagaLateReply } from './saga-late-reply';

const NOW = new Date('2026-11-01T12:00:00.000Z');

const instance = (status: SagaInstance['status']): SagaInstance => ({
  id: 's',
  name: 'payment',
  version: 1,
  steps: ['reserve', 'charge', 'ship'],
  status,
  stepIndex: 2,
  attempt: 1,
  incarnation: 7,
  generation: 1,
  context: {},
  failure: null,
  lastError: null,
  createdAt: NOW,
  updatedAt: NOW,
  finishedAt: null,
  wakeAt: null,
});

const reply = (generation: number, phase: SagaReply['phase'], stepIndex: number): SagaReply => ({
  commandId: '00000000-0000-4000-8000-000000000000',
  sagaId: 's',
  generation,
  phase,
  stepIndex,
  attempt: 1,
  outcome: 'ok',
});

const STATUSES = Object.values(SAGA_STATUS);
const FACTS = [
  { dispatched: false, done: false },
  { dispatched: true, done: false },
  { dispatched: true, done: true },
  { dispatched: false, done: true },
];

/** Все статусы — литералом: раннер групп планирует по тексту число тестов `it.each`. */
const STATUS_ROWS = [
  'running',
  'compensating',
  'completed',
  'compensated',
  'stuck',
  'resolved',
  'blocked',
] as const;

describe('sagaLateReply: таблица статус × поколение × фаза × шаг × что знает журнал', () => {
  it('выборка полна: все статусы; на статус 2 поколения × 2 фазы × 2 шага × 4 состояния журнала', () => {
    expect([...STATUS_ROWS].sort()).toEqual([...STATUSES].sort());
    expect(FACTS).toHaveLength(4);
  });

  // Строка таблицы на статус — все 32 сочетания сразу, сравнением целиком:
  // запись только при «команда ушла, шаг не выполнен», что бы ни было в статусе,
  // поколении, фазе и шаге.
  it.each(STATUS_ROWS)('%s', (status) => {
    const rows = [1, 0].flatMap((generation) =>
      (['execute', 'compensate'] as const).flatMap((phase) =>
        [2, 1].flatMap((stepIndex) => FACTS.map((facts) => ({ generation, phase, stepIndex, facts }))),
      ),
    );
    expect(rows).toHaveLength(32);
    const actual = rows.map(
      ({ generation, phase, stepIndex, facts }) =>
        sagaLateReply(instance(status), reply(generation, phase, stepIndex), facts) !== null,
    );
    expect(actual).toEqual(rows.map(({ facts }) => facts.dispatched && !facts.done));
  });

  it.each([
    [
      'компенсация у застрявшего',
      SAGA_STATUS.stuck,
      1,
      'compensate',
      /компенсация выполнена участником.*stuck/,
    ],
    [
      'успех шага после начала отката',
      SAGA_STATUS.compensating,
      1,
      'execute',
      /после начала отката.*compensating/,
    ],
    [
      // resolved бывает и из running (замёрзший разобран без отката): «после начала отката» неверно.
      'успех шага у разобранного вручную',
      SAGA_STATUS.resolved,
      1,
      'execute',
      /^успех шага после ручного разбора экземпляра \(resolved\)/,
    ],
    [
      'успех поколения до ручного повтора',
      SAGA_STATUS.running,
      0,
      'execute',
      /поколения 0 после ручного повтора \(поколение 1\)/,
    ],
  ] as const)('текст записи: %s', (_name, status, generation, phase, text) => {
    const entry = sagaLateReply(instance(status), reply(generation, phase, 1), {
      dispatched: true,
      done: false,
    });
    expect(entry).toEqual({
      event: 'late-reply',
      phase,
      stepIndex: 1,
      step: 'charge',
      attempt: 1,
      generation,
      detail: expect.stringMatching(text),
    });
  });
});
