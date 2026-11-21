import { durationMs } from '../config/config-duration';
import { identifier } from '../config/config-identifier';
import { integer } from '../config/config-integer';
import { sagaName } from '../config/config-name';
import { defineSaga } from '../definition/define-saga';
import { SagaConfigError } from '../errors/saga-config.error';
import { SagaPayloadError } from '../errors/saga-payload.error';
import { parseSagaCommand } from '../remote/parse-saga-command';
import { parseSagaReply } from '../remote/parse-saga-reply';
import { sagaUpgradeSql } from '../schema/saga-upgrade-sql';
import { decodeTimestamp } from '../store/decode-timestamp';
import { parseSagaCommandPayload } from './parse-saga-command-payload';
import { payloadCount } from './payload-count';
import { sagaOutput } from './saga-output';
import { sagaValueText } from './saga-value-text';

const ADDRESS = {
  commandId: '00000000-0000-4000-8000-000000000000',
  sagaId: 's',
  generation: 0,
  phase: 'execute',
  stepIndex: 0,
  attempt: 1,
};

/** Значение, на котором `String()` бросает. */
const bare = (): unknown => Object.create(null);

/**
 * Класс «String(значение) без защиты»: каждое место, где значение извне (настройка,
 * разобранное сообщение, выход шага) попадает в текст отказа, отвечает СВОИМ
 * отказом с именем поля, а не TypeError «Cannot convert object to primitive value».
 */
describe('текст отказа о непреобразуемом значении', () => {
  it('sagaValueText: объект без прототипа, сломанный toString, бросающий Proxy — текст', () => {
    const trap = new Proxy(
      {},
      {
        get() {
          throw new Error('ловушка');
        },
      },
    );
    expect([sagaValueText(bare()), sagaValueText(trap), sagaValueText('x'), sagaValueText(1n)]).toEqual([
      '[object Object]',
      '[значение не преобразуется в текст]',
      'x',
      '1',
    ]);
  });

  it.each([
    ['integer', () => integer('f', bare(), 1), SagaConfigError],
    ['durationMs', () => durationMs('f', bare()), SagaConfigError],
    ['identifier', () => identifier('f', bare()), SagaConfigError],
    ['sagaName', () => sagaName('f', bare()), SagaConfigError],
    [
      'имя шага построителя',
      () => defineSaga('x', { version: 1 }).step(bare() as string, {} as never),
      SagaConfigError,
    ],
    [
      'зона sagaUpgradeSql',
      () =>
        sagaUpgradeSql({ instances: 'i', journal: 'j', outbox: 'o', outboxNaiveTimeZone: bare() as string }),
      SagaConfigError,
    ],
    [
      'адрес сообщения',
      () => parseSagaCommandPayload('команда', { ...ADDRESS, commandId: bare() }),
      SagaPayloadError,
    ],
    [
      'фаза сообщения',
      () => parseSagaCommandPayload('команда', { ...ADDRESS, phase: bare() }),
      SagaPayloadError,
    ],
    ['целое сообщения', () => payloadCount('команда', bare(), 0), SagaPayloadError],
    ['исход ответа', () => parseSagaReply({ ...ADDRESS, outcome: bare() }), SagaPayloadError],
    [
      'поле команды участнику',
      () => parseSagaCommand({ sagaCommand: { ...ADDRESS, saga: bare() }, data: {} }),
      SagaPayloadError,
    ],
    [
      'toJSON выхода бросает объект без прототипа',
      () =>
        sagaOutput('шаг', {
          toJSON() {
            throw Object.create(null);
          },
        }),
      SagaPayloadError,
    ],
  ] as const)('%s — свой отказ, а не TypeError', (_name, run, kind) => {
    expect(run).toThrow(kind);
  });

  it('время из базы непреобразуемым значением — отказ с причиной, а не TypeError', () => {
    expect(() => decodeTimestamp(bare())).toThrow(/время из базы не разобрано: \[object Object\]/);
  });
});
