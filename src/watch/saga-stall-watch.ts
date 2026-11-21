import type { OutboxMessage } from '@hoshinotatsuya/outbox';

import { durationMs } from '../config/config-duration';
import { integer } from '../config/config-integer';
import { SAGA_JOURNAL_EVENT } from '../constants/saga-journal-event.constant';
import { SAGA_LIMIT } from '../constants/saga-limit.constant';
import { SAGA_PHASE } from '../constants/saga-phase.constant';
import { SAGA_STATUS } from '../constants/saga-status.constant';
import { SagaConfigError } from '../errors/saga-config.error';
import { sagaErrorText } from '../runtime/saga-error-text';
import { sagaMessageId } from '../runtime/saga-message-id';
import { sagaTimeAfter } from '../runtime/saga-time-after';
import { SagaStore } from '../store/saga-store';
import type { SagaInstance } from '../types/saga-instance.type';
import type { SagaMessageKey } from '../types/saga-message-key.type';
import type { SagaStallWatchDeps } from '../types/saga-stall-watch-deps.type';
import type { SagaStallWatchReport } from '../types/saga-stall-watch-report.type';

/**
 * Сторож активных экземпляров — запускается по расписанию (`runOnce`):
 *
 *  - `blocked`: экземпляр стоит, а сообщение, которого он ждёт ИМЕННО по своему
 *    адресу (команда шага его поколения, фазы, шага и попытки — или таймер
 *    ответа удалённого шага), ушло в DEAD outbox. Сам экземпляр об этом не
 *    узнает никогда: переводится в `blocked`, в журнал — `delivery-dead` с id
 *    и причиной DEAD, хук `blocked`. Сообщение ищется по его id (он выведен из
 *    адреса и воплощения), а не по экземпляру: DEAD прошлой попытки или
 *    поколения живой экземпляр не останавливает;
 *  - `stalled`: активный экземпляр не двигается дольше `stalledAfterMs`, а
 *    DEAD его сообщения нет — хук `stalled`, один раз на эпизод в процессе.
 *    Повтора сторож не делает: живой долгий шаг иначе запустился бы второй раз.
 *
 * Проход берёт `batchSize` неподвижных активных по кругу (курсор по моменту
 * последнего движения и id — в памяти процесса): за несколько проходов
 * проверены все. Перевод в `blocked` — своей транзакцией под блокировкой
 * строки и только если экземпляр всё ещё ждёт тот же адрес. Сторожей может
 * быть несколько: второй увидит уже `blocked` и ничего не сделает.
 */
export class SagaStallWatch {
  private readonly _store: SagaStore;

  private readonly _clock: () => Date;

  private readonly _stalledAfterMs: number;

  private readonly _blockAfterMs: number;

  private readonly _batchSize: number;

  /** Курсор прохода по кругу: последний проверенный (движение, id); `null` — с начала. */
  private _cursor: { readonly at: Date; readonly id: string } | null = null;

  /** Курсор круга `stalled` — отдельный от круга `blocked` (у них разные пороги неподвижности). */
  private _stalledCursor: { readonly at: Date; readonly id: string } | null = null;
  /**
   * Эпизоды `stalled`, о которых уже сказано: ключ — экземпляр, адрес и момент
   * движения. `_reported` — итог прошлого полного круга, `_round` — набирается
   * в текущем; на конце круга круг становится итогом: сдвинувшиеся забыты,
   * память не больше числа неподвижных.
   */
  private _reported = new Set<string>();
  private _round = new Set<string>();

  public constructor(private readonly _deps: SagaStallWatchDeps) {
    this._store = new SagaStore(_deps.tables);
    this._clock = _deps.clock ?? ((): Date => new Date());
    this._stalledAfterMs = durationMs('stalledAfterMs', _deps.stalledAfterMs);
    this._blockAfterMs =
      _deps.blockAfterMs === undefined || _deps.blockAfterMs === 0
        ? 0
        : durationMs('blockAfterMs', _deps.blockAfterMs);
    this._batchSize = integer('batchSize', _deps.batchSize ?? 100, 1);
    if (this._batchSize > SAGA_LIMIT.listLimit) {
      throw new SagaConfigError('batchSize', `не больше ${SAGA_LIMIT.listLimit}`);
    }
    if (typeof (_deps.outbox as { deadLetters?: unknown } | undefined)?.deadLetters !== 'function') {
      throw new SagaConfigError('outbox', 'сторожу нужен OutboxStore той же базы (deadLetters)');
    }
  }

  /** Один проход: `blocked` по DEAD ожидаемого сообщения, затем эпизоды `stalled`. */
  public async runOnce(now: Date = this._clock()): Promise<SagaStallWatchReport> {
    const candidates = await this._store.idleActive(
      this._deps.db,
      new Date(now.getTime() - this._blockAfterMs),
      this._cursor,
      this._batchSize,
    );
    const last = candidates.at(-1);
    // Конец круга — следующий проход с начала.
    this._cursor =
      last === undefined || candidates.length < this._batchSize
        ? null
        : { at: SagaStallWatch._lastMove(last), id: last.id };
    const awaited = new Map(candidates.map((instance) => [instance.id, SagaStallWatch._awaited(instance)]));
    const ids = [...awaited.values()].flatMap((pair) => [pair.command, pair.timer]);
    const dead =
      ids.length === 0
        ? new Map<string, OutboxMessage>()
        : new Map(
            (await this._deps.outbox.deadLetters({ ids }, ids.length)).map((message) => [
              message.id,
              message,
            ]),
          );
    const blocked: string[] = [];
    for (const instance of candidates) {
      const pair = awaited.get(instance.id);
      const message =
        pair === undefined ? undefined : (dead.get(pair.command) ?? dead.get(pair.timer) ?? undefined);
      if (message !== undefined && (await this._block(instance, message))) {
        blocked.push(instance.id);
      }
    }
    return { checked: candidates.length, blocked, stalled: await this._stalled(now, new Set(blocked)) };
  }

  /** Перевести в `blocked`, если экземпляр под блокировкой всё ещё ждёт тот же адрес. */
  private async _block(seen: SagaInstance, message: OutboxMessage): Promise<boolean> {
    const phase = seen.status === SAGA_STATUS.running ? SAGA_PHASE.execute : SAGA_PHASE.compensate;
    const step = seen.steps[seen.stepIndex] ?? `#${seen.stepIndex}`;
    const error = message.lastError === null ? null : sagaErrorText(message.lastError);
    const done = await this._deps.transaction(async (tx) => {
      const instance = await this._store.lock(tx, seen.id);
      if (
        instance === null ||
        instance.status !== seen.status ||
        instance.generation !== seen.generation ||
        instance.stepIndex !== seen.stepIndex ||
        instance.attempt !== seen.attempt ||
        instance.incarnation !== seen.incarnation
      ) {
        return false;
      }
      const at = sagaTimeAfter(this._clock(), instance);
      await this._store.update(
        tx,
        instance.id,
        {
          status: SAGA_STATUS.blocked,
          stepIndex: instance.stepIndex,
          attempt: instance.attempt,
          generation: instance.generation,
          context: instance.context,
          failure: instance.failure,
          lastError: error ?? instance.lastError,
          finishedAt: null,
        },
        at,
      );
      await this._store.append(
        tx,
        instance.id,
        [
          {
            event: SAGA_JOURNAL_EVENT.deliveryDead,
            phase,
            stepIndex: instance.stepIndex,
            step: instance.steps[instance.stepIndex] ?? null,
            attempt: instance.attempt,
            generation: instance.generation,
            detail: sagaErrorText(`${message.eventType} ${message.id}: ${error ?? 'причины нет'}`),
          },
        ],
        at,
      );
      return true;
    });
    if (done) {
      this._call('blocked', () =>
        this._deps.metrics?.blocked?.({
          saga: seen.name,
          sagaId: seen.id,
          step,
          phase,
          attempt: seen.attempt,
          messageId: message.id,
          eventType: message.eventType,
          error,
        }),
      );
    }
    return done;
  }

  /**
   * Новые эпизоды `stalled`: о прежних (тот же адрес и момент движения) не
   * повторяется. Срез — `batchSize` неподвижных по кругу (курсор), так что при
   * неподвижных больше среза за несколько проходов сказано о каждом. Эпизод
   * считается сказанным, только если хук не отказал: отказавший повторится на
   * следующем проходе.
   */
  private async _stalled(now: Date, skip: ReadonlySet<string>): Promise<string[]> {
    const stalled = await this._store.idleActive(
      this._deps.db,
      new Date(now.getTime() - this._stalledAfterMs),
      this._stalledCursor,
      this._batchSize,
    );
    const last = stalled.at(-1);
    const roundEnds = last === undefined || stalled.length < this._batchSize;
    this._stalledCursor = roundEnds ? null : { at: SagaStallWatch._lastMove(last), id: last.id };
    const fresh: string[] = [];
    for (const instance of stalled) {
      if (skip.has(instance.id)) {
        continue;
      }
      const lastMove = SagaStallWatch._lastMove(instance);
      const key = [
        instance.id,
        instance.generation,
        instance.status,
        instance.stepIndex,
        instance.attempt,
        lastMove.getTime(),
      ].join('\u001f');
      if (this._reported.has(key) || this._round.has(key)) {
        this._round.add(key);
        continue;
      }
      fresh.push(instance.id);
      const told = this._call('stalled', () =>
        this._deps.metrics?.stalled?.({
          saga: instance.name,
          sagaId: instance.id,
          status: instance.status,
          step: instance.steps[instance.stepIndex] ?? `#${instance.stepIndex}`,
          attempt: instance.attempt,
          stalledForMs: Math.max(0, now.getTime() - lastMove.getTime()),
        }),
      );
      if (told) {
        this._round.add(key);
      }
    }
    if (roundEnds) {
      this._reported = this._round;
      this._round = new Set();
    }
    return fresh;
  }

  /** Вызвать хук наблюдения; `false` — хук отказал (отказ в журнал, сторож не ломается). */
  private _call(kind: string, hook: () => void): boolean {
    try {
      hook();
      return true;
    } catch (error) {
      try {
        this._deps.logger?.warn(`[pg-saga] хук сторожа ${kind} отказал: ${sagaErrorText(error)}`);
      } catch {
        // Наблюдение не вправе ломать сторожа.
      }
      return false;
    }
  }

  /** id сообщений, которых ждёт экземпляр по своему адресу: команда шага и таймер ответа. */
  private static _awaited(instance: SagaInstance): { command: string; timer: string } {
    const key: SagaMessageKey = {
      sagaId: instance.id,
      incarnation: instance.incarnation,
      generation: instance.generation,
      phase: instance.status === SAGA_STATUS.running ? SAGA_PHASE.execute : SAGA_PHASE.compensate,
      stepIndex: instance.stepIndex,
      attempt: instance.attempt,
    };
    return { command: sagaMessageId.command(key), timer: sagaMessageId.timer(key) };
  }

  private static _lastMove(instance: SagaInstance): Date {
    return new Date(Math.max(instance.updatedAt.getTime(), instance.wakeAt?.getTime() ?? 0));
  }
}
