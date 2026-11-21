import { sagaDate } from '../config/config-date';
import { durationMs } from '../config/config-duration';
import { integer } from '../config/config-integer';
import { SAGA_ACTIVE_STATUSES } from '../constants/saga-active-statuses.constant';
import { SAGA_AGGREGATE_TYPE } from '../constants/saga-aggregate-type.constant';
import { SAGA_JOURNAL_EVENT } from '../constants/saga-journal-event.constant';
import { SAGA_LIMIT } from '../constants/saga-limit.constant';
import { SAGA_PHASE } from '../constants/saga-phase.constant';
import { SAGA_STATUS } from '../constants/saga-status.constant';
import { SagaConfigError } from '../errors/saga-config.error';
import { SagaStateError } from '../errors/saga-state.error';
import { sagaErrorText } from '../runtime/saga-error-text';
import { sagaMessageId } from '../runtime/saga-message-id';
import { sagaOutput } from '../runtime/saga-output';
import { sagaTimeAfter } from '../runtime/saga-time-after';
import { SagaStore } from '../store/saga-store';
import type { SagaAdminDeps } from '../types/saga-admin-deps.type';
import type { SagaAdminMetrics } from '../types/saga-admin-metrics.type';
import type { SagaCommandPayload } from '../types/saga-command-payload.type';
import type { SagaInstance } from '../types/saga-instance.type';
import type { SagaJournalEntry } from '../types/saga-journal-entry.type';
import type { SagaListFilter } from '../types/saga-list-filter.type';
import type { SagaStats } from '../types/saga-stats.type';
import type { SagaStuckReportItem } from '../types/saga-stuck-report-item.type';

/**
 * Ручной разбор и срезы: показать экземпляр с журналом, список, отчёт о
 * застрявших, повторить компенсацию застрявшего, пометить разобранным,
 * удалить завершённые по сроку. Определения саг разбору не нужны: ручной
 * повтор пишет обычную команду компенсации, а исполняет её оркестратор.
 */
export class SagaAdmin {
  private readonly _store: SagaStore;

  private readonly _clock: () => Date;

  public constructor(private readonly _deps: SagaAdminDeps) {
    this._store = new SagaStore(_deps.tables);
    this._clock = _deps.clock ?? ((): Date => new Date());
  }

  public async show(id: string): Promise<{ instance: SagaInstance; journal: SagaJournalEntry[] } | null> {
    const instance = await this._store.get(this._deps.db, id);
    return instance === null ? null : { instance, journal: await this._store.journalOf(this._deps.db, id) };
  }

  public async list(filter: SagaListFilter = {}): Promise<SagaInstance[]> {
    return await this._store.list(this._deps.db, filter);
  }

  /** Срез по сагам для метрик: активные, застрявшие, начато/завершено с `since`, возраст старейшего. */
  public async stats(since: Date, now: Date = this._clock()): Promise<SagaStats[]> {
    if (!(since instanceof Date) || Number.isNaN(since.getTime()) || since.getTime() > now.getTime()) {
      throw new SagaConfigError('since', 'начало окна — момент не позже текущего');
    }
    return await this._store.stats(this._deps.db, now, since);
  }

  /**
   * Ждущие человека, старейшие первыми: застрявшие (`stuck`) и вставшие на DEAD
   * ожидаемого сообщения (`blocked`) — что сломалось, на каком шаге и сколько ждёт.
   */
  public async stuckReport(limit = 100, now: Date = this._clock()): Promise<SagaStuckReportItem[]> {
    const stuck = await this._store.list(this._deps.db, {
      statuses: [SAGA_STATUS.stuck, SAGA_STATUS.blocked],
      limit,
    });
    return stuck.map((instance) => SagaAdmin._reportItem(instance, now));
  }

  /**
   * Активные (running, compensating), не двигавшиеся не меньше `olderThanMs`,
   * старейшие первыми. Так видна сага, чья доставка ушла в DEAD на отказе
   * инфраструктуры: в `stuckReport` её нет, а сама она не сдвинется никогда.
   * Порог — больше самого долгого шага с паузами повтора: живой долгий шаг тоже
   * «не двигается».
   */
  public async stalledReport(
    olderThanMs: number,
    limit = 100,
    now: Date = this._clock(),
  ): Promise<SagaStuckReportItem[]> {
    const threshold = durationMs('olderThanMs', olderThanMs);
    const stalled = await this._store.list(this._deps.db, {
      statuses: SAGA_ACTIVE_STATUSES,
      updatedBefore: new Date(now.getTime() - threshold),
      limit,
    });
    return stalled.map((instance) => SagaAdmin._reportItem(instance, now));
  }

  private static _retryDetail(
    note: string | null,
    patch: Readonly<Record<string, unknown>> | null,
  ): string | null {
    const keys = patch === null ? [] : Object.keys(patch);
    // Пустая причина — её отсутствие, а не «отказ без описания» в журнале.
    const parts = [
      ...(note === null || note.trim().length === 0 ? [] : [note]),
      ...(keys.length === 0 ? [] : [`контекст поправлен: ${keys.join(', ')}`]),
    ];
    return parts.length === 0 ? null : sagaErrorText(parts.join('; '));
  }

  /** Хук после фиксации; его отказ не ломает разбор. */
  private _emit<K extends keyof SagaAdminMetrics>(
    kind: K,
    event: Parameters<NonNullable<SagaAdminMetrics[K]>>[0],
  ): void {
    try {
      (this._deps.metrics?.[kind] as ((e: unknown) => void) | undefined)?.call(this._deps.metrics, event);
    } catch {
      // Наблюдение не вправе ломать ручной разбор.
    }
  }

  /**
   * Примет ли `retry`/`resolve` этот экземпляр сейчас (`stalledForMs` — порог
   * замёрзшего): CLI проверяет это и без `--yes`, чтобы не обещать действие,
   * которое откажет.
   */
  public static eligible(instance: SagaInstance, now: Date, stalledForMs?: number): boolean {
    return SagaAdmin._eligible(instance, now, stalledForMs);
  }

  private static _eligible(instance: SagaInstance, now: Date, stalledForMs: number | undefined): boolean {
    // Застрявший и вставший на DEAD сами не двигаются: порог простоя им не нужен.
    if (instance.status === SAGA_STATUS.stuck || instance.status === SAGA_STATUS.blocked) {
      return true;
    }
    if (stalledForMs === undefined || !SAGA_ACTIVE_STATUSES.includes(instance.status)) {
      return false;
    }
    // Ждущий паузы повтора или таймера ответа не замёрз, пока срок не наступил.
    const lastMove = Math.max(instance.updatedAt.getTime(), instance.wakeAt?.getTime() ?? 0);
    return now.getTime() - lastMove >= durationMs('stalledForMs', stalledForMs);
  }

  private static _reportItem(instance: SagaInstance, now: Date): SagaStuckReportItem {
    return {
      id: instance.id,
      saga: instance.name,
      version: instance.version,
      status: instance.status,
      step: instance.steps[instance.stepIndex] ?? `#${instance.stepIndex}`,
      attempt: instance.attempt,
      failure: instance.failure,
      lastError: instance.lastError,
      // Запись сделала реплика с часами впереди: простой не бывает отрицательным.
      stuckForMs: Math.max(
        0,
        now.getTime() - Math.max(instance.updatedAt.getTime(), instance.wakeAt?.getTime() ?? 0),
      ),
    };
  }

  /**
   * Повторить застрявший экземпляр с того же шага: новое поколение (сообщения
   * прошлого поколения больше не применяются), попытка 1, команда — в outbox
   * той же транзакцией. `stuck` — повтор компенсации. `blocked` — шаг в той
   * фазе, в которой экземпляр встал (из записи `delivery-dead` журнала), без
   * порога простоя: экземпляр сам не двигается.
   *
   * `stalledForMs` — повтор и ЗАМЁРЗШЕГО активного экземпляра (running или
   * compensating, не двигался не меньше этого срока; см. `stalledReport`):
   * доставка его команды могла уйти в DEAD (отказ инфраструктуры на каждой
   * попытке), и сам экземпляр об этом не узнал. Шаг повторяется в своей фазе.
   * Без опции, или экземпляр двигался недавно, — отказ: живой шаг иначе
   * запустился бы второй раз параллельно.
   */
  public async retry(
    id: string,
    note: string | null = null,
    options: {
      readonly stalledForMs?: number;
      /**
       * Поправка контекста перед повтором (поля поверх прежних): застряли на
       * неверных бизнес-данных — повтор без поправки упадёт так же. Изменённые
       * ключи — в журнал.
       */
      readonly contextPatch?: object;
    } = {},
  ): Promise<void> {
    const patch =
      options.contextPatch === undefined
        ? null
        : sagaOutput('contextPatch ручного повтора', options.contextPatch);
    if (patch !== null && Object.keys(patch).length === 0) {
      // Пустая поправка (и null) — не «без поправки»: в журнал и хук ушло бы «контекст поправлен».
      throw new SagaConfigError('contextPatch', 'объект хотя бы с одним полем');
    }
    // Тип команды знает только оркестратор: угаданный `saga.command` при своих
    // eventTypes уходит в DEAD («нет обработчика»), а экземпляр навсегда
    // остаётся compensating — вне отчёта о застрявших и вне повторов.
    const outbox = this._deps.outbox;
    if (outbox === undefined) {
      throw new SagaConfigError(
        'outbox',
        'ручному повтору нужен outbox той же базы — в него пишется команда',
      );
    }
    const eventType = this._deps.commandEventType;
    if (typeof eventType !== 'string' || eventType.length === 0) {
      throw new SagaConfigError(
        'commandEventType',
        'ручному повтору нужен тип команды оркестратора (orchestrator.eventTypes.command)',
      );
    }
    if (options.stalledForMs !== undefined) {
      durationMs('stalledForMs', options.stalledForMs);
    }
    let retried = null as SagaInstance | null;
    await this._deps.transaction(async (tx) => {
      const instance = await this._store.lock(tx, id);
      // Время — после ожидания строки: шаг, державший её, мог идти секундами.
      // Отметка (updated_at, журнал) — не раньше последнего сдвига; срок
      // команды — по своим часам: зажим перенёс бы в него часы реплики,
      // ушедшей вперёд, и команда ждала бы, пока реальное время их догонит.
      const own = this._clock();
      const now = instance === null ? own : sagaTimeAfter(own, instance);
      if (instance === null || !SagaAdmin._eligible(instance, now, options.stalledForMs)) {
        throw new SagaStateError(id, instance?.status ?? null, 'повтор');
      }
      retried = instance;
      const generation = integer('generation', instance.generation + 1, 1);
      const phase = await this._phaseOf(tx, instance);
      await this._store.update(
        tx,
        id,
        {
          status: phase === SAGA_PHASE.execute ? SAGA_STATUS.running : SAGA_STATUS.compensating,
          stepIndex: instance.stepIndex,
          attempt: 1,
          generation,
          context: patch === null ? instance.context : { ...instance.context, ...patch },
          failure: instance.failure,
          lastError: instance.lastError,
          finishedAt: null,
        },
        now,
      );
      await this._store.append(
        tx,
        id,
        [
          {
            event: SAGA_JOURNAL_EVENT.manualRetry,
            phase,
            stepIndex: instance.stepIndex,
            step: instance.steps[instance.stepIndex] ?? null,
            attempt: 1,
            generation,
            detail: SagaAdmin._retryDetail(note, patch),
          },
        ],
        now,
      );
      const address = {
        sagaId: id,
        generation,
        phase,
        stepIndex: instance.stepIndex,
        attempt: 1,
      };
      const payload: SagaCommandPayload = {
        commandId: sagaMessageId.command({ ...address, incarnation: instance.incarnation }),
        ...address,
      };
      await outbox.enqueue(
        tx,
        [
          {
            id: payload.commandId,
            aggregateType: SAGA_AGGREGATE_TYPE.command,
            aggregateId: id,
            eventType,
            payload,
          },
        ],
        own,
      );
    });
    if (retried !== null) {
      this._emit('manualRetry', {
        saga: retried.name,
        sagaId: retried.id,
        from: retried.status,
        stalled: SAGA_ACTIVE_STATUSES.includes(retried.status),
        contextPatched: patch !== null,
      });
    }
  }

  /**
   * Фаза, в которой повторить шаг: у `blocked` — где он встал (запись
   * `delivery-dead` его поколения), у прочих — по состоянию. Записи нет
   * (строку правили руками) — отказ: угаданная фаза запустила бы не то.
   */
  private async _phaseOf(
    tx: Parameters<Parameters<SagaAdminDeps['transaction']>[0]>[0],
    instance: SagaInstance,
  ) {
    if (instance.status === SAGA_STATUS.running) {
      return SAGA_PHASE.execute;
    }
    if (instance.status !== SAGA_STATUS.blocked) {
      return SAGA_PHASE.compensate;
    }
    const entry = await this._store.lastEntry(
      tx,
      instance.id,
      SAGA_JOURNAL_EVENT.deliveryDead,
      instance.generation,
    );
    if (entry?.phase !== SAGA_PHASE.execute && entry?.phase !== SAGA_PHASE.compensate) {
      throw new SagaStateError(
        instance.id,
        instance.status,
        'повтор (нет записи delivery-dead поколения — фаза неизвестна)',
      );
    }
    return entry.phase;
  }

  /**
   * Пометить застрявший экземпляр разобранным вручную (`resolved`, конечное):
   * причина обязательна — она остаётся в журнале.
   */
  public async resolve(
    id: string,
    note: string,
    options: { readonly stalledForMs?: number } = {},
  ): Promise<void> {
    if (typeof note !== 'string' || note.trim().length === 0) {
      throw new SagaConfigError('note', 'причина разбора обязательна');
    }
    if (options.stalledForMs !== undefined) {
      durationMs('stalledForMs', options.stalledForMs);
    }
    let resolved = null as SagaInstance | null;
    await this._deps.transaction(async (tx) => {
      const instance = await this._store.lock(tx, id);
      const now = instance === null ? this._clock() : sagaTimeAfter(this._clock(), instance);
      if (instance === null || !SagaAdmin._eligible(instance, now, options.stalledForMs)) {
        throw new SagaStateError(id, instance?.status ?? null, 'разбор');
      }
      resolved = instance;
      await this._store.update(
        tx,
        id,
        {
          status: SAGA_STATUS.resolved,
          stepIndex: instance.stepIndex,
          attempt: instance.attempt,
          generation: instance.generation,
          context: instance.context,
          failure: instance.failure,
          lastError: instance.lastError,
          finishedAt: now,
        },
        now,
      );
      await this._store.append(
        tx,
        id,
        [
          {
            event: SAGA_JOURNAL_EVENT.manualResolve,
            phase: null,
            stepIndex: instance.stepIndex,
            step: instance.steps[instance.stepIndex] ?? null,
            attempt: instance.attempt,
            generation: instance.generation,
            detail: sagaErrorText(note),
          },
        ],
        now,
      );
    });
    if (resolved !== null) {
      this._emit('manualResolve', { saga: resolved.name, sagaId: resolved.id });
    }
  }

  /** Удалить завершённые экземпляры (`completed`, `compensated`, `resolved`) старше `before`, до `limit`. */
  public async prune(before: Date, limit = 5_000): Promise<number> {
    sagaDate('before', before);
    const batch = integer('limit', limit, 1);
    if (batch > SAGA_LIMIT.listLimit) {
      throw new SagaConfigError('limit', `не больше ${SAGA_LIMIT.listLimit} за вызов`);
    }
    return await this._store.prune(this._deps.db, before, batch);
  }
}
