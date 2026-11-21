import { randomInt } from 'node:crypto';

import type { NewOutboxMessage, OutboxHandler, OutboxTransaction } from '@hoshinotatsuya/outbox';
import { sql } from 'drizzle-orm';

import { sagaDate } from '../config/config-date';
import { integer } from '../config/config-integer';
import { sagaName } from '../config/config-name';
import { SAGA_ACTIVE_STATUSES } from '../constants/saga-active-statuses.constant';
import { SAGA_AGGREGATE_TYPE } from '../constants/saga-aggregate-type.constant';
import { SAGA_DEFAULTS } from '../constants/saga-defaults.constant';
import { SAGA_EVENT_TYPE } from '../constants/saga-event-type.constant';
import { SAGA_HANDLED } from '../constants/saga-handled.constant';
import { SAGA_JOURNAL_EVENT } from '../constants/saga-journal-event.constant';
import { SAGA_LIMIT } from '../constants/saga-limit.constant';
import { SAGA_PHASE } from '../constants/saga-phase.constant';
import { SAGA_REPLY_OUTCOME } from '../constants/saga-reply-outcome.constant';
import { SAGA_STATUS } from '../constants/saga-status.constant';
import { SagaConfigError } from '../errors/saga-config.error';
import { SagaDefinitionUnavailableError } from '../errors/saga-definition-unavailable.error';
import { SagaPayloadError } from '../errors/saga-payload.error';
import { SagaStepRejectedError } from '../errors/saga-step-rejected.error';
import { SagaStepTimeoutError } from '../errors/saga-step-timeout.error';
import { parseSagaReply } from '../remote/parse-saga-reply';
import { SagaStore } from '../store/saga-store';
import type { SagaCommandDraft } from '../types/saga-command-draft.type';
import type { SagaCommandMeta } from '../types/saga-command-meta.type';
import type { SagaCommandPayload } from '../types/saga-command-payload.type';
import type { SagaContext } from '../types/saga-context.type';
import type { SagaDefinition } from '../types/saga-definition.type';
import type { SagaEventTypes } from '../types/saga-event-types.type';
import type { SagaHandled } from '../types/saga-handled.type';
import type { SagaInstance } from '../types/saga-instance.type';
import type { SagaLogger } from '../types/saga-logger.type';
import type { SagaMoment } from '../types/saga-moment.type';
import type { SagaOrchestratorDeps } from '../types/saga-orchestrator-deps.type';
import type { SagaRemoteCommand } from '../types/saga-remote-command.type';
import type { SagaSignal } from '../types/saga-signal.type';
import type { SagaStartResult } from '../types/saga-start-result.type';
import type { SagaStep } from '../types/saga-step.type';
import type { SagaTransactionRunner } from '../types/saga-transaction-runner.type';
import type { SagaTransition } from '../types/saga-transition.type';
import { parseSagaCommandPayload } from './parse-saga-command-payload';
import { raceSignal } from './race-signal';
import { sagaAggregateAheadMs } from './saga-aggregate-ahead-ms';
import { sagaEnqueueRejected } from './saga-enqueue-rejected';
import { sagaErrorText } from './saga-error-text';
import { sagaFencedTransaction } from './saga-fenced-transaction';
import { sagaFrozenContext } from './saga-frozen-context';
import { sagaLateReply } from './saga-late-reply';
import { sagaMessageBelongs } from './saga-message-belongs';
import { sagaMessageId } from './saga-message-id';
import { sagaOutput } from './saga-output';
import { sagaRemoteAggregateType } from './saga-remote-aggregate-type';
import { sagaRunnerLimits } from './saga-runner-limits';
import { SagaStepFailure } from './saga-step-failure';
import { sagaStepKey } from './saga-step-key';
import { sagaTimeAfter } from './saga-time-after';
import { sagaTransactionRunner } from './saga-transaction-runner';
import { SagaTransitions } from './saga-transitions';
import { sagaValueText } from './saga-value-text';

/**
 * Оркестратор саг на Postgres поверх outbox.
 *
 * Каждое сообщение саги (команда шага, таймер, ответ участника) обрабатывается
 * ОДНОЙ транзакцией: отметка inbox (сообщение применяется один раз) →
 * блокировка строки экземпляра `FOR UPDATE` (один исполнитель на экземпляр;
 * второй ждёт и читает уже сдвинутое состояние) → проверка, что экземпляр
 * ждёт именно это сообщение (шаг, фаза, попытка, поколение — и воплощение:
 * id сообщения выведен из момента создания экземпляра) → шаг → новое
 * состояние, журнал и следующая команда в outbox. Время перехода снимается
 * ПОСЛЕ блокировки строки. Эффекты локального шага,
 * запись о нём и следующая команда фиксируются вместе или не фиксируются
 * вовсе.
 *
 * Отказ самого шага откатывает его транзакцию и записывается второй:
 * повтор с паузой или компенсации завершённых шагов в обратном порядке;
 * компенсация, исчерпавшая повторы, — `stuck` (ручной разбор, `SagaAdmin`).
 * Отказ инфраструктуры (обрыв, неизвестный исход COMMIT, блокировка) уходит
 * наружу: доставку повторит outbox, а inbox и проверка ожидания не дадут
 * применить её дважды.
 */
export class SagaOrchestrator {
  public readonly eventTypes: SagaEventTypes;

  private readonly _store: SagaStore;

  private readonly _transaction: SagaTransactionRunner;

  private readonly _consumer: string;

  private readonly _clock: () => Date;

  private readonly _random: () => number;

  private readonly _logger: SagaLogger;

  private readonly _definitions = new Map<string, Map<number, SagaDefinition>>();

  /**
   * Потолок доставки релея — из настроек хранилища outbox, над которым его
   * строят (`OutboxRelay` берёт его оттуда же): копия значения по умолчанию
   * расходилась бы с настроенным релеем.
   */
  private readonly _deliveryTimeoutMs: number;

  /**
   * Ожидание строки экземпляра у своей транзакции оркестратора (без
   * `deps.transaction`) и у своей обёртки, чьи потолки пакету неизвестны.
   * Раннер `sagaTransactionRunner` приносит свои (`sagaRunnerLimits`).
   */
  private readonly _lockTimeoutMs: number;

  public constructor(private readonly _deps: SagaOrchestratorDeps) {
    this._store = new SagaStore(_deps.tables);
    const config = (_deps.outbox as { config?: SagaOrchestratorDeps['outbox']['config'] } | undefined)
      ?.config;
    this._deliveryTimeoutMs = integer('outbox.config.deliveryTimeoutMs', config?.deliveryTimeoutMs, 1);
    // Не число (NaN, −1, 1.5) прошло бы register — сверка бюджета с NaN всегда
    // «укладывается», — и первая доставка отказала бы на сервере до DEAD.
    this._lockTimeoutMs = integer('lockTimeoutMs', _deps.lockTimeoutMs ?? SAGA_DEFAULTS.lockTimeoutMs, 1);
    // Простой транзакции — до потолка доставки: законный локальный шаг
    // (`register` держит его короче потолка) им не обрывается, а сессия,
    // брошенная клиентом в замёрзшей сети, отпускает строку экземпляра.
    this._transaction =
      _deps.transaction ??
      sagaTransactionRunner(_deps.db, {
        lockTimeoutMs: this._lockTimeoutMs,
        idleInTransactionTimeoutMs: this._deliveryTimeoutMs,
      });
    this._consumer = _deps.consumer ?? SAGA_DEFAULTS.consumer;
    this._clock = _deps.clock ?? ((): Date => new Date());
    this._random = _deps.random ?? ((): number => Math.random());
    this._logger = _deps.logger ?? { warn: (message) => console.warn(message) };
    this.eventTypes = Object.freeze({ ...SAGA_EVENT_TYPE, ..._deps.eventTypes });
    if (new Set(Object.values(this.eventTypes)).size !== 3) {
      throw new SagaConfigError('eventTypes', 'типы команды, таймера и ответа обязаны различаться');
    }
    for (const [kind, eventType] of Object.entries(this.eventTypes)) {
      // event_type outbox — varchar(64). Длиннее или со слиянием (outbox
      // отказывает событию с id у такого типа) — отказ записи каждой команды,
      // таймера или ответа: замёрзший экземпляр вместо отказа на старте.
      sagaName(`eventTypes.${kind}`, eventType);
      if (config?.coalescing?.rules?.[eventType] !== undefined) {
        throw new SagaConfigError(
          `eventTypes.${kind}`,
          `у типа ${eventType} в outbox правило слияния: события саги идут со своим id, слиянию не подлежат`,
        );
      }
    }
  }

  /**
   * Зарегистрировать определение. Версий одной саги может быть несколько:
   * экземпляр доезжает по той, с которой начат. Та же версия второй раз —
   * отказ (молчаливая подмена была бы видна только по сломанным экземплярам).
   */
  public register(definition: SagaDefinition<object, object>): this {
    const versions = this._definitions.get(definition.name) ?? new Map<number, SagaDefinition>();
    if (versions.has(definition.version)) {
      throw new SagaConfigError(
        'definition',
        `сага ${definition.name} версии ${definition.version} уже зарегистрирована`,
      );
    }
    SagaOrchestrator._checkShape(definition as SagaDefinition);
    this._checkDelivery(definition as SagaDefinition);
    versions.set(definition.version, definition as SagaDefinition);
    this._definitions.set(definition.name, versions);
    return this;
  }

  /**
   * Подписать обработчики команд, таймеров и ответов (обычно — `OutboxHandlers`
   * релея, построенного над тем же хранилищем outbox, что `deps.outbox`).
   */
  public registerHandlers(target: { register(eventType: string, handler: OutboxHandler): unknown }): void {
    for (const eventType of Object.values(this.eventTypes)) {
      target.register(eventType, async (payload, _delivery, context) => {
        await this.handle(eventType, payload, context?.signal);
      });
    }
  }

  /**
   * Начать экземпляр в ТРАНЗАКЦИИ ВЫЗЫВАЮЩЕГО: экземпляр и первая команда
   * появляются вместе с его записью или не появляются вовсе. Повтор с тем же
   * `id` ничего не делает (`started: false`). `now` — момент создания
   * (`created_at`, журнал); срок первой команды — по часам оркестратора.
   */
  public async start<Input extends object>(
    tx: OutboxTransaction,
    definition: SagaDefinition<Input, object>,
    args: { readonly id: string; readonly input: Input },
    now: Date = this._clock(),
  ): Promise<SagaStartResult> {
    if (typeof (tx as { rollback?: unknown } | null)?.rollback !== 'function') {
      throw new TypeError('pg-saga: start — нужна ТРАНЗАКЦИЯ вызывающего, а получена база без rollback()');
    }
    if (this._definitions.get(definition.name)?.get(definition.version) !== definition) {
      throw new SagaConfigError(
        'definition',
        `сага ${definition.name} версии ${definition.version} не зарегистрирована в оркестраторе (register)`,
      );
    }
    const { id } = args;
    if (
      typeof id !== 'string' ||
      id.length === 0 ||
      id.length > SAGA_LIMIT.idLength ||
      id.includes('\u0000') ||
      !id.isWellFormed()
    ) {
      // NUL и непарный суррогат — отказ сервера посреди транзакции вызывающего.
      throw new SagaConfigError(
        'id',
        `непустая строка до ${SAGA_LIMIT.idLength} знаков без NUL и непарных суррогатов`,
      );
    }
    const input: unknown = args.input;
    if (typeof input !== 'object' || input === null || Array.isArray(input)) {
      // null не становится молча пустым контекстом: вход — объект.
      throw new SagaConfigError(
        'input',
        `вход саги — объект, получено ${input === null ? 'null' : typeof input}`,
      );
    }
    sagaDate('now', now);
    const context = sagaOutput(`старт ${definition.name}`, input);
    // Воплощение — случайное, а не момент старта: тот же id после `prune` при
    // тех же часах (неподвижные часы теста, `now` из времени события) иначе
    // дал бы те же id сообщений — outbox и inbox сочли бы первую команду
    // дублем, и сага не поехала бы никогда. Не `deps.random`: его подменяют
    // постоянным ради предсказуемой паузы повтора.
    const incarnation = randomInt(0, 2 ** 48 - 1);
    const started = await this._store.insert(
      tx,
      {
        id,
        name: definition.name,
        version: definition.version,
        steps: definition.steps.map((s) => s.name),
        incarnation,
        context,
      },
      now,
    );
    if (!started) {
      const existing = await this._store.get(tx, id);
      return existing === null
        ? { started: false }
        : {
            started: false,
            existing: { name: existing.name, version: existing.version, status: existing.status },
          };
    }
    await this._store.append(
      tx,
      id,
      [
        {
          event: SAGA_JOURNAL_EVENT.sagaStarted,
          phase: null,
          stepIndex: null,
          step: null,
          attempt: null,
          generation: 0,
          detail: `версия ${definition.version}`,
        },
      ],
      now,
    );
    await this._enqueueCommand(
      tx,
      { id, incarnation },
      { phase: SAGA_PHASE.execute, stepIndex: 0, attempt: 1, generation: 0, delayMs: null },
      null,
      this._clock(),
    );
    return { started: true };
  }

  /**
   * Отменить ИДУЩУЮ сагу в ТРАНЗАКЦИИ ВЫЗЫВАЮЩЕГО (отмена заказа и т. п.):
   * новое поколение — летящие команда, таймер и поздний ответ больше не
   * применяются, — и компенсации выполненных шагов в обратном порядке; текущий
   * удалённый шаг (команда могла уйти) компенсируется тоже. Не `running`
   * (уже откатывается, завершена, застряла) — ничего не делает и говорит
   * состояние. Хука метрик в транзакции вызывающего нет (она может
   * откатиться); хук `compensated` вызывает оркестратор после своей фиксации —
   * и когда компенсировать было нечего (отмена записывает ему уведомление).
   * `at` — отметка перехода (журнал, `updated_at`); по умолчанию часы
   * оркестратора ПОСЛЕ блокировки строки. Сроки команд — всегда по часам
   * оркестратора.
   */
  public async cancel(
    tx: OutboxTransaction,
    id: string,
    reason: string,
    at?: Date,
  ): Promise<{ readonly cancelled: boolean; readonly status: SagaInstance['status'] | null }> {
    if (typeof (tx as { rollback?: unknown } | null)?.rollback !== 'function') {
      throw new TypeError('pg-saga: cancel — нужна ТРАНЗАКЦИЯ вызывающего, а получена база без rollback()');
    }
    if (typeof reason !== 'string' || reason.trim().length === 0) {
      throw new SagaConfigError('reason', 'причина отмены обязательна');
    }
    if (at !== undefined) {
      sagaDate('at', at);
    }
    const instance = await this._store.lock(tx, id);
    if (instance === null || instance.status !== SAGA_STATUS.running) {
      return { cancelled: false, status: instance?.status ?? null };
    }
    // Новое поколение — int4: отказ до записи, а не отказ сервера посреди транзакции вызывающего.
    integer('generation', instance.generation + 1, 1);
    // Время — после ожидания строки: шаг, державший её, мог идти секундами.
    const moment = this._momentAfter(instance);
    // Своя отметка — тоже не раньше последнего сдвига: иначе updated_at и
    // журнал ушли бы назад, а finished_at — раньше created_at.
    const time = at === undefined ? moment : { at: sagaTimeAfter(at, instance), own: moment.own };
    const definition = this._definitionOf(instance);
    const transition = SagaTransitions.cancel(definition, instance, reason, time.at);
    await this._apply(tx, instance, transition, time, []);
    return { cancelled: true, status: transition.state.status };
  }

  /**
   * Обработать сообщение саги по типу события outbox. `signal` — сигнал
   * доставки релея: по нему брошенная релеем доставка не записывается как
   * отказ шага (релей её повторит).
   */
  public async handle(eventType: string, payload: unknown, signal?: AbortSignal): Promise<SagaHandled> {
    switch (eventType) {
      case this.eventTypes.command:
        return await this._onCommand(parseSagaCommandPayload('команда', payload), signal);
      case this.eventTypes.timer:
        return await this._onTimer(parseSagaCommandPayload('таймер', payload));
      case this.eventTypes.reply:
        return await this.handleReply(payload);
      default:
        throw new SagaPayloadError(`тип события ${eventType} не принадлежит саге`);
    }
  }

  /**
   * Ответ удалённого участника (из своего транспорта или события ответа).
   * Применяется не больше раза на команду; ответ на попытку, которую уже не
   * ждут, ничего не меняет. Успех ПРЕЖНЕЙ попытки того же шага принимается:
   * шаг выполнен, повтор у участника отсечёт его идемпотентность по `stepKey`.
   */
  public async handleReply(payload: unknown): Promise<SagaHandled> {
    const reply = parseSagaReply(payload);
    return await this._inTransaction(reply.commandId, async (tx, signals) => {
      const instance = await this._lock(tx, reply.sagaId);
      if (instance === null) {
        return SAGA_HANDLED.stale;
      }
      if (!sagaMessageBelongs('remote', instance, reply)) {
        // Ответ на команду, которую это воплощение не выдавало: прежний
        // экземпляр того же id (до prune) или выдуманный id. Ни применения, ни
        // записи в журнал нового экземпляра.
        this._warn(
          `[pg-saga] ответ ${reply.commandId} не на команду экземпляра ${instance.id} (прежнее воплощение?) — устарел`,
        );
        return SAGA_HANDLED.stale;
      }
      const time = this._momentAfter(instance);
      const now = time.at;
      const ok = reply.outcome === SAGA_REPLY_OUTCOME.ok;
      const awaited =
        SAGA_ACTIVE_STATUSES.includes(instance.status) &&
        reply.generation === instance.generation &&
        reply.phase === SagaTransitions.phaseOf(instance) &&
        reply.stepIndex === instance.stepIndex &&
        (reply.attempt === instance.attempt || (ok && reply.attempt < instance.attempt));
      if (!awaited) {
        // Поздний успех — событие, только если эффект у участника есть, а сага
        // его не учла (`sagaLateReply`): это решает журнал, а не статус.
        const late = ok
          ? sagaLateReply(instance, reply, await this._store.stepFacts(tx, instance.id, reply))
          : null;
        if (late !== null) {
          await this._store.append(tx, instance.id, [late], now);
        }
        return SAGA_HANDLED.stale;
      }
      const definition = this._definitionOf(instance);
      const step = this._stepOf(definition, instance);
      if (step.kind !== 'remote') {
        // Шаг стал локальным без новой версии: сверка по именам этого не видит,
        // а ответ не применится никогда. Причина и метрика — как у разошедшихся
        // шагов, а не безликий отказ разбора.
        throw new SagaDefinitionUnavailableError(
          instance.name,
          instance.version,
          `ответ участника на шаг ${step.name}, который в определении локальный — вид шага сменили без новой версии (экземпляр ${instance.id})`,
        );
      }
      let output: SagaContext = {};
      let unusable: string | null = null;
      // Отказ, пришедший после попытки с неизвестным исходом (таймаут ответа):
      // прежняя попытка могла выполниться у участника — компенсируется и шаг.
      // Защита в глубину: идемпотентный по stepKey участник ответил бы успехом.
      const uncertainBefore =
        !ok &&
        reply.phase === SAGA_PHASE.execute &&
        (await this._store.stepTimedOut(tx, instance.id, instance.stepIndex, instance.generation));
      if (ok && reply.phase === SAGA_PHASE.execute) {
        try {
          output = sagaOutput(`ответ ${step.name}`, reply.data);
        } catch (error) {
          if (!(error instanceof SagaPayloadError)) {
            throw error;
          }
          // Участник шаг выполнил, но данные ответа сохранить нельзя: исход
          // для саги неизвестен — отказ без повторов, компенсируется и сам шаг.
          // Бросок здесь повторялся бы на каждой доставке ответа до DEAD.
          unusable = sagaErrorText(error);
        }
      }
      const transition =
        unusable !== null
          ? SagaTransitions.failure(
              definition,
              instance,
              { error: unusable, retryable: false, uncertain: true, timedOut: false },
              now,
              this._random,
            )
          : ok
            ? SagaTransitions.success(definition, instance, output, now)
            : SagaTransitions.failure(
                definition,
                instance,
                {
                  error: reply.error ?? 'участник ответил отказом без причины',
                  retryable: reply.retryable !== false,
                  uncertain: uncertainBefore,
                  timedOut: false,
                },
                now,
                this._random,
              );
      await this._apply(tx, instance, transition, time, signals);
      return ok && unusable === null ? SAGA_HANDLED.applied : SAGA_HANDLED.failureRecorded;
    });
  }

  private async _onCommand(
    command: SagaCommandPayload,
    signal: AbortSignal | undefined,
  ): Promise<SagaHandled> {
    try {
      return await this._inTransaction(command.commandId, async (tx, signals) => {
        const instance = await this._lock(tx, command.sagaId);
        if (instance !== null && this._isCancelNotice(instance, command)) {
          // Отмена завершила сагу сама (компенсировать было нечего) в
          // транзакции вызывающего: хук — здесь, после фиксации этой.
          signals.push({
            kind: 'compensated',
            event: { saga: instance.name, sagaId: instance.id, failure: instance.failure },
          });
          return SAGA_HANDLED.applied;
        }
        if (instance === null || !this._awaits(instance, command, 'command')) {
          this._warnForeign(instance, command, 'command');
          return SAGA_HANDLED.stale;
        }
        const definition = this._definitionOf(instance);
        const step = this._stepOf(definition, instance);
        if (step.kind === 'remote') {
          await this._dispatch(tx, instance, definition, step, command, this._momentAfter(instance));
          return SAGA_HANDLED.dispatched;
        }
        const output = await this._runLocal(tx, instance, step, signal);
        // Время ПОСЛЕ шага: шаг идёт секундами, а от этого момента считаются
        // `finished_at`, длительность саги и пауза до следующей команды.
        const done = this._momentAfter(instance);
        await this._apply(
          tx,
          instance,
          SagaTransitions.success(definition, instance, output, done.at),
          done,
          signals,
        );
        return SAGA_HANDLED.applied;
      });
    } catch (error) {
      if (!(error instanceof SagaStepFailure)) {
        throw error;
      }
      return await this._recordFailure(command, error.reason);
    }
  }

  /**
   * Отказ шага — своей транзакцией, после отката транзакции шага (его эффекты
   * откатились вместе с ней). Отметка inbox команды ставится здесь: повторная
   * доставка той же команды отказ второй раз не запишет.
   */
  private async _recordFailure(command: SagaCommandPayload, reason: unknown): Promise<SagaHandled> {
    return await this._inTransaction(command.commandId, async (tx, signals) => {
      const instance = await this._lock(tx, command.sagaId);
      // Пока откатывалась транзакция шага, экземпляр мог сдвинуться (отмена,
      // ручной повтор): отказ не к чему применять.
      if (instance === null || !this._awaits(instance, command, 'command')) {
        return SAGA_HANDLED.stale;
      }
      // Пауза повтора — от момента отказа, а не от начала шага: иначе шаг дольше
      // паузы (таймаут 15 с при паузе 1 с) ставил бы повтор в прошлое — сразу.
      const time = this._momentAfter(instance);
      const definition = this._definitionOf(instance);
      const transition = SagaTransitions.failure(
        definition,
        instance,
        {
          error: sagaErrorText(reason),
          retryable: !SagaStepRejectedError.is(reason) && !(reason instanceof SagaPayloadError),
          uncertain: false,
          timedOut: reason instanceof SagaStepTimeoutError,
        },
        time.at,
        this._random,
      );
      await this._apply(tx, instance, transition, time, signals);
      return SAGA_HANDLED.failureRecorded;
    });
  }

  /** Таймер удалённого шага: ответ на эту попытку не пришёл — отказ с неизвестным исходом. */
  private async _onTimer(timer: SagaCommandPayload): Promise<SagaHandled> {
    return await this._inTransaction(timer.commandId, async (tx, signals) => {
      const instance = await this._lock(tx, timer.sagaId);
      if (instance === null || !this._awaits(instance, timer, 'timer')) {
        this._warnForeign(instance, timer, 'timer');
        return SAGA_HANDLED.stale;
      }
      const time = this._momentAfter(instance);
      const definition = this._definitionOf(instance);
      const step = this._stepOf(definition, instance);
      const transition = SagaTransitions.failure(
        definition,
        instance,
        {
          error: `нет ответа участника за ${String(step.timeoutMs)} мс`,
          retryable: true,
          uncertain: true,
          timedOut: true,
        },
        time.at,
        this._random,
      );
      await this._apply(tx, instance, transition, time, signals);
      return SAGA_HANDLED.failureRecorded;
    });
  }

  /**
   * Одна транзакция на сообщение: отметка inbox по `messageId` и работа — вместе.
   * Сигналы метрик — после фиксации; при повторе транзакции (40001) — заново.
   */
  private async _inTransaction(
    messageId: string,
    work: (tx: OutboxTransaction, signals: SagaSignal[]) => Promise<SagaHandled>,
  ): Promise<SagaHandled> {
    let signals: SagaSignal[] = [];
    try {
      const handled = await this._transaction(async (tx) => {
        signals = [];
        // Отметка inbox — по часам оркестратора, как и всё остальное в
        // переходе; время самого перехода снимается позже, под блокировкой строки.
        const once = await this._deps.inbox.once(
          tx,
          this._consumer,
          messageId,
          () => work(tx, signals),
          this._clock(),
        );
        return once.processed ? once.result : SAGA_HANDLED.duplicate;
      });
      this._emit(signals);
      return handled;
    } catch (error) {
      if (error instanceof SagaDefinitionUnavailableError) {
        this._emit([
          {
            kind: 'definitionUnavailable',
            event: { saga: error.saga, version: error.version, reason: error.reason },
          },
        ]);
      }
      throw error;
    }
  }

  /**
   * Экземпляр под блокировкой. Нет строки — сообщение устарело: экземпляр
   * удалён `prune` (а таймер удалённого шага ещё ждал срока) или ответ пришёл
   * после удаления. Ошибка здесь была бы детерминированной: повторы доставки,
   * затем DEAD — на каждую удалённую сагу с ждущим таймером.
   */
  private async _lock(tx: OutboxTransaction, sagaId: string): Promise<SagaInstance | null> {
    const instance = await this._store.lock(tx, sagaId);
    if (instance === null) {
      this._warn(`[pg-saga] сообщение экземпляру ${sagaId}, которого нет (удалён prune?) — устарело`);
    }
    return instance;
  }

  private _warn(message: string): void {
    try {
      this._logger.warn(message);
    } catch {
      // Наблюдение не вправе ломать доставку.
    }
  }

  /**
   * Время перехода — после блокировки строки: отметка не раньше последнего
   * сдвига (`sagaTimeAfter`), сроки — от своих часов (`SagaMoment`).
   */
  private _momentAfter(instance: SagaInstance): SagaMoment {
    const own = this._clock();
    return { at: sagaTimeAfter(own, instance), own };
  }

  /**
   * Команда или таймер с адресом, который экземпляр ждёт, но с чужим id: это
   * сообщение прежнего воплощения того же id (до `prune`) — или команда
   * ручного повтора из админки/CLI версии 0.1.0, которая не знает воплощения и
   * оставила бы экземпляр стоять в новом поколении. Не применяется (`stale`),
   * но и не молчит: без строки в логе такой экземпляр виден только в отчёте о
   * замёрзших.
   */
  private _warnForeign(
    instance: SagaInstance | null,
    message: SagaCommandPayload,
    kind: 'command' | 'timer',
  ): void {
    if (
      instance !== null &&
      SAGA_ACTIVE_STATUSES.includes(instance.status) &&
      message.generation === instance.generation &&
      message.phase === SagaTransitions.phaseOf(instance) &&
      message.stepIndex === instance.stepIndex &&
      message.attempt === instance.attempt &&
      !sagaMessageBelongs(kind, instance, message)
    ) {
      this._warn(
        `[pg-saga] ${kind === 'command' ? 'команда' : 'таймер'} ${message.commandId} с адресом, который ждёт экземпляр ` +
          `${instance.id}, выдан не им (прежнее воплощение или ручной повтор из админки/CLI 0.1.0) — не применён; ` +
          'повторите экземпляр админкой текущей версии',
      );
    }
  }

  /**
   * Экземпляр ждёт ровно это сообщение: активен, то же поколение, фаза, шаг и
   * попытка — и сообщение выдано этим воплощением (id от момента создания).
   */
  private _awaits(instance: SagaInstance, message: SagaCommandPayload, kind: 'command' | 'timer'): boolean {
    return (
      SAGA_ACTIVE_STATUSES.includes(instance.status) &&
      message.generation === instance.generation &&
      message.phase === SagaTransitions.phaseOf(instance) &&
      message.stepIndex === instance.stepIndex &&
      message.attempt === instance.attempt &&
      sagaMessageBelongs(kind, instance, message)
    );
  }

  /**
   * Уведомление отмены, завершившей сагу сразу (`SagaTransitions.cancel`):
   * экземпляр compensated, а адрес команды — его же (поколение отмены,
   * компенсация, шаг и попытка, на которых отменили). Другой команды с этим
   * адресом не бывает: поколение отмены ничего, кроме уведомления, не выдаёт.
   */
  private _isCancelNotice(instance: SagaInstance, command: SagaCommandPayload): boolean {
    return (
      instance.status === SAGA_STATUS.compensated &&
      command.generation === instance.generation &&
      command.phase === SAGA_PHASE.compensate &&
      command.stepIndex === instance.stepIndex &&
      command.attempt === instance.attempt &&
      sagaMessageBelongs('command', instance, command)
    );
  }

  /**
   * Определение, по которому начат экземпляр: та же версия И те же шаги.
   * Иначе — отказ с причиной; экземпляр не трогается.
   */
  private _definitionOf(instance: SagaInstance): SagaDefinition {
    const definition = this._definitions.get(instance.name)?.get(instance.version);
    if (definition === undefined) {
      throw new SagaDefinitionUnavailableError(
        instance.name,
        instance.version,
        `версия не зарегистрирована в этом процессе (есть: ${
          [...(this._definitions.get(instance.name)?.keys() ?? [])].join(', ') || 'нет'
        }) — экземпляр ${instance.id} ждёт реплику с ней`,
      );
    }
    if (instance.stepIndex < 0 || instance.stepIndex >= instance.steps.length) {
      // Строку правили руками или миграцией: причина — явная, а не голый Error.
      throw new SagaDefinitionUnavailableError(
        instance.name,
        instance.version,
        `шаг ${instance.stepIndex} вне шагов экземпляра ${instance.id} (их ${instance.steps.length})`,
      );
    }
    const names = definition.steps.map((step) => step.name);
    if (
      names.length !== instance.steps.length ||
      names.some((name, index) => name !== instance.steps[index])
    ) {
      throw new SagaDefinitionUnavailableError(
        instance.name,
        instance.version,
        `шаги определения [${names.join(', ')}] разошлись с записанными в экземпляре ${instance.id} ` +
          `[${instance.steps.join(', ')}] — правка шагов требует новой версии`,
      );
    }
    return definition;
  }

  private _stepOf(definition: SagaDefinition, instance: SagaInstance): SagaStep {
    const step = definition.steps[instance.stepIndex];
    if (step === undefined) {
      throw new Error(`pg-saga: у саги ${definition.name} нет шага ${instance.stepIndex}`);
    }
    return step;
  }

  /** Локальный шаг или его компенсация в транзакции перехода; отказ — `SagaStepFailure`. */
  private async _runLocal(
    tx: OutboxTransaction,
    instance: SagaInstance,
    step: SagaStep,
    delivery: AbortSignal | undefined,
  ): Promise<SagaContext> {
    const phase = SagaTransitions.phaseOf(instance);
    const run = phase === SAGA_PHASE.execute ? step.execute : step.compensate;
    if (run === null) {
      throw new Error(`pg-saga: у шага ${step.name} нет ${phase}`);
    }
    // Потолок и для SQL шага: отмена ожидания в процессе не останавливает
    // запрос на сервере — без statement_timeout он держал бы строку экземпляра
    // и соединение до своего конца. Настройка транзакционная (is_local) и
    // возвращается после шага: переход и следующий шаг идут без неё.
    // Определение, собранное мимо построителя, может нести null: потолок всё
    // равно есть — по умолчанию, — иначе шаг висел бы до обрыва доставки релеем.
    const limitMs = step.timeoutMs ?? SAGA_DEFAULTS.localStepTimeoutMs;
    const restore = await SagaOrchestrator._limitStatements(tx, limitMs);
    const timeout = new AbortController();
    const signal = delivery === undefined ? timeout.signal : AbortSignal.any([timeout.signal, delivery]);
    // Транзакция шага закрывается вместе с шагом — и в момент отказа по
    // сигналу, раньше отката: брошенный код не пишет мимо транзакции. Каждый
    // запрос шага получает потолком ОСТАТОК бюджета: поздно начатый запрос не
    // переживёт потолок шага на сервере и не задержит ROLLBACK.
    const fence = sagaFencedTransaction(tx, step.name, { deadline: performance.now() + limitMs, limitMs });
    signal.addEventListener('abort', fence.close, { once: true });
    const timer = setTimeout(() => timeout.abort(new SagaStepTimeoutError(limitMs)), limitMs);
    try {
      const output = await raceSignal(
        Promise.resolve().then(() =>
          run({
            tx: fence.tx,
            context: sagaFrozenContext(instance.context) as never,
            sagaId: instance.id,
            stepKey: SagaOrchestrator._stepKey(instance, phase),
            executeStepKey: SagaOrchestrator._stepKey(instance, SAGA_PHASE.execute),
            attempt: instance.attempt,
            signal,
          }),
        ),
        signal,
      );
      await tx.execute(sql`SELECT set_config('statement_timeout', ${restore}, true)`);
      return phase === SAGA_PHASE.execute ? sagaOutput(`шаг ${step.name}`, output) : {};
    } catch (error) {
      if (delivery?.aborted === true && !timeout.signal.aborted) {
        // Релей бросил доставку (потолок, остановка): это не отказ шага, повторит релей.
        throw error;
      }
      // Потолок шага снимает либо таймер здесь, либо сервер (57014 от
      // statement_timeout) — смотря кто раньше; исход один: таймаут шага.
      const timedOut = timeout.signal.aborted || SagaOrchestrator._hasCode(error, '57014');
      // Шаг поймал ошибку своего SQL и вернулся: транзакция прервана (25P02), и
      // первая же следующая инструкция — возврат потолка SQL — падает. Это отказ
      // шага (с понятной причиной), а не инфраструктуры: иначе повторы
      // доставки до DEAD и вечный running.
      const swallowed = !timedOut && SagaOrchestrator._hasCode(error, '25P02');
      throw new SagaStepFailure(
        timeout.signal.aborted
          ? timeout.signal.reason
          : timedOut
            ? new SagaStepTimeoutError(limitMs)
            : swallowed
              ? new Error(`шаг ${step.name} проглотил ошибку SQL: транзакция прервана (25P02)`, {
                  cause: error,
                })
              : error,
      );
    } finally {
      clearTimeout(timer);
      signal.removeEventListener('abort', fence.close);
      fence.close();
    }
  }

  /** Команда удалённому участнику и таймер ответа — в той же транзакции. */
  private async _dispatch(
    tx: OutboxTransaction,
    instance: SagaInstance,
    definition: SagaDefinition,
    step: SagaStep,
    command: SagaCommandPayload,
    time: SagaMoment,
  ): Promise<void> {
    const phase = SagaTransitions.phaseOf(instance);
    const build = phase === SAGA_PHASE.execute ? step.command : step.compensation;
    if (build === null) {
      throw new Error(`pg-saga: у удалённого шага ${step.name} нет ${phase}`);
    }
    const stepKey = SagaOrchestrator._stepKey(instance, phase);
    let remote: SagaRemoteCommand;
    try {
      remote = build({
        context: sagaFrozenContext(instance.context) as never,
        sagaId: instance.id,
        stepKey,
        executeStepKey: SagaOrchestrator._stepKey(instance, SAGA_PHASE.execute),
        attempt: instance.attempt,
      });
      if (typeof remote?.eventType !== 'string' || remote.eventType.length === 0) {
        throw new SagaPayloadError(`команда шага ${step.name}: нет eventType`);
      }
      if (remote.aggregateType === SAGA_AGGREGATE_TYPE.command) {
        // Агрегат команд самой саги: команда лежащему участнику держала бы
        // при FIFO по агрегату компенсации и отмену этого экземпляра.
        throw new SagaPayloadError(
          `команда шага ${step.name}: aggregateType «${SAGA_AGGREGATE_TYPE.command}» — агрегат команд самой саги`,
        );
      }
    } catch (error) {
      throw new SagaStepFailure(error);
    }
    const key = {
      sagaId: instance.id,
      incarnation: instance.incarnation,
      generation: command.generation,
      phase: command.phase,
      stepIndex: command.stepIndex,
      attempt: command.attempt,
    };
    const meta: SagaCommandMeta = {
      commandId: sagaMessageId.remote(key),
      sagaId: instance.id,
      saga: definition.name,
      version: definition.version,
      generation: instance.generation,
      phase,
      stepIndex: instance.stepIndex,
      step: step.name,
      attempt: instance.attempt,
      stepKey,
      ...(instance.incarnation === null ? {} : { incarnation: instance.incarnation }),
      ...(phase === SAGA_PHASE.compensate
        ? { executeStepKey: SagaOrchestrator._stepKey(instance, SAGA_PHASE.execute) }
        : {}),
    };
    const timerId = sagaMessageId.timer(key);
    const aggregateType = sagaRemoteAggregateType(remote, instance.stepIndex);
    const created = await this._remoteCreatedAt(tx, aggregateType, instance.id, time.own);
    const messages: NewOutboxMessage[] = [
      {
        id: meta.commandId,
        // Агрегат — участник шага и экземпляр (`sagaRemoteAggregateType`): при
        // FIFO по агрегату команда лежащему участнику (повторы релея до суток)
        // держит только команды ТОМУ ЖЕ участнику этого экземпляра — его же
        // возврат не обгоняет списание, — а не компенсации у живых участников
        // и не команды самой саги.
        aggregateType,
        aggregateId: instance.id,
        eventType: remote.eventType,
        payload: { sagaCommand: meta, data: remote.payload },
        ...(remote.headers === undefined ? {} : { headers: remote.headers }),
      },
      {
        // Свой агрегат у каждого таймера: при FIFO по агрегату (outbox
        // `ordering: aggregate`) таймер с отложенной доставкой в агрегате
        // экземпляра держал бы за собой ответ участника и следующую команду
        // до своего срока — шаг ложно отваливался бы по таймауту.
        id: timerId,
        aggregateType: SAGA_AGGREGATE_TYPE.timer,
        aggregateId: timerId,
        eventType: this.eventTypes.timer,
        payload: { ...command, commandId: timerId },
        // timeoutMs удалённого шага проверен при регистрации (`_checkShape`).
        // Срок — от своих часов (`SagaMoment`), а не от отметки перехода; от
        // момента записи команды, если она встала за событием с часов впереди.
        deliverAt: new Date(created.getTime() + (step.timeoutMs ?? 0)),
      },
    ];
    try {
      await this._deps.outbox.enqueue(tx, messages, created);
    } catch (error) {
      // Команду, которую outbox не запишет НИКОГДА (не JSON, NUL, payload
      // больше maxPayloadBytes, заголовки без опции, eventType длиннее
      // varchar(64), …), — отказ шага без повторов, а не отказ доставки: тот
      // повторялся бы до DEAD, и экземпляр навсегда остался бы running.
      if (sagaEnqueueRejected(error)) {
        throw new SagaStepFailure(
          new SagaPayloadError(`команда шага ${step.name} не записывается в outbox: ${sagaErrorText(error)}`),
        );
      }
      throw error;
    }
    await this._store.awaitUntil(tx, instance.id, messages[1]?.deliverAt ?? created, time.at);
    await this._store.append(
      tx,
      instance.id,
      [
        {
          event: SAGA_JOURNAL_EVENT.stepDispatched,
          phase,
          stepIndex: instance.stepIndex,
          step: step.name,
          attempt: instance.attempt,
          generation: instance.generation,
          detail: remote.eventType,
        },
      ],
      time.at,
    );
  }

  /**
   * Момент записи команды участнику: свои часы — и строго позже ждущего
   * события того же агрегата, когда outbox упорядочивает по агрегату. Иначе
   * команда с отстающих (или тех же, до миллисекунды) часов встала бы в FIFO
   * ПЕРЕД ждущей: возврат обогнал бы списание, ушедшее с реплики, чьи часы
   * впереди. Такая команда ждёт, пока свои часы не дойдут до её момента, —
   * цена порядка при расхождении часов; без ждущего предшественника ожидания нет.
   */
  private async _remoteCreatedAt(
    tx: OutboxTransaction,
    aggregateType: string,
    aggregateId: string,
    own: Date,
  ): Promise<Date> {
    const config = this._deps.outbox.config;
    if (config.ordering === null || config.ordering === undefined) {
      return own;
    }
    const ahead = await sagaAggregateAheadMs(tx, config, { type: aggregateType, id: aggregateId }, own);
    return ahead === null || ahead < 0 ? own : new Date(own.getTime() + Math.floor(ahead) + 1);
  }

  /** Записать переход: состояние, журнал, следующая команда — в транзакцию шага. */
  private async _apply(
    tx: OutboxTransaction,
    instance: SagaInstance,
    transition: SagaTransition,
    time: SagaMoment,
    signals: SagaSignal[],
  ): Promise<void> {
    const delayMs = transition.command?.delayMs ?? null;
    const deliverAt = delayMs === null ? null : new Date(time.own.getTime() + delayMs);
    await this._store.update(tx, instance.id, transition.state, time.at, deliverAt);
    await this._store.append(tx, instance.id, transition.journal, time.at);
    if (transition.command !== null) {
      await this._enqueueCommand(tx, instance, transition.command, deliverAt, time.own);
    }
    signals.push(...transition.signals);
  }

  /** Команда исполнителю саги; `deliverAt` и `own` — по своим часам (`SagaMoment`). */
  private async _enqueueCommand(
    tx: OutboxTransaction,
    instance: Pick<SagaInstance, 'id' | 'incarnation'>,
    draft: SagaCommandDraft,
    deliverAt: Date | null,
    own: Date,
  ): Promise<void> {
    const sagaId = instance.id;
    const address = {
      sagaId,
      generation: draft.generation,
      phase: draft.phase,
      stepIndex: draft.stepIndex,
      attempt: draft.attempt,
    };
    const payload: SagaCommandPayload = {
      commandId: sagaMessageId.command({ ...address, incarnation: instance.incarnation }),
      ...address,
    };
    await this._deps.outbox.enqueue(
      tx,
      [
        {
          id: payload.commandId,
          // Отложенная команда (пауза повтора) — в своём агрегате: при FIFO по
          // агрегату (outbox `ordering: aggregate`) она, ещё не созревшая,
          // держала бы за собой команду, поставленную позже (следующий шаг после
          // позднего успеха прежней попытки, компенсации после отмены) — до
          // своего срока, у компенсации до минут. Порядок собственных команд
          // саги не нужен: их держат блокировка строки и сверка адреса.
          aggregateType: deliverAt === null ? SAGA_AGGREGATE_TYPE.command : SAGA_AGGREGATE_TYPE.deferred,
          aggregateId: deliverAt === null ? sagaId : payload.commandId,
          eventType: this.eventTypes.command,
          payload,
          ...(deliverAt === null ? {} : { deliverAt }),
        },
      ],
      own,
    );
  }

  private _emit(signals: readonly SagaSignal[]): void {
    const metrics = this._deps.metrics;
    if (metrics === undefined) {
      return;
    }
    for (const signal of signals) {
      try {
        (metrics[signal.kind] as ((event: unknown) => void) | undefined)?.call(metrics, signal.event);
      } catch (error) {
        try {
          this._logger.warn(`[pg-saga] хук метрик ${signal.kind} отказал: ${sagaErrorText(error)}`);
        } catch {
          // Логгер отказал тоже: наблюдение не вправе ломать сагу.
        }
      }
    }
  }

  /**
   * Определение, собранное мимо `defineSaga`, проверяется тем же, что и
   * построителем: иначе пустая сага или длинное имя шага всплыли бы отказом
   * сервера посреди транзакции перехода, а не на старте.
   */
  private static _checkShape(definition: SagaDefinition): void {
    sagaName('definition.name', definition.name);
    integer('definition.version', definition.version, 1);
    if (!Array.isArray(definition.steps) || definition.steps.length === 0) {
      throw new SagaConfigError('definition.steps', `у саги ${definition.name} нет шагов`);
    }
    const names = new Set<string>();
    for (const step of definition.steps) {
      const field = `шаг ${sagaValueText(step?.name)} саги ${definition.name}`;
      sagaName(`шаг саги ${definition.name}`, step?.name);
      if (names.has(step.name)) {
        // Как у построителя: шаги различаются по имени (журнал, отчёты, сверка версий).
        throw new SagaConfigError(field, 'имя шага повторяется');
      }
      names.add(step.name);
      // Всё, на чём переход иначе споткнулся бы посреди доставки (и повторял бы
      // это до DEAD, оставляя экземпляр running), — отказ на старте.
      const fn = (value: unknown): boolean => typeof value === 'function';
      const fnOrNull = (value: unknown): boolean => value === null || fn(value);
      const local = step.kind === 'local' && fn(step.execute) && step.command === null;
      const remote = step.kind === 'remote' && fn(step.command) && step.execute === null;
      if (
        !(local || remote) ||
        !fnOrNull(step.compensate) ||
        !fnOrNull(step.compensation) ||
        (local && step.compensation !== null) ||
        (remote && step.compensate !== null)
      ) {
        throw new SagaConfigError(
          field,
          'вид local с execute (и compensate) или remote с command (и compensation) — как у построителя',
        );
      }
      if (remote || step.timeoutMs !== null) {
        integer(`${field}: timeoutMs`, step.timeoutMs, 1);
      }
      for (const [name, retry] of [
        ['retry', step.retry],
        ['compensationRetry', step.compensationRetry],
      ] as const) {
        integer(`${field}: ${name}.maxAttempts`, retry?.maxAttempts, 1);
        integer(`${field}: ${name}.backoffMs`, retry?.backoffMs, 0);
        integer(`${field}: ${name}.maxBackoffMs`, retry?.maxBackoffMs, retry.backoffMs);
      }
    }
  }

  /**
   * Каждый локальный шаг обязан уложиться в потолок доставки релея вместе с
   * ожиданием строки экземпляра и ROLLBACK после отказа
   * (`timeoutMs + lockTimeoutMs + rollbackTimeoutMs`): иначе релей обрывает
   * доставку раньше записи отказа, доставка повторяется до DEAD, а экземпляр
   * замерзает. Потолки — того раннера, который их применяет
   * (`sagaRunnerLimits`); у своей обёртки — из настроек оркестратора и
   * `SAGA_DEFAULTS.rollbackTimeoutMs`. Нарушение — отказ `register`.
   */
  private _checkDelivery(definition: SagaDefinition): void {
    const ceiling = this._deliveryTimeoutMs;
    const limits = sagaRunnerLimits.get(this._transaction);
    const idle = limits?.idleInTransactionTimeoutMs;
    const lockMs = limits?.lockTimeoutMs ?? this._lockTimeoutMs;
    const rollbackMs = limits?.rollbackTimeoutMs ?? SAGA_DEFAULTS.rollbackTimeoutMs;
    for (const step of definition.steps) {
      const stepMs = step.timeoutMs ?? SAGA_DEFAULTS.localStepTimeoutMs;
      if (step.kind === 'local' && idle !== undefined && stepMs >= idle) {
        // Шаг между своими запросами — простой транзакции: сервер оборвал бы
        // законный шаг раньше его собственного потолка.
        throw new SagaConfigError(
          'timeoutMs',
          `шаг ${step.name} саги ${definition.name}@${definition.version}: timeoutMs ${String(step.timeoutMs)} мс ` +
            `не меньше потолка простоя транзакции ${idle} мс (sagaTransactionRunner idleInTransactionTimeoutMs)`,
        );
      }
      const budget = stepMs + lockMs + rollbackMs;
      if (step.kind === 'local' && budget >= ceiling) {
        throw new SagaConfigError(
          'timeoutMs',
          `шаг ${step.name} саги ${definition.name}@${definition.version}: timeoutMs ${String(step.timeoutMs)} мс ` +
            `+ ожидание строки ${lockMs} мс + ROLLBACK ${rollbackMs} мс не меньше потолка доставки релея ${ceiling} мс ` +
            '(outbox.config.deliveryTimeoutMs) — релей оборвёт доставку раньше записи отказа',
        );
      }
    }
  }

  /** SQLSTATE `code` где-то в цепочке `cause` (драйвер оборачивает ошибку сервера). */
  private static _hasCode(error: unknown, code: string): boolean {
    let current: unknown = error;
    for (let depth = 0; depth < 5 && current !== null && typeof current === 'object'; depth += 1) {
      if ((current as { code?: unknown }).code === code) {
        return true;
      }
      current = (current as { cause?: unknown }).cause;
    }
    return false;
  }

  /** statement_timeout = потолок шага до конца шага; возвращает прежнее значение. */
  private static async _limitStatements(tx: OutboxTransaction, timeoutMs: number): Promise<string> {
    const previous = await tx.execute(sql`SELECT current_setting('statement_timeout') AS value`);
    await tx.execute(sql`SELECT set_config('statement_timeout', ${`${String(timeoutMs)}ms`}, true)`);
    return String((previous.rows[0] as { value: unknown } | undefined)?.value ?? '0');
  }

  private static _stepKey(instance: SagaInstance, phase: string): string {
    return sagaStepKey(instance, phase, instance.stepIndex);
  }
}
