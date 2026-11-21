import type { SagaStatus } from '../types/saga-status.type';

/** Ручное действие над экземпляром не в том состоянии (повтор не-`stuck` и т. п.). */
export class SagaStateError extends Error {
  public override readonly name = 'SagaStateError';

  public constructor(
    public readonly sagaId: string,
    public readonly status: SagaStatus | null,
    action: string,
  ) {
    super(
      status === null
        ? `pg-saga: ${action}: экземпляра ${sagaId} нет`
        : `pg-saga: ${action}: экземпляр ${sagaId} в состоянии ${status}, нужно stuck или blocked (или замёрзший с порогом простоя)`,
    );
  }
}
