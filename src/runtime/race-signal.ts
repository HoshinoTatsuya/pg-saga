/**
 * Промис работы против сигнала: сигнал сработал раньше — отказ с его причиной.
 * Брошенная работа продолжает идти, но её исход больше никого не ждёт (отказ
 * обработан здесь же и необработанным не становится).
 */
export const raceSignal = <T>(work: Promise<T>, signal: AbortSignal): Promise<T> => {
  if (signal.aborted) {
    work.catch(() => undefined);
    return Promise.reject(signal.reason);
  }
  return new Promise<T>((resolve, reject) => {
    const onAbort = (): void => reject(signal.reason);
    signal.addEventListener('abort', onAbort, { once: true });
    work.then(
      (value) => {
        signal.removeEventListener('abort', onAbort);
        resolve(value);
      },
      (error: unknown) => {
        signal.removeEventListener('abort', onAbort);
        reject(error);
      },
    );
  });
};
