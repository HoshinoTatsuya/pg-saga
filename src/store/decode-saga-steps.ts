/**
 * `steps` — массив имён (экземпляр версии 0.1.0: воплощения нет, id сообщений
 * и ключ шага — её формата) или `{ names, stepKeyFormat: 2, incarnation }`.
 * Строка формата 2 без воплощения (сборка до соли воплощения) получает
 * воплощение по моменту создания — так были выведены её id и ключи.
 */
export const decodeSagaSteps = (
  raw: unknown,
  createdAt: Date,
): { names: string[]; incarnation: number | null } => {
  if (Array.isArray(raw)) {
    return { names: raw as string[], incarnation: null };
  }
  const record = raw as { names?: unknown; stepKeyFormat?: unknown; incarnation?: unknown };
  const names = Array.isArray(record?.names) ? (record.names as string[]) : [];
  if (record?.stepKeyFormat !== 2) {
    return { names, incarnation: null };
  }
  const { incarnation } = record;
  return {
    names,
    incarnation:
      typeof incarnation === 'number' && Number.isSafeInteger(incarnation) && incarnation >= 0
        ? incarnation
        : createdAt.getTime(),
  };
};
