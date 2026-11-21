import { sagaValueText } from '../runtime/saga-value-text';

/**
 * `timestamptz` из ответа → Date. drizzle над node-postgres отдаёт время
 * СТРОКОЙ (свои парсеры типов на соединении), голый `pg` — тем, что вернул
 * ГЛОБАЛЬНЫЙ парсер типа 1184. Текст разбирается здесь, а не глобальным
 * парсером: приложение вправе его переопределить (строки, luxon, dayjs), и
 * тогда чтение экземпляра падало бы на каждом сообщении саги. Формат — вывод
 * Postgres при `DateStyle = ISO` (`2026-11-01 12:00:00.123456+03:30`); объект
 * с `toJSON()` (обёртка парсера приложения) — по его ISO-строке.
 */
export const decodeTimestamp = (value: unknown): Date => {
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) {
      throw new Error('pg-saga: время из базы — недопустимая дата (Invalid Date)');
    }
    return value;
  }
  const text =
    typeof value === 'string'
      ? value
      : typeof (value as { toJSON?: unknown } | null)?.toJSON === 'function'
        ? (value as { toJSON(): unknown }).toJSON()
        : undefined;
  const match =
    typeof text === 'string'
      ? /^(\d{4,})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?(?:(Z)|([+-])(\d{2})(?::?(\d{2}))?(?::?(\d{2}))?)$/.exec(
          text,
        )
      : null;
  if (match === null) {
    throw new Error(
      `pg-saga: время из базы не разобрано: ${sagaValueText(text ?? value)} — ожидается Date или текст timestamptz ` +
        'Postgres (DateStyle ISO); парсер типа 1184 в pg переопределён так, что не отдаёт ни того, ни другого?',
    );
  }
  const [, year, month, day, hour, minute, second, fraction, , sign, offsetH, offsetM, offsetS] = match;
  const date = new Date(0);
  date.setUTCFullYear(Number(year), Number(month) - 1, Number(day));
  date.setUTCHours(
    Number(hour),
    Number(minute),
    Number(second),
    Number((fraction ?? '').padEnd(3, '0').slice(0, 3)),
  );
  const offsetMs = (Number(offsetH ?? 0) * 3_600 + Number(offsetM ?? 0) * 60 + Number(offsetS ?? 0)) * 1_000;
  return new Date(date.getTime() - (sign === '-' ? -offsetMs : offsetMs));
};
