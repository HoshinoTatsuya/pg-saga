import { types } from 'pg';

import { decodeTimestamp } from './decode-timestamp';

/** Парсер драйвера по умолчанию — эталон: разбор свой обязан совпадать с ним. */
const DRIVER = types.getTypeParser(types.builtins.TIMESTAMPTZ) as (text: string) => Date;

describe('decodeTimestamp: свой разбор текста timestamptz, независимый от глобального парсера pg', () => {
  it.each([
    '2026-11-01 12:00:00+00',
    '2026-11-01 12:00:00.5+00',
    '2026-11-01 12:00:00.123456+00',
    '2026-11-01 12:00:00.999999-03',
    '2026-11-01 15:30:00.25+05:30',
    '1999-12-31 23:59:59.001+14',
    '0950-03-04 01:02:03-00:25:21',
  ])('%s — как у парсера драйвера по умолчанию', (text) => {
    expect(decodeTimestamp(text).toISOString()).toBe(DRIVER(text).toISOString());
  });

  it('Date — как есть; объект с toJSON (обёртка luxon/dayjs) — по ISO-строке', () => {
    const date = new Date('2026-11-01T12:00:00.000Z');
    expect(decodeTimestamp(date)).toBe(date);
    expect(decodeTimestamp({ toJSON: () => '2026-11-01T15:00:00.000+03:00' }).toISOString()).toBe(
      '2026-11-01T12:00:00.000Z',
    );
  });

  it('не время — отказ с причиной (переопределённый парсер), а не голое «не время»', () => {
    for (const bad of ['infinity', 'вчера', 42, null, new Date(Number.NaN)]) {
      expect(() => decodeTimestamp(bad)).toThrow(/pg-saga: время из базы/);
    }
    expect(() => decodeTimestamp('позавчера')).toThrow(/парсер типа 1184/);
  });
});
