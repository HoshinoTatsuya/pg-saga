import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

/**
 * Карта exports — граница API: файл dist вне неё недоступен потребителю.
 * Вход корня ведёт в существующий модуль исходников, раздельно для import
 * (`.d.mts`) и require (`.d.ts`), и совпадает с main/types для тех, кто читает
 * только их. Подпутей нет — нет и typesVersions; bin — существующий модуль.
 */
const ROOT = join(__dirname, '..');
const manifest = JSON.parse(readFileSync(join(ROOT, 'package.json'), 'utf8')) as {
  main: string;
  types: string;
  exports: Record<string, unknown>;
  typesVersions?: unknown;
  bin: Record<string, string>;
};

interface Conditions {
  types: string;
  default: string;
}

describe('карта exports пакета', () => {
  it('входы — ровно корень и package.json', () => {
    expect(Object.keys(manifest.exports).sort()).toEqual(['.', './package.json']);
    expect(manifest.typesVersions).toBeUndefined();
  });

  it('корень: import и require ведут в существующий модуль; require совпадает с main/types', () => {
    const root = manifest.exports['.'] as { import: Conditions; require: Conditions };
    for (const conditions of [root.import, root.require]) {
      expect(conditions.types.replace(/\.d\.m?ts$/, '')).toBe(conditions.default.replace(/\.js$/, ''));
      expect(
        existsSync(join(ROOT, conditions.default.replace(/^\.\/dist\//, 'src/').replace(/\.js$/, '.ts'))),
      ).toBe(true);
    }
    expect(root.import.types.endsWith('.d.mts')).toBe(true);
    expect(root.require).toEqual({ types: manifest.types, default: manifest.main });
  });

  it('bin ведёт в существующий модуль исходников', () => {
    const targets = Object.values(manifest.bin);
    expect(targets).toHaveLength(1);
    for (const target of targets) {
      expect(existsSync(join(ROOT, target.replace(/^\.\/dist\//, 'src/').replace(/\.js$/, '.ts')))).toBe(
        true,
      );
    }
  });
});
