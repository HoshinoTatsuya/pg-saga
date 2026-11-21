import { join } from 'node:path';

import ts from 'typescript';

/**
 * Типы контекста — на реалистичном использовании (`testing/saga-consumer-types.ts`)
 * под настройками пакета (`strict`, `exactOptionalPropertyTypes`): ни одной
 * ошибки, в том числе «неиспользованной» `@ts-expect-error` (тип стал `any`).
 */
describe('типы контекста саги у потребителя', () => {
  it('проба компилируется без ошибок под tsc --strict', () => {
    const root = join(__dirname, '..', '..');
    const config = ts.getParsedCommandLineOfConfigFile(
      join(root, 'tsconfig.json'),
      {},
      {
        ...ts.sys,
        onUnRecoverableConfigFileDiagnostic: (diagnostic) => {
          throw new Error(ts.flattenDiagnosticMessageText(diagnostic.messageText, '\n'));
        },
      },
    );
    if (config === undefined) {
      throw new Error('tsconfig.json не прочитан');
    }
    const fixture = join(root, 'src', 'testing', 'saga-consumer-types.ts');
    const program = ts.createProgram([fixture], { ...config.options, noEmit: true });
    const diagnostics = ts.getPreEmitDiagnostics(program).map((diagnostic) => {
      const where =
        diagnostic.file === undefined || diagnostic.start === undefined
          ? ''
          : `${diagnostic.file.fileName.slice(root.length + 1)}:${
              diagnostic.file.getLineAndCharacterOfPosition(diagnostic.start).line + 1
            } `;
      return `${where}${ts.flattenDiagnosticMessageText(diagnostic.messageText, ' ')}`;
    });
    // Выборка не выродилась: проба на месте и в программе.
    expect(program.getSourceFile(fixture)?.statements.length ?? 0).toBeGreaterThan(10);
    expect(diagnostics).toEqual([]);
  }, 60_000);
});
