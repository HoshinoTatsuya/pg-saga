import { deepFreeze } from '../deep-freeze';

/**
 * Теги `Object.prototype.toString` значений, которые `JSON.stringify` молча
 * опустошает: `{}` вместо содержимого (Map, Set, WeakMap, WeakSet, Promise) или
 * без сообщения и стека (Error и его наследники — поля неперечислимые), `{}` у
 * RegExp, ArrayBuffer и WeakRef. Типизированные массивы и DataView ловит
 * `ArrayBuffer.isView` (у каждого свой тег; JSON раскладывает их в ключи
 * «0», «1»…). Тег, а
 * не `instanceof`: значение из другого realm (vm, worker) тоже узнаётся.
 */
export const SAGA_LOSSY_TAGS: readonly string[] = [
  '[object Map]',
  '[object Set]',
  '[object WeakMap]',
  '[object WeakSet]',
  '[object Promise]',
  '[object Error]',
  // RegExp, буферы и слабая ссылка — тоже `{}`.
  '[object RegExp]',
  '[object ArrayBuffer]',
  '[object SharedArrayBuffer]',
  '[object WeakRef]',
];

deepFreeze(SAGA_LOSSY_TAGS);
