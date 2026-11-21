/**
 * Значение → строка для текста отказа, и тогда, когда `String()` бросает:
 * объект без прототипа (`Object.create(null)`), сломанный `toString`, Proxy с
 * бросающими ловушками. Иначе отказ с причиной превращался бы в TypeError
 * «Cannot convert object to primitive value» — и там, где текст пишется в
 * журнал отказа шага, отказ не записывался бы вовсе: доставка повторялась бы
 * до DEAD, а экземпляр оставался running.
 */
export const sagaValueText = (value: unknown): string => {
  try {
    return String(value);
  } catch {
    try {
      return Object.prototype.toString.call(value);
    } catch {
      return '[значение не преобразуется в текст]';
    }
  }
};
