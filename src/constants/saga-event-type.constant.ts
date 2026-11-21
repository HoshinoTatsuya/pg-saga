import { deepFreeze } from '../deep-freeze';

/**
 * Типы событий outbox, которыми сага двигает сама себя (по умолчанию):
 * команда шага, таймер шага и ответ участника, записанный в тот же outbox.
 * Свои — опцией `eventTypes` оркестратора (две независимые группы саг в одной
 * базе — разные типы и разный `consumer` inbox).
 */
export const SAGA_EVENT_TYPE = {
  command: 'saga.command',
  timer: 'saga.timer',
  reply: 'saga.reply',
} as const;

deepFreeze(SAGA_EVENT_TYPE);
