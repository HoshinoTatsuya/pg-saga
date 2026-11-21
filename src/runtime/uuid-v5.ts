import { createHash } from 'node:crypto';

/** uuid v5 (RFC 9562, SHA-1): пространство имён (uuid) + имя → uuid. */
export const uuidV5 = (namespace: string, name: string): string => {
  const space = Buffer.from(namespace.replaceAll('-', ''), 'hex');
  if (space.length !== 16) {
    throw new Error(`uuidV5: пространство имён — не uuid: ${namespace}`);
  }
  const bytes = createHash('sha1').update(space).update(name, 'utf8').digest().subarray(0, 16);
  bytes[6] = ((bytes[6] ?? 0) & 0x0f) | 0x50;
  bytes[8] = ((bytes[8] ?? 0) & 0x3f) | 0x80;
  const hex = bytes.toString('hex');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
};
