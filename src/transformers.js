import { Transform } from 'stream';
import {
  createHash,
  createCipheriv,
  createDecipheriv,
  randomBytes,
} from 'crypto';
import { createReadStream } from 'node:fs';
import chardet from 'chardet';
import iconv from 'iconv-lite';

const NATIVE_ENCODINGS_MAP = {
  utf8: 'utf8',
  utf: 'utf8',
  ascii: 'ascii',
  usascii: 'ascii',
  utf16le: 'utf16le',
  ucs2: 'utf16le',
  latin1: 'latin1',
  iso88591: 'latin1',
};

export function parseKeywords(str) {
  if (!str) return [];
  const regex = /"[^"\\]*(?:\\.[^"\\]*)*"|'[^'\\]*(?:\\.[^'\\]*)*'|[^,\s]+/g;
  const matches = str.match(regex);
  return matches
    ? matches.map((k) => k.replace(/^['"]|['"]$/g, '').trim()).filter(Boolean)
    : [];
}

export function getNativeNodeEncoding(detected) {
  if (!detected) return null;
  const cleanKey = detected.toLowerCase().replace(/[^a-z0-9]/g, '');
  return NATIVE_ENCODINGS_MAP[cleanKey] || null;
}

export function encryptText(text, keyBuffer) {
  if (!text) return '';
  const iv = randomBytes(12);
  const cipher = createCipheriv('aes-256-gcm', keyBuffer, iv);
  const encrypted = Buffer.concat([
    cipher.update(text, 'utf8'),
    cipher.final(),
  ]);
  return `${iv.toString('hex')}:${cipher.getAuthTag().toString('hex')}:${encrypted.toString('hex')}`;
}

export function decryptText(encryptedTarget, keyBuffer) {
  if (!encryptedTarget) return '';
  const [ivHex, authTagHex, encryptedText] = encryptedTarget.split(':');
  const decipher = createDecipheriv(
    'aes-256-gcm',
    keyBuffer,
    Buffer.from(ivHex, 'hex')
  );
  decipher.setAuthTag(Buffer.from(authTagHex, 'hex'));
  return Buffer.concat([
    decipher.update(Buffer.from(encryptedText, 'hex')),
    decipher.final(),
  ]).toString('utf8');
}

export async function calculateFileMd5(filepath) {
  const hash = createHash('md5');
  for await (const chunk of createReadStream(filepath)) {
    hash.update(chunk);
  }
  return hash.digest('hex');
}

export async function preparePipelineStreams(
  filepath,
  transformerStream,
  chunkSize
) {
  const detected = await chardet.detectFile(filepath, { sampleSize: 10000 });
  if (!detected)
    throw new Error('Could not identify character encoding layout.');

  const native = getNativeNodeEncoding(detected);
  if (native) {
    return {
      encoding: detected,
      steps: [
        createReadStream(filepath, {
          encoding: native,
          highWaterMark: chunkSize,
        }),
        transformerStream,
      ],
    };
  }

  if (!iconv.encodingExists(detected))
    throw new Error(`Encoding "${detected}" is unrecognized.`);
  return {
    encoding: detected,
    steps: [
      createReadStream(filepath, { highWaterMark: chunkSize }),
      iconv.decodeStream(detected),
      transformerStream,
    ],
  };
}

export function createRedactionTransformer({
  keywords,
  contextLen,
  insertStmt,
  filename,
  originalMd5,
  encryptEnabled,
  masterKeyBuffer,
}) {
  const sortedKeywords = [...keywords].sort((a, b) => b.length - a.length);
  const escapedKeywords = sortedKeywords.map((k) =>
    k.replace(/[-\/\\^$*+?.()|[\]{}]/g, '\\$&')
  );
  const masterRegex = new RegExp(`(${escapedKeywords.join('|')})`, 'gi');

  const maxKeywordLen = Math.max(...sortedKeywords.map((k) => k.length), 0);
  const overlapWindowSize = (maxKeywordLen + contextLen) * 2;

  let textBuffer = '';
  let globalCharOffset = 0;

  return new Transform({
    writableObjectMode: false,
    readableObjectMode: false,

    transform(chunk, encoding, callback) {
      textBuffer += chunk.toString();

      let match;
      let lastIndex = 0;
      let cleanedText = '';

      masterRegex.lastIndex = 0;

      while ((match = masterRegex.exec(textBuffer)) !== null) {
        const matchIndex = match.index;
        const matchedWord = match[0];

        cleanedText += textBuffer.substring(lastIndex, matchIndex);

        const absoluteCharPosition = globalCharOffset + cleanedText.length;

        const contextBefore = textBuffer.substring(
          Math.max(0, matchIndex - contextLen),
          matchIndex
        );
        const contextAfter = textBuffer.substring(
          matchIndex + matchedWord.length,
          matchIndex + matchedWord.length + contextLen
        );

        let dbName = filename;
        let dbWord = matchedWord;
        let dbBefore = contextBefore;
        let dbAfter = contextAfter;
        let encryptFlagValue = 0;

        if (encryptEnabled) {
          dbName = encryptText(filename, masterKeyBuffer);
          dbWord = encryptText(matchedWord, masterKeyBuffer);
          dbBefore = encryptText(contextBefore, masterKeyBuffer);
          dbAfter = encryptText(contextAfter, masterKeyBuffer);
          encryptFlagValue = 1;
        }

        // Write row directly into sqlite file storage
        insertStmt.run(
          dbName,
          originalMd5,
          null, // updated downstream in index.js post pipeline completion pass
          dbWord,
          absoluteCharPosition,
          dbBefore,
          dbAfter,
          encryptFlagValue
        );

        cleanedText += 'XXXX';
        lastIndex = masterRegex.lastIndex;
      }

      cleanedText += textBuffer.substring(lastIndex);
      const safePushLen = Math.max(0, cleanedText.length - overlapWindowSize);

      if (safePushLen > 0) {
        this.push(cleanedText.substring(0, safePushLen));
        globalCharOffset += safePushLen;
        textBuffer = cleanedText.substring(safePushLen);
      } else {
        textBuffer = cleanedText;
      }
      callback();
    },

    flush(callback) {
      if (textBuffer) {
        this.push(textBuffer);
      }
      callback();
    },
  });
}

export function createUnredactionTransformer(unredactMap) {
  let mapTrackers = [...unredactMap].sort(
    (a, b) => a.redactedPosition - b.redactedPosition
  );
  let textBuffer = '';
  let globalCharOffset = 0;

  let totalDelta = 0;

  const overlapWindowSize = 100;

  return new Transform({
    writableObjectMode: false,
    readableObjectMode: false,

    transform(chunk, encoding, callback) {
      textBuffer += chunk.toString();

      while (mapTrackers.length > 0) {
        const nextTarget = mapTrackers[0];

        const localIndex =
          nextTarget.redactedPosition + totalDelta - globalCharOffset;

        if (localIndex < 0 || localIndex + 4 > textBuffer.length) {
          break;
        }

        mapTrackers.shift();

        if (textBuffer.substring(localIndex, localIndex + 4) === 'XXXX') {
          const original = nextTarget.originalWord;
          textBuffer =
            textBuffer.substring(0, localIndex) +
            original +
            textBuffer.substring(localIndex + 4);
          totalDelta += original.length - 4;
        }
      }

      const safePushLen = Math.max(0, textBuffer.length - overlapWindowSize);
      if (safePushLen > 0) {
        this.push(textBuffer.substring(0, safePushLen));
        globalCharOffset += safePushLen;
        textBuffer = textBuffer.substring(safePushLen);
      }

      callback();
    },

    flush(callback) {
      while (mapTrackers.length > 0) {
        const nextTarget = mapTrackers.shift();
        const localIndex =
          nextTarget.redactedPosition + totalDelta - globalCharOffset;

        if (localIndex >= 0 && localIndex + 4 <= textBuffer.length) {
          if (textBuffer.substring(localIndex, localIndex + 4) === 'XXXX') {
            const original = nextTarget.originalWord;
            textBuffer =
              textBuffer.substring(0, localIndex) +
              original +
              textBuffer.substring(localIndex + 4);
            totalDelta += original.length - 4;
          }
        }
      }

      if (textBuffer) {
        this.push(textBuffer);
      }
      callback();
    },
  });
}
