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

  const decoder = iconv.decodeStream(detected);
  decoder._writableState.highWaterMark = chunkSize;
  decoder._readableState.highWaterMark = chunkSize;

  return {
    encoding: detected,
    steps: [
      createReadStream(filepath, { highWaterMark: chunkSize }),
      decoder,
      transformerStream,
    ],
  };
}

/**
 * 1. PURE REDACTION TRANSFORMER
 * Pure chunk processor. Emits structured data chunks downstream.
 */
export function createRedactionTransformer({ keywords, contextLen }) {
  const sortedKeywords = [...keywords].sort((a, b) => b.length - a.length);
  const escapedKeywords = sortedKeywords.map((k) =>
    k.replace(/[-\/\\^$*+?.()|[\]{}]/g, '\\$&')
  );
  const masterRegex = new RegExp(`(${escapedKeywords.join('|')})`, 'gi');

  const maxKeywordLen = Math.max(...sortedKeywords.map((k) => k.length), 0);
  const tailOverlapSize = maxKeywordLen + contextLen;

  let carryOverTail = '';
  let originalCharOffset = 0;
  let redactedCharOffset = 0;

  return new Transform({
    writableObjectMode: false,
    readableObjectMode: true, // Emits objects containing text & found matches

    transform(chunk, encoding, callback) {
      const currentSegment = carryOverTail + chunk.toString();
      let match;
      let lastIndex = 0;
      let cleanedText = '';
      const matchesFound = [];

      masterRegex.lastIndex = 0;
      const safeScanBoundary = currentSegment.length - tailOverlapSize;

      while ((match = masterRegex.exec(currentSegment)) !== null) {
        const matchIndex = match.index;
        if (matchIndex > safeScanBoundary && safeScanBoundary > 0) {
          break;
        }

        const matchedWord = match[0];
        const precedingFragment = currentSegment.substring(
          lastIndex,
          matchIndex
        );
        cleanedText += precedingFragment;

        const absoluteOriginalPosition =
          originalCharOffset + precedingFragment.length;
        const absoluteRedactedPosition =
          redactedCharOffset + cleanedText.length;

        matchesFound.push({
          matchedWord,
          absoluteOriginalPosition,
          absoluteRedactedPosition,
          contextBefore: currentSegment.substring(
            Math.max(0, matchIndex - contextLen),
            matchIndex
          ),
          contextAfter: currentSegment.substring(
            matchIndex + matchedWord.length,
            matchIndex + matchedWord.length + contextLen
          ),
        });

        cleanedText += 'XXXX';
        originalCharOffset += precedingFragment.length + matchedWord.length;
        lastIndex = masterRegex.lastIndex;
      }

      const staticFragment = currentSegment.substring(lastIndex);
      if (staticFragment.length > tailOverlapSize) {
        const safePushLen = staticFragment.length - tailOverlapSize;
        const pushText = staticFragment.substring(0, safePushLen);
        cleanedText += pushText;

        this.push({ text: cleanedText, matches: matchesFound });
        originalCharOffset += pushText.length;
        redactedCharOffset += cleanedText.length;
        carryOverTail = staticFragment.substring(safePushLen);
      } else {
        this.push({ text: cleanedText, matches: matchesFound });
        redactedCharOffset += cleanedText.length;
        carryOverTail = staticFragment;
      }
      callback();
    },

    flush(callback) {
      if (carryOverTail) {
        this.push({ text: carryOverTail, matches: [] });
      }
      callback();
    },
  });
}

/**
 * 2. TRANSACTIONAL DATABASE BATCH WRITER
 * Flushes row packets directly inside a safe, high-speed transaction block.
 * Memory layout is capped to preserve memory headroom.
 */
export function createDatabaseBatchWriterTransformer({
  db,
  insertStmt,
  filename,
  originalMd5,
  encryptEnabled,
  masterKeyBuffer,
  batchSize = 100000, // Safe memory barrier limit
}) {
  let recordBatch = [];

  // Create a fast, synchronous transaction runner right inside the transformer setup
  const runTransaction = db.transaction((rows) => {
    for (const row of rows) {
      insertStmt.run(row);
    }
  });

  const flushBatch = () => {
    if (recordBatch.length === 0) return;

    const databaseRows = [];
    for (const match of recordBatch) {
      let dbName = filename;
      let dbWord = match.matchedWord;
      let dbBefore = match.contextBefore;
      let dbAfter = match.contextAfter;
      let encryptFlagValue = 0;

      if (encryptEnabled) {
        dbName = encryptText(filename, masterKeyBuffer);
        dbWord = encryptText(match.matchedWord, masterKeyBuffer);
        dbBefore = encryptText(match.contextBefore, masterKeyBuffer);
        dbAfter = encryptText(match.contextAfter, masterKeyBuffer);
        encryptFlagValue = 1;
      }

      databaseRows.push({
        documentName: dbName,
        originalMd5,
        redactedWord: dbWord,
        charPosition: match.absoluteOriginalPosition,
        redactedPosition: match.absoluteRedactedPosition,
        contextBefore: dbBefore,
        contextAfter: dbAfter,
        isEncrypted: encryptFlagValue,
      });
    }

    recordBatch = []; // Free array references early to enable garbage collection execution passes
    runTransaction(databaseRows);
  };

  return new Transform({
    writableObjectMode: true,
    readableObjectMode: true,

    transform(chunk, encoding, callback) {
      if (chunk.matches?.length > 0) {
        recordBatch.push(...chunk.matches);
        if (recordBatch.length >= batchSize) {
          flushBatch();
        }
      }
      this.push(chunk); // Pass object cleanly to next filter
      callback();
    },

    flush(callback) {
      flushBatch();
      callback();
    },
  });
}

/**
 * 3. TEXT OUTPUT EXTRACTION FILTER
 */
export function createTextExtractionTransformer() {
  return new Transform({
    writableObjectMode: true,
    readableObjectMode: false,

    transform(chunk, encoding, callback) {
      if (chunk.text) {
        this.push(chunk.text);
      }
      callback();
    },
  });
}

/**
 * 4. UNREDACTION TRANSFORMER
 */
export function createUnredactionTransformer(unredactMap) {
  let mapTrackers = [...unredactMap].sort(
    (a, b) => a.redactedPosition - b.redactedPosition
  );
  let textBuffer = '';
  let globalCharOffset = 0;
  const overlapWindowSize = 100;

  return new Transform({
    writableObjectMode: false,
    readableObjectMode: false,

    transform(chunk, encoding, callback) {
      textBuffer += chunk.toString();

      while (mapTrackers.length > 0) {
        const nextTarget = mapTrackers[0];
        const localIndex = nextTarget.redactedPosition - globalCharOffset;

        if (localIndex + 4 > textBuffer.length) {
          break;
        }

        mapTrackers.shift();

        if (textBuffer.substring(localIndex, localIndex + 4) === 'XXXX') {
          textBuffer =
            textBuffer.substring(0, localIndex) +
            nextTarget.originalWord +
            textBuffer.substring(localIndex + 4);

          globalCharOffset -= nextTarget.originalWord.length - 4;
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
        const localIndex = nextTarget.redactedPosition - globalCharOffset;
        if (localIndex >= 0 && localIndex + 4 <= textBuffer.length) {
          if (textBuffer.substring(localIndex, localIndex + 4) === 'XXXX') {
            textBuffer =
              textBuffer.substring(0, localIndex) +
              nextTarget.originalWord +
              textBuffer.substring(localIndex + 4);
            globalCharOffset -= nextTarget.originalWord.length - 4;
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
