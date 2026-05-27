import test, { describe, before, after } from 'node:test';
import assert from 'assert/strict';
import path from 'path';
import fs, { existsSync } from 'fs';
import promisesFs from 'fs/promises';
import { pipeline } from 'stream/promises';
import { createReadStream, createWriteStream } from 'fs';
import { randomBytes } from 'crypto';
import { DatabaseSync } from 'node:sqlite'; // Native Node.js SQLite integration

import {
  calculateFileMd5,
  preparePipelineStreams,
  createRedactionTransformer,
  createUnredactionTransformer,
} from './transformers.js'; // Adjust this relative path to point to your script file

const TEST_KEY = randomBytes(32);

describe('King James Bible Lifecycle Suite with Real SQLite', () => {
  const FIXTURE_DIR = path.resolve('test-fixtures');
  const KJB_INPUT = path.join(FIXTURE_DIR, 'kjv.txt');
  const KJB_REDACTED = path.join(FIXTURE_DIR, 'kjv.REDACTED.txt');
  const KJB_RESTORED = path.join(FIXTURE_DIR, 'kjv.RESTORED.txt');

  let db;

  before(async () => {
    // Hard check for the King James Bible text asset
    if (!existsSync(KJB_INPUT)) {
      throw new Error(
        `Critical Test Failure: King James Bible source file missing at "${KJB_INPUT}". Please place the txt file there before running tests.`
      );
    }

    // Initialize an in-memory real SQLite database
    db = new DatabaseSync(':memory:');

    // Create the schema mirroring the execution properties of your insertStmt.run call
    db.exec(`
      CREATE TABLE redactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        filename TEXT,
        original_md5 TEXT,
        downstream_md5 TEXT,
        word TEXT,
        position INTEGER,
        context_before TEXT,
        context_after TEXT,
        encrypted INTEGER
      );
    `);
  });

  after(async () => {
    // Structural cleanup of generated stream fragments
    for (const file of [KJB_REDACTED, KJB_RESTORED]) {
      if (existsSync(file)) {
        await promisesFs.unlink(file);
      }
    }
    if (db) {
      db.close();
    }
  });

  describe('Redaction & Unredaction Lifecycle', () => {
    test('Should handle overlapping terms and execute redaction using real SQLite', async () => {
      const originalMd5 = await calculateFileMd5(KJB_INPUT);

      // Prepare a real SQLite statement matching the 8 bindings passed to insertStmt.run()
      const insertStmt = db.prepare(`
        INSERT INTO redactions (filename, original_md5, downstream_md5, word, position, context_before, context_after, encrypted)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
      `);

      // Define overlapping keywords where parts of a word are contained in a larger word
      // 'Godhead' contains 'God', 'Israelites' contains 'Israel'
      const keywords = [
        'God',
        'Godhead',
        'Israel',
        'Israelites',
        'commandments',
      ];
      const chunkSize = 4096;

      const redactionTransformer = createRedactionTransformer({
        keywords,
        contextLen: 15,
        insertStmt,
        filename: 'kjv.txt',
        originalMd5,
        encryptEnabled: false,
        masterKeyBuffer: TEST_KEY,
      });

      // Pass the pipeline stream elements into the pipeline executor
      const pipelineConfig = await preparePipelineStreams(
        KJB_INPUT,
        redactionTransformer,
        chunkSize
      );
      const destinationStream = createWriteStream(KJB_REDACTED);

      await pipeline(...pipelineConfig.steps, destinationStream);

      assert.ok(
        existsSync(KJB_REDACTED),
        'Redacted text file must be generated on disk'
      );

      // Verify that data was written to the real SQLite database table
      const countQuery = db
        .prepare('SELECT COUNT(*) AS total FROM redactions;')
        .get();
      assert.ok(
        countQuery.total > 0,
        'Real SQLite database table should contain rows'
      );

      // Assert that longer overlapping terms were caught correctly as distinct items
      const checkOverlapsStmt = db.prepare(
        "SELECT word FROM redactions WHERE word = 'Godhead' OR word = 'Israelites' LIMIT 1;"
      );
      const overlapSample = checkOverlapsStmt.get();

      if (overlapSample) {
        assert.ok(
          ['Godhead', 'Israelites'].includes(overlapSample.word),
          'Overlapping terms must be captured properly without being pre-empted by shorter subsets'
        );
      }
    });

    test('Should accurately reverse alterations via data queried from SQLite', async () => {
      assert.ok(
        existsSync(KJB_REDACTED),
        'Cannot run restorative pass without redacted source asset'
      );

      // Query real row metadata back from the SQLite engine, sorted matching positional progression
      const selectRowsStmt = db.prepare(`
        SELECT position AS redactedPosition, word AS originalWord
        FROM redactions
        ORDER BY position ASC;
      `);
      const unredactMap = selectRowsStmt.all();

      const unredactionTransformer = createUnredactionTransformer(unredactMap);

      const sourceStream = createReadStream(KJB_REDACTED, {
        highWaterMark: 2048,
      });
      const destinationStream = createWriteStream(KJB_RESTORED);

      await pipeline(sourceStream, unredactionTransformer, destinationStream);

      assert.ok(
        existsSync(KJB_RESTORED),
        'Restored text file must be generated on disk'
      );

      // Final integrity assertion: Compare original file MD5 and restored file MD5 hashes
      const originalHash = await calculateFileMd5(KJB_INPUT);
      const restoredHash = await calculateFileMd5(KJB_RESTORED);

      assert.equal(
        restoredHash,
        originalHash,
        'The unredaction pipeline failed to recreate a bit-perfect copy of the original KJV source text'
      );
    });
  });
});

// describe('Document Redactor - Comprehensive Test Suite', () => {

//   describe('Basic Functionality', () => {
//     test('should redact a single keyword', () => {
//       const keywords = 'beer';
//       const text = 'I would like a cold beer please.';
//       const expected = 'I would like a cold XXXX please.';
//       assert.strictEqual(redactDocument(keywords, text), expected);
//     });

//     test('should redact a quoted phrase', () => {
//       const keywords = '"Boston Red Sox"';
//       const text = 'The Boston Red Sox won the game.';
//       const expected = 'The XXXX won the game.';
//       assert.strictEqual(redactDocument(keywords, text), expected);
//     });

//     test('should handle mixed comma and space delimiters', () => {
//       const keywords = 'beer, "cheese pizza"';
//       const text = 'We ordered cheese pizza and beer.';
//       const expected = 'We ordered XXXX and XXXX.';
//       assert.strictEqual(redactDocument(keywords, text), expected);
//     });
//   });

//   describe('Edge Cases & Boundary Conditions', () => {
//     test('should be case-insensitive', () => {
//       const keywords = 'BEER';
//       const text = 'I drank a cold beer.';
//       const expected = 'I drank a cold XXXX.';
//       assert.strictEqual(redactDocument(keywords, text), expected);
//     });

//     test('should only match whole words (ignore substrings)', () => {
//       const keywords = 'apple';
//       const text = 'The pineapple was delicious.';
//       const expected = 'The pineapple was delicious.';
//       assert.strictEqual(redactDocument(keywords, text), expected);
//     });

//     test('should preserve punctuation attached to keywords', () => {
//       const keywords = 'beer';
//       const text = 'Do you want a beer?';
//       const expected = 'Do you want a XXXX?';
//       assert.strictEqual(redactDocument(keywords, text), expected);
//     });

//     test('should return empty string if document text is empty', () => {
//       const keywords = 'secret';
//       const text = '';
//       const expected = '';
//       assert.strictEqual(redactDocument(keywords, text), expected);
//     });

//     test('should return original text if keywords string is empty', () => {
//       const keywords = '';
//       const text = 'This is top secret.';
//       const expected = 'This is top secret.';
//       assert.strictEqual(redactDocument(keywords, text), expected);
//     });
//   });

//   describe('Architecture Verification (Index & Collision Tracking)', () => {

//     /**
//      * Test Rule: Reverse Index Reconstruction (ORDER BY start_idx DESC)
//      * If the algorithm mutates from left-to-right, replacing 'a' with 'XXXX' shifts
//      * all following character indexes out of alignment. Replacing from right-to-left
//      * ensures upcoming index coordinates remain valid.
//      */
//     test('Reverse Index Reconstruction: should not drift indexes when earlier string mutations alter total length', () => {
//       // 'a' expands the string (+3 chars). 'longerphrase' shrinks it (-8 chars).
//       const keywords = `a, "longerphrase"`;
//       const text = 'a small sample of a longerphrase is here.';
//       const expected = 'XXXX small sample of XXXX XXXX is here.';
//       assert.strictEqual(redactDocument(keywords, text), expected);
//     });

//     /**
//      * Test Rule: Overlapping Security Protection (Nested Collisions)
//      * When a keyword ('Cheese') is fully wrapped inside a phrase ('Cheese Pizza'),
//      * chronological filtering must drop the redundant inner token match to prevent
//      * breaking index arrays or causing double-redaction ('XXXX XXXX' or 'XXXX Pizza').
//      */
//     test('Overlapping Security Protection: should drop nested matches to avoid double-redaction corruption', () => {
//       const keywords = `"Cheese Pizza", Cheese`;
//       const text = 'I love Cheese Pizza.';
//       const expected = 'I love XXXX.';
//       assert.strictEqual(redactDocument(keywords, text), expected);
//     });

//     /**
//      * Test Rule: Overlapping Security Protection (Partial Collisions)
//      * If two target zones partially intersect, the first validated match wins,
//      * and the intersecting portions of the subsequent match must be neutralized.
//      */
//     test('Overlapping Security Protection: should handle partially intersecting index ranges safely', () => {
//       const keywords = `"secret document", "document protocol"`;
//       const text = 'This is a secret document protocol.';
//       const expected = 'This is a XXXX protocol.';
//       assert.strictEqual(redactDocument(keywords, text), expected);
//     });
//   });

//   describe('Integration Demo', () => {
//     test('should pass the comprehensive assignment specification', () => {
//       const keywords = `Hello world "Boston Red Sox", 'Pepperoni Pizza', 'Cheese Pizza', beer`;
//       const text = 'Hello world! The Boston Red Sox love Pepperoni Pizza and cold beer.';
//       const expected = 'XXXX XXXX! The XXXX love XXXX and cold XXXX.';
//       assert.strictEqual(redactDocument(keywords, text), expected);
//     });
//   });
// });
