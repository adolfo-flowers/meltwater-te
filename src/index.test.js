import { test, describe, before, after } from 'node:test';
import assert from 'node:assert';
import fs from 'node:fs/promises';
import { existsSync } from 'node:fs';
import path from 'path';

// Import CLI runners directly to bypass Commander's process.exit constraints
import { runRedactionPhase, runUnredactionPhase } from './index.js';
import { initDatabase } from './lib.js';

describe('Declassify Core Pipeline - King James Bible Structural Tests', () => {
  const FIXTURE_DIR = path.resolve('test-fixtures');
  const KJB_INPUT = path.join(FIXTURE_DIR, 'kjv.txt');
  const KJB_REDACTED = path.join(FIXTURE_DIR, 'kjv.REDACTED.txt');
  const KJB_RESTORED = path.join(FIXTURE_DIR, 'kjv.RESTORED.txt');

  let db;

  before(async () => {
    // Hard check for the King James Bible text asset
    if (!existsSync(KJB_INPUT)) {
      throw new Error(`Critical Test Failure: King James Bible source file missing at "${KJB_INPUT}". Please place the txt file there before running tests.`);
    }
  });

  after(async () => {
    // Structural cleanup of generated stream fragments
    for (const file of [KJB_REDACTED, KJB_RESTORED]) {
      try { await fs.unlink(file); } catch {}
    }
    if (db) db.close();
  });

  describe('Redaction & Unredaction Lifecycle', () => {

    test('should execute complete text-masking and restoration cycle on KJB', async () => {
      db = initDatabase();

      // Read original snapshot to compare content post-restoration
      const originalText = await fs.readFile(KJB_INPUT, 'utf8');

      // 1. CONFIGURE REDACTION MATRIX
      // Validates high-frequency keywords, multi-word phrases, and long targets
      const redactOpts = {
        context: '20',
        keywords: 'Jesus, "heaven and the earth", "all these words, saying", "and the Lord said"',
        encrypt: false,
        keyfile: null
      };

      const redactResults = await runRedactionPhase([KJB_INPUT], redactOpts, db, null);

      // Extract the result item from the processed files array array
      const resultItem = redactResults[0];
      assert.ok(resultItem, 'Pipeline did not yield any execution results items.');
      assert.strictEqual(resultItem.success, true, `Redaction engine crashed: ${resultItem.error}`);
      assert.strictEqual(existsSync(KJB_REDACTED), true, 'Redacted output text file missing.');

      // 2. SCRUTINIZE REDACTED STREAM OUTPUT
      const redactedText = await fs.readFile(KJB_REDACTED, 'utf8');

      // Assert that exactly zero unredacted matches of the keyword "Jesus" remain
      const jesusMatches = redactedText.match(/\bJesus\b/g);
      assert.strictEqual(jesusMatches, null, 'The keyword "Jesus" escaped target sanitization.');

      // Assert long phrases are correctly replaced by placeholders
      assert.ok(redactedText.includes('XXXX'), 'Redaction placeholder tokens "XXXX" were not injected.');
      assert.ok(!redactedText.includes('heaven and the earth'), 'Long multi-word phrase escaped redaction.');
      assert.ok(!redactedText.includes('all these words, saying'), 'Punctuation-enveloped phrase escaped redaction.');

      // 3. EXECUTE UNREDACTION/RECONSTRUCTION REVERSAL
      const unredactOpts = { encrypt: false, keyfile: null };
      const unredactResults = await runUnredactionPhase([KJB_REDACTED], unredactOpts, db, null);

      const unredactItem = unredactResults[0];
      assert.ok(unredactItem, 'Pipeline did not yield any restoration results items.');
      assert.strictEqual(unredactItem.success, true, `Restoration engine crashed: ${unredactItem.error}`);
      assert.strictEqual(existsSync(KJB_RESTORED), true, 'Restored text file artifact missing.');

      // 4. VERIFY SYSTEM REVERSAL INTEGRITY
      const restoredText = await fs.readFile(KJB_RESTORED, 'utf8');

      // The output text must exactly mirror the original source line-for-line, byte-for-byte
      assert.strictEqual(restoredText, originalText, 'Critical structural deviation found between original and restored text.');

      db.close();
    });

    test('should gracefully handle empty or absent keyword lists without wiping text data', async () => {
      db = initDatabase();

      const originalText = await fs.readFile(KJB_INPUT, 'utf8');
      const blankOpts = { context: '20', keywords: '', encrypt: false };

      const redactResults = await runRedactionPhase([KJB_INPUT], blankOpts, db, null);

      const resultItem = redactResults[0];
      assert.strictEqual(resultItem.success, true);

      const redactedText = await fs.readFile(KJB_REDACTED, 'utf8');
      assert.strictEqual(redactedText, originalText, 'The application altered text despite no keywords being targeted.');

      db.close();
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
