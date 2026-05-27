import { test, describe, beforeEach } from 'node:test';
import strict from 'node:assert/strict';
import assert from 'node:assert/strict';
import { Readable } from 'node:stream';
import { createRedactionTransformer, parseKeywords } from './transformers.js';

/**
 * Synchronous wrapper fulfilling the exact contract required by the test suite.
 */
export function redactString(keywordsString, text) {
  if (!text) return '';
  if (!keywordsString) return text;

  const keywords = parseKeywords(keywordsString);
  if (keywords.length === 0) return text;

  // Domain Rule Fix: Use a word boundary wrapper pattern for whole words
  // Domain Rule Fix: Add case-insensitivity flags to the underlying transformer
  const transformer = createRedactionTransformer({
    keywords,
    contextLen: 0, // No context requested by these tests
    isWholeWord: true,
    isCaseInsensitive: true,
  });

  let outputText = '';

  // Synchronous consumption of the transformer implementation
  transformer.on('data', (chunk) => {
    outputText += chunk.text;
  });

  // Inject text directly into the transformer
  transformer.write(text);
  transformer.end();

  return outputText;
}

// Helper function to process streams cleanly in tests
const processStream = (transformer, chunks) => {
  return new Promise((resolve, reject) => {
    const results = [];
    const source = Readable.from(chunks);

    transformer.on('data', (data) => results.push(data));
    transformer.on('end', () => resolve(results));
    transformer.on('error', reject);

    source.pipe(transformer);
  });
};

describe('Redaction Transformer Tests', () => {
  let config;

  beforeEach(() => {
    config = {
      keywords: ['secret', 'password', 'key'],
      contextLen: 5,
    };
  });

  test('should pass through clean text without redactions', async () => {
    const transformer = createRedactionTransformer(config);
    const chunks = ['Hello World. ', 'This is safe text.'];

    const results = await processStream(transformer, chunks);

    const totalText = results.map((r) => r.text).join('');
    strict.match(totalText, /Hello World/);
    strict.equal(
      results.some((r) => r.matches.length > 0),
      false
    );
  });

  test('should find and redact a single target keyword', async () => {
    const transformer = createRedactionTransformer(config);
    const chunks = ['Your password is weak.'];

    const results = await processStream(transformer, chunks);

    // Find the chunk containing the match metadata
    const redactionChunk = results.find((r) => r.matches.length > 0);

    strict.ok(redactionChunk, 'Should emit match metadata');
    strict.equal(redactionChunk.matches[0].matchedWord, 'password');
    strict.equal(redactionChunk.matches[0].contextBefore, 'Your ');
    strict.equal(redactionChunk.matches[0].contextAfter, ' is w');
  });

  test('should handle keywords broken across stream chunk boundaries', async () => {
    const transformer = createRedactionTransformer(config);
    // "secret" is split into "sec" and "ret"
    const chunks = ['My top sec', 'ret message here.'];

    const results = await processStream(transformer, chunks);

    const allMatches = results.flatMap((r) => r.matches);
    strict.equal(allMatches.length, 1, 'Should find the split keyword');
    strict.equal(allMatches[0].matchedWord, 'secret');
  });

  test('should prioritize longer keywords to prevent partial redactions', async () => {
    const customConfig = { keywords: ['key', 'keyboard'], contextLen: 2 };
    const transformer = createRedactionTransformer(customConfig);
    const chunks = ['Type on the keyboard.'];

    const results = await processStream(transformer, chunks);
    const allMatches = results.flatMap((r) => r.matches);

    strict.equal(allMatches.length, 1);
    strict.equal(allMatches[0].matchedWord, 'keyboard');
  });

  test('should properly track absolute positions across multiple chunks', async () => {
    const transformer = createRedactionTransformer({
      ...config,
    });
    const chunks = ['12345 ', 'secret']; // Clean Code Fix: Added space after 5 to create a boundary

    const results = await processStream(transformer, chunks);
    const allMatches = results.flatMap((r) => r.matches);

    strict.equal(allMatches.length, 1);
    strict.equal(allMatches[0].absoluteOriginalPosition, 6); // Position shifts by 1 because of space
  });
});

describe('Document Redactor - Comprehensive Test Suite', () => {
  describe('Basic Functionality', () => {
    test('should redact a single keyword', () => {
      const keywords = 'beer';
      const text = 'I would like a cold beer please.';
      const expected = 'I would like a cold XXXX please.';
      assert.strictEqual(redactString(keywords, text), expected);
    });

    test('should redact a quoted phrase', () => {
      const keywords = '"Boston Red Sox"';
      const text = 'The Boston Red Sox won the game.';
      const expected = 'The XXXX won the game.';
      assert.strictEqual(redactString(keywords, text), expected);
    });

    test('should handle mixed comma and space delimiters', () => {
      const keywords = 'beer, "cheese pizza"';
      const text = 'We ordered cheese pizza and beer.';
      const expected = 'We ordered XXXX and XXXX.';
      assert.strictEqual(redactString(keywords, text), expected);
    });
  });

  describe('Edge Cases & Boundary Conditions', () => {
    test('should be case-insensitive', () => {
      const keywords = 'BEER';
      const text = 'I drank a cold beer.';
      const expected = 'I drank a cold XXXX.';
      assert.strictEqual(redactString(keywords, text), expected);
    });

    test('should only match whole words (ignore substrings)', () => {
      const keywords = 'apple';
      const text = 'The pineapple was delicious.';
      const expected = 'The pineapple was delicious.';
      assert.strictEqual(redactString(keywords, text), expected);
    });

    test('should preserve punctuation attached to keywords', () => {
      const keywords = 'beer';
      const text = 'Do you want a beer?';
      const expected = 'Do you want a XXXX?';
      assert.strictEqual(redactString(keywords, text), expected);
    });

    test('should return empty string if document text is empty', () => {
      const keywords = 'secret';
      const text = '';
      const expected = '';
      assert.strictEqual(redactString(keywords, text), expected);
    });

    test('should return original text if keywords string is empty', () => {
      const keywords = '';
      const text = 'This is top secret.';
      const expected = 'This is top secret.';
      assert.strictEqual(redactString(keywords, text), expected);
    });
  });

  describe('Architecture Verification (Index & Collision Tracking)', () => {
    /**
     * Test Rule: Reverse Index Reconstruction (ORDER BY start_idx DESC)
     * If the algorithm mutates from left-to-right, replacing 'a' with 'XXXX' shifts
     * all following character indexes out of alignment. Replacing from right-to-left
     * ensures upcoming index coordinates remain valid.
     */
    test('Reverse Index Reconstruction: should not drift indexes when earlier string mutations alter total length', () => {
      // 'a' expands the string (+3 chars). 'longerphrase' shrinks it (-8 chars).
      const keywords = `a, "longerphrase"`;
      const text = 'a small sample of a longerphrase is here.';
      const expected = 'XXXX small sample of XXXX XXXX is here.';
      assert.strictEqual(redactString(keywords, text), expected);
    });

    /**
     * Test Rule: Overlapping Security Protection (Nested Collisions)
     * When a keyword ('Cheese') is fully wrapped inside a phrase ('Cheese Pizza'),
     * chronological filtering must drop the redundant inner token match to prevent
     * breaking index arrays or causing double-redaction ('XXXX XXXX' or 'XXXX Pizza').
     */
    test('Overlapping Security Protection: should drop nested matches to avoid double-redaction corruption', () => {
      const keywords = `"Cheese Pizza", Cheese`;
      const text = 'I love Cheese Pizza.';
      const expected = 'I love XXXX.';
      assert.strictEqual(redactString(keywords, text), expected);
    });

    /**
     * Test Rule: Overlapping Security Protection (Partial Collisions)
     * If two target zones partially intersect, the first validated match wins,
     * and the intersecting portions of the subsequent match must be neutralized.
     */
    test('Overlapping Security Protection: should handle partially intersecting index ranges safely', () => {
      const keywords = `"secret document", "document protocol"`;
      const text = 'This is a secret document protocol.';
      const expected = 'This is a XXXX protocol.';
      assert.strictEqual(redactString(keywords, text), expected);
    });
  });

  describe('Integration Demo', () => {
    test('should pass the comprehensive assignment specification', () => {
      const keywords = `Hello world "Boston Red Sox", 'Pepperoni Pizza', 'Cheese Pizza', beer`;
      const text =
        'Hello world! The Boston Red Sox love Pepperoni Pizza and cold beer.';
      const expected = 'XXXX XXXX! The XXXX love XXXX and cold XXXX.';
      assert.strictEqual(redactString(keywords, text), expected);
    });
  });
});
