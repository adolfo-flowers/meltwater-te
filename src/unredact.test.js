import { test, describe } from 'node:test';
import assert from 'node:assert';
import { Readable, pipeline } from 'node:stream';
import { createUnredactionTransformer } from './transformers.js'; // Adjust path

async function runStream(inputChunks, unredactMap) {
  const source = Readable.from(inputChunks);
  const transformer = createUnredactionTransformer(unredactMap);
  const output = [];

  return new Promise((resolve, reject) => {
    pipeline(
      source,
      transformer,
      async function* (sourceStream) {
        for await (const chunk of sourceStream) {
          output.push(chunk.toString());
        }
      },
      (err) => {
        if (err) reject(err);
        else resolve(output.join(''));
      }
    );
  });
}

describe('Unredaction Transformer', () => {

  test('should pass through text unchanged when map is empty', async () => {
    const chunks = ['Hello ', 'world, this is ', 'a test.'];
    const unredactMap = [];

    const result = await runStream(chunks, unredactMap);

    assert.strictEqual(result, 'Hello world, this is a test.');
  });

  test('should replace a single redacted word in a single chunk', async () => {
    const chunks = ['Hello XXXX world'];
    const unredactMap = [{ redactedPosition: 6, originalWord: 'Bob' }];
    const result = await runStream(chunks, unredactMap);
    assert.strictEqual(result, 'Hello Bob world');
  });

  test('should handle out-of-order map entries by sorting them internally', async () => {
    const chunks = ['XXXX met XXXX today'];
    const unredactMap = [
      { redactedPosition: 9, originalWord: 'Alice' },
      { redactedPosition: 0, originalWord: 'Bob' }
    ];

    const result = await runStream(chunks, unredactMap);

    assert.strictEqual(result, 'Bob met Alice today');
  });

  test('should replace placeholder split across chunk boundaries', async () => {
    const chunks = ['The secret is XX', 'XX right now'];
    const unredactMap = [{ redactedPosition: 14, originalWord: 'safe' }];

    const result = await runStream(chunks, unredactMap);

    assert.strictEqual(result, 'The secret is safe right now');
  });

  test('should correctly stream out text longer than the overlap window size', async () => {
    const longBase = 'A'.repeat(150);
    const chunks = [`${longBase} XXXX finished.`];
    const unredactMap = [{ redactedPosition: 151, originalWord: 'task' }];

    const result = await runStream(chunks, unredactMap);

    assert.strictEqual(result, `${longBase} task finished.`);
  });

  test('should skip unredaction if placeholder text is not XXXX at given position', async () => {
    const chunks = ['Hello YYYY world'];
    const unredactMap = [{ redactedPosition: 6, originalWord: 'Bob' }];

    const result = await runStream(chunks, unredactMap);

    assert.strictEqual(result, 'Hello YYYY world');
  });

  test('should flush remaining text and complete unredaction during the flush phase', async () => {
    const chunks = ['Short XXXX'];
    const unredactMap = [{ redactedPosition: 6, originalWord: 'story' }];

    const result = await runStream(chunks, unredactMap);

    assert.strictEqual(result, 'Short story');
  });
});
