import { fileURLToPath } from 'node:url';
import { createWriteStream } from 'fs';
import { pipeline } from 'stream/promises';
import path from 'path';
import {
  Worker,
  isMainThread,
  workerData,
  parentPort,
} from 'node:worker_threads';
import { program } from 'commander';
import pc from 'picocolors';
import {
  initDatabase,
  createWorkerDatabaseConnection,
  validateCliOptions,
  resolveMasterKey,
  renderFinalAuditReport,
} from './lib.js';
import {
  decryptText,
  parseKeywords,
  calculateFileMd5,
  preparePipelineStreams,
  createRedactionTransformer,
  createDatabaseBatchWriterTransformer,
  createTextExtractionTransformer,
  createUnredactionTransformer,
} from './transformers.js';

const __filename = fileURLToPath(import.meta.url);

if (isMainThread) {
  program
    .name('declassify-master-tool')
    .description(
      'Concurrently redacts or unredacts files using isolated worker threads, each utilizing individual database connections.'
    )
    .argument('<filepaths...>', 'paths to the target text files')
    .option('-r, --redact', 'Execute text masking / redaction phase')
    .option(
      '-u, --unredact',
      'Execute document restoration / unredaction phase'
    )
    .option(
      '-k, --keywords <string>',
      'comma/space split keywords (required for redaction). Wrap phrases in quotes.'
    )
    .option(
      '-c, --context <number>',
      'number of characters of padding around the redaction context tracking',
      '20'
    )
    .option(
      '--encrypt',
      'Enable AES-256-GCM encryption for stored metadata parameters'
    )
    .option(
      '--keyfile <path>',
      'Path to the central master encryption key file'
    )
    .option(
      '--generate-key',
      'Automatically generate the master key file if it does not exist'
    );

  /**
   * Spawns a dedicated worker thread to process a target file.
   */
  function runWorker(workerOptions) {
    return new Promise((resolve) => {
      const worker = new Worker(__filename, { workerData: workerOptions });

      worker.on('error', (err) => {
        resolve({
          filepath: workerOptions.filepath,
          error: `Worker Crash: ${err.message}`,
          success: false,
        });
      });

      worker.on('exit', (code) => {
        if (code !== 0) {
          resolve({
            filepath: workerOptions.filepath,
            error: `Worker exited with status code ${code}`,
            success: false,
          });
        }
      });

      worker.on('message', (msg) => {
        if (msg.type === 'RESULT') {
          resolve(msg.data);
        }
      });
    });
  }

  async function main() {
    program.parse(process.argv);
    const filepaths = program.args;
    const opts = program.opts();

    if (filepaths.length === 0) {
      console.error(
        pc.red('Error: Please provide at least one target file path.')
      );
      process.exit(1);
    }

    validateCliOptions(opts);

    // Master ensures schema and WAL are initialized once before workers boot up
    const masterKeyBuffer = resolveMasterKey(opts);
    const masterDb = initDatabase();
    masterDb.close(); // Close to avoid holding an extra connection handle unnecessarily

    let operationalSummaryResults = [];

    if (opts.redact) {
      console.log(
        pc.red(pc.bold(`\n🔒 INITIALIZING MULTI-THREADED DB REDACTION PROCESS`))
      );

      const workerPromises = filepaths.map((filepath) =>
        runWorker({
          mode: 'REDACT',
          filepath,
          opts,
          masterKeyBuffer,
        })
      );
      operationalSummaryResults = await Promise.all(workerPromises);
    } else if (opts.unredact) {
      console.log(
        pc.cyan(
          pc.bold(
            `\n🔓 INITIALIZING MULTI-THREADED DB UNREDACTION RECONSTRUCTION`
          )
        )
      );

      const workerPromises = filepaths.map((filepath) =>
        runWorker({
          mode: 'UNREDACT',
          filepath,
          masterKeyBuffer,
        })
      );
      operationalSummaryResults = await Promise.all(workerPromises);
    }

    renderFinalAuditReport(operationalSummaryResults);
  }

  main().catch((err) => {
    console.error(pc.red(`Fatal Execution Panic Error: ${err.message}`));
    process.exit(1);
  });
} else {
  // ==========================================
  // WORKER THREAD ISOLATED EXECUTION BOUNDARY
  // ==========================================
  const CHUNK_SIZE = 64 * 1024;

  async function executeRedactionWorker({ filepath, opts, masterKeyBuffer }) {
    const filename = path.basename(filepath);
    const parsedPath = path.parse(filepath);
    const outputPath = path.join(
      parsedPath.dir,
      `${parsedPath.name}.REDACTED${parsedPath.ext}`
    );

    let originalMd5 = '';
    try {
      originalMd5 = await calculateFileMd5(filepath);
    } catch (err) {
      return {
        filepath,
        error: `Hash Failure: ${err.message}`,
        success: false,
      };
    }

    // Initialize individual concurrent worker connection instance
    const db = createWorkerDatabaseConnection();

    const insertStmt = db.prepare(`
      INSERT INTO redactions (document_name, original_md5, redacted_word, char_position, redacted_position, context_before, context_after, is_encrypted)
      VALUES (@documentName, @originalMd5, @redactedWord, @charPosition, @redactedPosition, @contextBefore, @contextAfter, @isEncrypted)
    `);

    const updateHashStmt = db.prepare(`
      UPDATE redactions SET redacted_md5 = ? WHERE original_md5 = ? AND redacted_md5 IS NULL
    `);

    const keywords = parseKeywords(opts.keywords);
    const contextLen = parseInt(opts.context, 10);

    const redactionTransformer = createRedactionTransformer({
      keywords,
      contextLen,
    });
    const textExtractionTransformer = createTextExtractionTransformer();

    const batchWriterTransformer = createDatabaseBatchWriterTransformer({
      db,
      insertStmt,
      filename,
      originalMd5,
      encryptEnabled: !!opts.encrypt,
      masterKeyBuffer,
      batchSize: 1000, // Safe batch size utilizing your standard transformer configurations
    });

    try {
      const { encoding, steps } = await preparePipelineStreams(
        filepath,
        redactionTransformer,
        CHUNK_SIZE
      );

      await pipeline(
        ...steps,
        batchWriterTransformer,
        textExtractionTransformer,
        createWriteStream(outputPath)
      );

      const redactedMd5 = await calculateFileMd5(outputPath);
      updateHashStmt.run(redactedMd5, originalMd5);

      return {
        filepath,
        outputPath,
        encoding,
        mode: 'REDACTED',
        success: true,
      };
    } catch (err) {
      return { filepath, error: err.message, success: false };
    } finally {
      db.close(); // Clean up thread resources
    }
  }

  async function executeUnredactionWorker({ filepath, masterKeyBuffer }) {
    const parsedPath = path.parse(filepath);
    const cleanName = parsedPath.name.replace('.REDACTED', '');
    const outputPath = path.join(
      parsedPath.dir,
      `${cleanName}.RESTORED${parsedPath.ext}`
    );

    let inputMd5 = '';
    try {
      inputMd5 = await calculateFileMd5(filepath);
    } catch (err) {
      return { filepath, error: `Read error: ${err.message}`, success: false };
    }

    // Initialize individual concurrent worker connection instance
    const db = createWorkerDatabaseConnection();

    try {
      const records = db
        .prepare('SELECT * FROM redactions WHERE redacted_md5 = ?')
        .all(inputMd5);

      if (records.length === 0) {
        return {
          filepath,
          error:
            'Document signature could not be found in tracking database registry.',
          success: false,
        };
      }

      const firstRow = records[0];
      const targetIsEncrypted = firstRow && firstRow.is_encrypted === 1;

      if (targetIsEncrypted && !masterKeyBuffer) {
        return {
          filepath,
          error:
            'This database context is encrypted. Please specify the valid path using the --keyfile parameter.',
          success: false,
        };
      }

      const unredactMap = [];
      records.forEach((r) => {
        let word = r.redacted_word;
        if (targetIsEncrypted) {
          word = decryptText(r.redacted_word, masterKeyBuffer);
        }
        unredactMap.push({
          redactedPosition: r.redacted_position,
          originalWord: word,
        });
      });

      const transformer = createUnredactionTransformer(unredactMap);
      const { encoding, steps } = await preparePipelineStreams(
        filepath,
        transformer,
        CHUNK_SIZE
      );

      await pipeline(...steps, createWriteStream(outputPath));
      return {
        filepath,
        outputPath,
        encoding,
        mode: 'RESTORED',
        success: true,
      };
    } catch (err) {
      return { filepath, error: err.message, success: false };
    } finally {
      db.close(); // Clean up thread resources
    }
  }

  (async () => {
    let result;
    if (workerData.mode === 'REDACT') {
      result = await executeRedactionWorker(workerData);
    } else if (workerData.mode === 'UNREDACT') {
      result = await executeUnredactionWorker(workerData);
    }
    parentPort.postMessage({ type: 'RESULT', data: result });
    process.exit(0);
  })();
}
