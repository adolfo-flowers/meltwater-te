import { fileURLToPath } from 'node:url';
import { createWriteStream } from 'fs';
import { pipeline } from 'stream/promises';
import path from 'path';
import { program } from 'commander';
import pc from 'picocolors';

// Import helpers from lib.js
import {
  initDatabase,
  validateCliOptions,
  resolveMasterKey,
  renderFinalAuditReport,
} from './lib.js';

// Import streaming operations from transformers.js
import {
  parseKeywords,
  decryptText,
  calculateFileMd5,
  preparePipelineStreams,
  createRedactionTransformer,
  createUnredactionTransformer,
} from './transformers.js';

program
  .name('declassify-master-tool')
  .description(
    'Concurrently redacts or unredacts files using an indexed SQLite database and a central Master Key file.'
  )
  .argument('<filepaths...>', 'paths to the target text files')
  .option('-r, --redact', 'Execute text masking / redaction phase')
  .option('-u, --unredact', 'Execute document restoration / unredaction phase')
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
  .option('--keyfile <path>', 'Path to the central master encryption key file')
  .option(
    '--generate-key',
    'Automatically generate the master key file if it does not exist'
  );

/**
 * Handles processing steps for the Redaction phase.
 */
export async function runRedactionPhase(filepaths, opts, db, masterKeyBuffer) {
  const contextLen = parseInt(opts.context, 10);
  const keywords = parseKeywords(opts.keywords);
  const CHUNK_SIZE = 1000;

  // Prepared statements for high performance execution loops
  const insertStmt = db.prepare(`
    INSERT INTO redactions (document_name, original_md5, redacted_md5, redacted_word, char_position, context_before, context_after, is_encrypted)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const updateHashStmt = db.prepare(`
    UPDATE redactions SET redacted_md5 = ? WHERE original_md5 = ? AND redacted_md5 IS NULL
  `);

  console.log(
    pc.red(pc.bold(`\n🔒 INITIALIZING INLINE PIPELINE REDACTION PROCESS`))
  );
  if (opts.encrypt) {
    console.log(
      pc.green(
        `🛡  Database Encryption Active using Master Key file: ${pc.bold(opts.keyfile)}\n`
      )
    );
  } else {
    console.log(
      pc.yellow(
        `⚠️  Database Encryption Disabled (Storing metadata parameters as cleartext)\n`
      )
    );
  }

  const pipelinePromises = filepaths.map(async (filepath) => {
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

    // Instantiating the streamlined transformer with inline context references
    const transformer = createRedactionTransformer({
      keywords,
      contextLen,
      db,
      insertStmt,
      filename,
      originalMd5,
      encryptEnabled: !!opts.encrypt,
      masterKeyBuffer,
    });

    try {
      const { encoding, steps } = await preparePipelineStreams(
        filepath,
        transformer,
        CHUNK_SIZE
      );

      // Execute stream processing pipeline
      await pipeline(...steps, createWriteStream(outputPath));

      // Calculate final target validation output signature
      const redactedMd5 = await calculateFileMd5(outputPath);

      // Backfill signature tracking properties matching original rows via clean updates
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
    }
  });

  return Promise.all(pipelinePromises);
}

/**
 * Handles processing steps for the Unredaction phase.
 */
export async function runUnredactionPhase(
  filepaths,
  opts,
  db,
  masterKeyBuffer
) {
  const CHUNK_SIZE = 1000;
  console.log(
    pc.cyan(pc.bold(`\n🔓 INITIALIZING INLINE UNREDACTION RECONSTRUCTION`))
  );

  const pipelinePromises = filepaths.map(async (filepath) => {
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

    const records = db
      .prepare(
        'SELECT * FROM redactions WHERE redacted_md5 = ? ORDER BY char_position ASC'
      )
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
    const targetIsEncrypted = firstRow.is_encrypted === 1;

    if (targetIsEncrypted && !masterKeyBuffer) {
      return {
        filepath,
        error:
          'This database context is encrypted. Please specify the valid path using the --keyfile parameter.',
        success: false,
      };
    }

    let currentOffsetTrackingDifference = 0;
    const unredactMap = [];

    try {
      records.forEach((r) => {
        let word = r.redacted_word;
        if (targetIsEncrypted) {
          word = decryptText(r.redacted_word, masterKeyBuffer);
        }
        // Positions map directly to where placeholders appear in the redacted file
        const redactedPosition = r.char_position;
        unredactMap.push({ redactedPosition, originalWord: word });
      });
    } catch (err) {
      return {
        filepath,
        error: `Decryption execution failure. Verify master key file contents: ${err.message}`,
        success: false,
      };
    }

    // Instantiating standard tracking transformer components
    const transformer = createUnredactionTransformer(unredactMap);

    try {
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
    }
  });

  return Promise.all(pipelinePromises);
}

/**
 * Main application entry point orchestrator function.
 */
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

  // Validate inputs using our helper from lib.js
  validateCliOptions(opts);

  const masterKeyBuffer = resolveMasterKey(opts);
  const db = initDatabase();
  let operationalSummaryResults = [];

  if (opts.redact) {
    operationalSummaryResults = await runRedactionPhase(
      filepaths,
      opts,
      db,
      masterKeyBuffer
    );
  } else if (opts.unredact) {
    operationalSummaryResults = await runUnredactionPhase(
      filepaths,
      opts,
      db,
      masterKeyBuffer
    );
  }

  // Display summary results
  renderFinalAuditReport(operationalSummaryResults);
}

const isMain = process.argv[1] === fileURLToPath(import.meta.url);
if (isMain) {
  main().catch((err) => {
    console.error(pc.red(`Fatal Execution Panic Error: ${err.message}`));
    process.exit(1);
  });
}
