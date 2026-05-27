import { existsSync, writeFileSync, readFileSync } from 'fs';
import { randomBytes } from 'crypto';
import Database from 'better-sqlite3';
import pc from 'picocolors';

export function initDatabase() {
  const db = new Database('redactions.db');
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = NORMAL');
  db.pragma('cache_size = -64000');
  db.pragma('temp_store = MEMORY');
  db.prepare(
    `
    CREATE TABLE IF NOT EXISTS redactions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      document_name TEXT,
      original_md5 TEXT,
      redacted_md5 TEXT,
      redacted_word TEXT,
      char_position INTEGER,
      redacted_position INTEGER,
      context_before TEXT,
      context_after TEXT,
      is_encrypted INTEGER DEFAULT 0
    )
  `
  ).run();

  db.prepare(
    `CREATE INDEX IF NOT EXISTS idx_redactions_original_md5 ON redactions (original_md5)`
  ).run();
  db.prepare(
    `CREATE INDEX IF NOT EXISTS idx_redactions_redacted_md5 ON redactions (redacted_md5)`
  ).run();
  return db;
}

export function validateCliOptions(opts) {
  if (opts.redact && opts.unredact) {
    console.error(
      pc.red(pc.bold('🚨 CLI Error: ')) +
        'You cannot combine options --redact (-r) and --unredact (-u) together.'
    );
    process.exit(1);
  }
  if (!opts.redact && !opts.unredact) {
    console.error(
      pc.red(pc.bold('🚨 CLI Error: ')) +
        'You must choose exactly one action: --redact (-r) OR --unredact (-u).'
    );
    process.exit(1);
  }
  if (opts.redact && !opts.keywords) {
    console.error(
      pc.red(pc.bold('🚨 CLI Error: ')) +
        'The --keywords (-k) option is required during the redaction phase.'
    );
    process.exit(1);
  }
  if (opts.encrypt && !opts.keyfile) {
    console.error(
      pc.red(pc.bold('🚨 CLI Error: ')) +
        'The --keyfile option is required when encryption is enabled.'
    );
    process.exit(1);
  }
}

export function resolveMasterKey(opts) {
  if (!opts.encrypt && !opts.unredact) {
    return null;
  }
  if (!opts.keyfile) {
    return null;
  }

  const keyFileExists = existsSync(opts.keyfile);

  if (!keyFileExists) {
    if (opts.redact && opts.generateKey) {
      const generatedKey = randomBytes(32);
      writeFileSync(opts.keyfile, generatedKey.toString('hex'), 'utf8');
      console.log(
        pc.green(
          `✨ Successfully generated a new Master Key file at: ${pc.bold(opts.keyfile)}`
        )
      );
      return generatedKey;
    }

    if (opts.unredact) {
      console.error(
        pc.red(pc.bold('🚨 CLI Error: ')) +
          `Master key file not found at "${opts.keyfile}". Cannot decrypt records.`
      );
      process.exit(1);
    } else {
      console.error(
        pc.red(pc.bold('🚨 CLI Error: ')) +
          `Master key file not found at "${opts.keyfile}". Pass --generate-key to create it.`
      );
      process.exit(1);
    }
  }

  try {
    const hexKey = readFileSync(opts.keyfile, 'utf8').trim();
    if (hexKey.length !== 64) {
      throw new Error('Key file content must be exactly 64 hex characters.');
    }
    return Buffer.from(hexKey, 'hex');
  } catch (err) {
    console.error(
      pc.red(pc.bold('🚨 Master Key File Corruption Error: ')) + err.message
    );
    process.exit(1);
  }
}

export function renderFinalAuditReport(results) {
  console.log(
    `\n` +
      pc.bgMagenta(
        pc.black(pc.bold(' 📊 ENGINE DECLASSIFICATION OPERATIONS REPORT '))
      )
  );
  console.log(pc.magenta('─'.repeat(80)));
  results.forEach((res) => {
    if (res.success) {
      console.log(
        `${pc.green('✔')} ${pc.bold(res.filepath.padEnd(18))} ${pc.dim(`(${res.encoding})`)}\n` +
          `   └─ Target document successfully ${pc.green(res.mode)}: ${pc.cyan(res.outputPath)}`
      );
    } else {
      console.log(
        `${pc.red('✖')} ${pc.red(res.filepath.padEnd(18))}\n` +
          `   └─ ${pc.inverse(pc.red(' FAILED/ABORTED '))}: ${pc.red(res.error)}`
      );
    }
  });
  console.log(pc.magenta('─'.repeat(80)));
}
