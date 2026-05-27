# ⚡ ShadowWalker

> High-performance, concurrent CLI utility engineered to systematically mask sensitive strings and reconstruct original document state trees using an indexed SQLite registry and AES-256-GCM hardware encryption.

---

## 🚀 Installation

Ensure you have **Node.js (v22.0.0 or higher)** installed on your host system to support native experimental SQLite testing flags.

### 1. Clone & Navigate
```bash
git clone https://github.com/adolfo-flowers/meltwater-te
cd meltwater-te
```

### 2. Install Project Dependencies

```bash
npm install
```

---

## 🛠️ Global Command Reference

Execute the tool via the core script module using target absolute or relative file pathways:

```bash
node index.js <filepaths...> [options]
```

### Command Flags


| Flag | Alias | Description | Default |
| :--- | :--- | :--- | :--- |
| `--redact` | `-r` | Executes deep string-masking loops across files. | N/A |
| `--unredact` | `-u` | Restores files using historical database indices. | N/A |
| `--keywords` | `-k` | Target strings to scrub (comma/space separated). | N/A |
| `--context` | `-c` | Size padding of bytes captured around targets. | `20` |
| `--encrypt` | N/A | Enforces AES-256-GCM security over metadata logs. | `false` |
| `--keyfile` | N/A | Local filepath pointer to the system Master Key. | N/A |
| `--generate-key`| N/A | Writes a fresh cryptographic file if missing. | N/A |

---

## 🕹️ Operational Walkthroughs

### 1. The Redaction Sequence (Cloak)
Parses target logs, strips tracking phrases, saves localized chunk coordinates to SQLite, and dumps a safe `.REDACTED` mirror payload.

```bash
node index.js system.log database.cfg -r -k "admin_root, 10.0.4.21, secret_hash"
```

### 2. The Restoration Sequence (De-Cloak)
Scans the modified document's MD5 check signature, queries internal tracking registries, and structurally patches text blocks back to a clean `.RESTORED` file.

```bash
node index.js system.REDACTED.log -u
```

### 3. Maximum OpSec Configuration (Encrypted Storage)
Safeguards the SQLite tracking index table so that intercepted or stolen databases expose nothing about your redacted keywords without the separate master key file.

```bash
# Encrypt registry rows on the fly with an auto-generated token file
node index.js finance.csv -r -k "revenue_q4" --encrypt --generate-key --keyfile ./sys_vault.key

# Reverse encryption layers to fully recover the document asset
node index.js finance.REDACTED.csv -u --keyfile ./sys_vault.key
```

---

## 🤖 Development & Automation

### Run Automated Tests
Executes the native Node.js test runner suite utilizing experimental built-in database support flags:
```bash
npm test
```

### Code Formatting & Quality Checks
Enforce style guidelines, automatically patch syntax errors, and maintain standard file spacing before code commits:

```bash
# Analyze source patterns using ESLint
npm run lint

# Repair autofixable programmatic lint errors
npm run lint:fix

# Format text syntax structure using Prettier
npm run format
```
