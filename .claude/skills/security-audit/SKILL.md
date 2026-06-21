---
name: security-audit
description: Conducts a comprehensive cybersecurity audit, safely fixes vulnerabilities, and rigorously tests fixes to ensure nothing breaks.
disable-model-invocation: true
allowed-tools: Bash(*) Glob Grep ReadFile EditFile
---

# Cybersecurity Audit & Auto-Fix

You are acting as a Senior Application Security Engineer. Your task is to audit the current project, fix any discovered vulnerabilities, and **rigorously test your changes to ensure you do not break the application.**

## Target Scope
$ARGUMENTS

## Phase 1: Baseline Verification (DO NOT SKIP)
Before touching ANY code, you MUST establish a baseline to ensure the app is currently working.
1. Find and run the project's test suite (e.g., `npm test`, `pytest`, `cargo test`, `make test`).
2. Record the passing/failing state. If the baseline is already broken, inform the user and ask for permission before proceeding with security fixes.

## Phase 2: Vulnerability Discovery
Use your Bash tool to run the following security scans. If a tool is not installed, gracefully skip it.
1. **Secrets Detection:** `gitleaks detect --no-git -v` 
2. **SAST (Code Vulnerabilities):** `semgrep scan --config auto`
3. **Vulnerable Dependencies:** `npm audit` or `pip-audit` or `trivy fs .`

*Fallback:* If no scanners are available, use `Glob` and `Grep` to manually hunt for OWASP Top 10 flaws (e.g., SQL Injection, XSS, hardcoded passwords, missing CSRF tokens).

## Phase 3: Safe Remediation & Verification Loop
For every vulnerability found, follow this strict loop to guarantee you do not break the app:
1. **Plan:** Identify the secure fix (e.g., parameterizing a query, hashing a password, sanitizing input).
2. **Fix:** Edit the file to apply the fix.
3. **Test:** IMMEDIATELY run the test suite again.
4. **Rollback if Broken:** If your fix causes ANY test to fail, or breaks the build, **revert the file change immediately**, rethink your approach, and apply an alternative fix. Do not leave the code in a broken state.

## Core Security Rules to Enforce:
- **No Secrets in Code:** Move hardcoded credentials to environment variables.
- **Input Validation:** Never trust user input. Always sanitize.
- **Cryptography:** Replace weak crypto (MD5/SHA1) with modern standards (bcrypt/Argon2/AES-GCM).
- **Least Privilege:** Ensure infrastructure files (Dockerfiles, CI/CD) run as non-root users.

## Final Report
When finished, output a summary of:
1. Vulnerabilities found.
2. Files modified.
3. Confirmation that all tests are still passing.