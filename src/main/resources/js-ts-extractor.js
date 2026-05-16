#!/usr/bin/env node
/**
 * js-ts-extractor.js — JavaScript/TypeScript source extractor for pharos.
 *
 * Usage:
 *   node js-ts-extractor.js --root <dir>    # extract all .js/.ts/.jsx/.tsx under dir
 *   node js-ts-extractor.js --file <path>   # extract a single file
 *
 * Output: same JSON schema as python-extractor.py (written to stdout).
 * Requires: Node.js 14+ (no npm packages needed).
 */

'use strict';

const fs   = require('fs');
const path = require('path');

// ── CLI args ──────────────────────────────────────────────────────────────────

const args = process.argv.slice(2);
let mode = null, target = null;
for (let i = 0; i < args.length; i++) {
    if (args[i] === '--root')  { mode = 'root'; target = args[++i]; }
    if (args[i] === '--file')  { mode = 'file'; target = args[++i]; }
}
if (!mode || !target) {
    process.stderr.write('Usage: js-ts-extractor.js --root <dir> | --file <file>\n');
    process.exit(1);
}

// ── Directory walk ────────────────────────────────────────────────────────────

const EXTENSIONS = new Set(['.js', '.ts', '.jsx', '.tsx', '.mjs', '.cjs']);
const SKIP_DIRS  = new Set(['node_modules', '.next', 'dist', 'build', '.git',
                             'coverage', '__pycache__', '.cache', 'out', '.turbo']);

function collectFiles(dir) {
    const results = [];
    function walk(d) {
        let entries;
        try { entries = fs.readdirSync(d, { withFileTypes: true }); }
        catch (_) { return; }
        for (const e of entries) {
            if (e.isDirectory()) {
                if (!SKIP_DIRS.has(e.name)) walk(path.join(d, e.name));
            } else if (EXTENSIONS.has(path.extname(e.name).toLowerCase())) {
                results.push(path.join(d, e.name));
            }
        }
    }
    walk(dir);
    return results;
}

// ── Source analysis ───────────────────────────────────────────────────────────

/**
 * Strip comments and string literals to avoid false-positive regex matches,
 * replacing each character with a space so line numbers stay intact.
 */
function stripStringsAndComments(src) {
    let out = '';
    let i = 0;
    const n = src.length;
    while (i < n) {
        // Block comment
        if (src[i] === '/' && src[i+1] === '*') {
            const end = src.indexOf('*/', i + 2);
            if (end === -1) { out += ' '.repeat(n - i); break; }
            const chunk = src.slice(i, end + 2);
            out += chunk.replace(/[^\n]/g, ' ');
            i = end + 2;
            continue;
        }
        // Line comment
        if (src[i] === '/' && src[i+1] === '/') {
            let j = i;
            while (j < n && src[j] !== '\n') j++;
            out += ' '.repeat(j - i);
            i = j;
            continue;
        }
        // Template literal (backtick)
        if (src[i] === '`') {
            let j = i + 1;
            while (j < n && src[j] !== '`') {
                if (src[j] === '\\') j++;
                j++;
            }
            const chunk = src.slice(i, Math.min(j + 1, n));
            out += chunk.replace(/[^\n]/g, ' ');
            i = j + 1;
            continue;
        }
        // Single/double-quoted string
        if (src[i] === '"' || src[i] === "'") {
            const q = src[i]; let j = i + 1;
            while (j < n && src[j] !== q && src[j] !== '\n') {
                if (src[j] === '\\') j++;
                j++;
            }
            const chunk = src.slice(i, Math.min(j + 1, n));
            out += chunk.replace(/[^\n]/g, ' ');
            i = j + 1;
            continue;
        }
        out += src[i++];
    }
    return out;
}

/** Extract JSDoc comment ending just before `lineNo` (1-based). */
function extractJsdoc(lines, lineNo) {
    let i = lineNo - 2; // 0-based, line before declaration
    // Skip blank lines
    while (i >= 0 && lines[i].trim() === '') i--;
    if (i < 0 || !lines[i].trimEnd().endsWith('*/')) return null;
    const endLine = i;
    while (i >= 0 && !lines[i].trimStart().startsWith('/**')) i--;
    if (i < 0) return null;
    return lines.slice(i, endLine + 1)
        .map(l => l.replace(/^\s*\*\s?/, '').trim())
        .filter(l => l !== '/**' && l !== '*/')
        .join(' ')
        .trim() || null;
}

/** Line number (1-based) of a character offset in the source. */
function lineOf(src, offset) {
    let n = 1;
    for (let i = 0; i < offset; i++) if (src[i] === '\n') n++;
    return n;
}

/**
 * Heuristic: find where the block starting at `openPos` ('{') ends.
 * Returns the offset of the matching '}'.
 */
function findBlockEnd(src, openPos) {
    let depth = 0;
    for (let i = openPos; i < src.length; i++) {
        if (src[i] === '{') depth++;
        else if (src[i] === '}') { if (--depth === 0) return i; }
    }
    return src.length - 1;
}

/**
 * Scan backwards from lineNo (1-based) to collect TypeScript/JS decorators
 * (@Decorator or @Decorator(...)) on the lines immediately preceding a declaration.
 */
function extractDecorators(lines, lineNo) {
    const decorators = [];
    let i = lineNo - 2; // 0-based index of the line before the declaration
    while (i >= 0) {
        const line = lines[i].trim();
        if (line === '') { i--; continue; }
        const m = /^@([A-Za-z$_][A-Za-z0-9$_]*)/.exec(line);
        if (m) { decorators.unshift(m[1]); i--; }
        else break;
    }
    return decorators;
}

/**
 * Extract class property / field declarations from a class body.
 * Handles TypeScript typed fields and plain JavaScript class fields.
 * Skips method declarations (lines followed by '(').
 */
function extractClassFields(srcLines, startLine, endLine) {
    const fields = [];
    const seen = new Set();
    // Modifiers that can precede a field name
    const MOD_RE = /^(?:(?:public|private|protected|readonly|static|override|abstract|declare)\s+)*/;
    const FIELD_RE = /^([A-Za-z$_][A-Za-z0-9$_]*)[?!]?\s*(?::\s*([^=;\n{(]+?))?(?:\s*=|\s*;|$)/;

    for (let i = startLine; i <= endLine - 1 && i < srcLines.length; i++) {
        const raw  = srcLines[i];
        const line = raw.trim();
        if (!line || line.startsWith('//') || line.startsWith('*') || line.startsWith('/*')) continue;
        // Strip leading modifiers
        const stripped = line.replace(MOD_RE, '');
        const m = FIELD_RE.exec(stripped);
        if (!m) continue;
        const name = m[1];
        if (BUILTIN_CALLS.has(name) || name === 'constructor') continue;
        // If the very next non-space char after the name (and optional ?) is '(' it's a method
        const afterName = stripped.slice(m[1].length).trimStart();
        if (afterName.startsWith('(') || afterName.startsWith('?:') && afterName[2] === '(') continue;
        if (seen.has(name)) continue;
        seen.add(name);
        const fieldType = m[2] ? m[2].trim().replace(/\s+/g, ' ') : null;
        fields.push({ name, type: fieldType, line: i + 1 });
    }
    return fields;
}

/** Extract simple call names from a function body text. */
function extractCalls(body) {
    const calls = new Set();
    const re = /\b([a-zA-Z_$][a-zA-Z0-9_$]*)\s*\(/g;
    let m;
    while ((m = re.exec(body)) !== null) {
        const name = m[1];
        if (!BUILTIN_CALLS.has(name)) calls.add(name);
    }
    return [...calls];
}

const BUILTIN_CALLS = new Set([
    'if','for','while','switch','catch','typeof','instanceof','return','throw',
    'new','delete','void','await','yield','require','import','super','console',
    'Object','Array','String','Number','Boolean','Promise','JSON','Math','Date',
    'Error','Symbol','Map','Set','WeakMap','WeakSet','parseInt','parseFloat',
    'isNaN','isFinite','setTimeout','setInterval','clearTimeout','clearInterval',
    'process','Buffer','module','exports','describe','it','test','expect','jest',
]);

// ── Main extractor ────────────────────────────────────────────────────────────

function extractFile(filePath, rootDir) {
    let src;
    try { src = fs.readFileSync(filePath, 'utf8'); }
    catch (_) { return null; }

    const lines  = src.split('\n');
    const clean  = stripStringsAndComments(src);

    // Derive package from relative path
    const rel    = rootDir ? path.relative(rootDir, filePath) : filePath;
    const dirParts = path.dirname(rel).split(path.sep).filter(p => p && p !== '.');
    const pkg    = dirParts.join('.');

    const classes   = [];
    const functions = [];
    const classMap  = {};  // className → ParsedClass

    // ── Classes ──────────────────────────────────────────────────────────────
    // Matches: [export] [default] [abstract] class Foo [extends Bar] [implements I, J] {
    const CLASS_RE = /(?:export\s+(?:default\s+)?)?(?:abstract\s+)?class\s+([A-Z$_][A-Za-z0-9$_]*)(?:\s+extends\s+([A-Za-z0-9$_.]+))?/g;
    let m;
    while ((m = CLASS_RE.exec(clean)) !== null) {
        const name     = m[1];
        const base     = m[2] || null;
        const lineNo   = lineOf(src, m.index);
        const openPos  = clean.indexOf('{', m.index + m[0].length);
        const closePos = openPos >= 0 ? findBlockEnd(clean, openPos) : src.length;
        const endLine  = lineOf(src, closePos);
        const qualified= pkg ? pkg + '.' + name : name;
        const jsdoc    = extractJsdoc(lines, lineNo);
        const decs     = extractDecorators(lines, lineNo);

        // Extract `implements` clause from the header text (between match end and '{')
        const header = clean.slice(m.index + m[0].length, openPos);
        const implM  = /\bimplements\s+([\w$.,\s]+)/.exec(header);
        const interfaces = implM
            ? implM[1].split(',').map(s => s.trim()).filter(Boolean)
            : [];

        const cls = { name, qualified, bases: base ? [base] : [], interfaces,
                      decorators: decs, docstring: jsdoc, start: lineNo, end: endLine,
                      fields: extractClassFields(lines, lineNo, endLine) };
        classes.push(cls);
        classMap[name] = { cls, openPos, closePos };
    }

    // ── Interface / type alias (TypeScript) ──────────────────────────────────
    const IFACE_RE = /(?:export\s+)?interface\s+([A-Z$_][A-Za-z0-9$_]*)/g;
    while ((m = IFACE_RE.exec(clean)) !== null) {
        if (classMap[m[1]]) continue; // already handled
        const name     = m[1];
        const lineNo   = lineOf(src, m.index);
        const openPos  = clean.indexOf('{', m.index + m[0].length);
        const closePos = openPos >= 0 ? findBlockEnd(clean, openPos) : src.length;
        const endLine  = lineOf(src, closePos);
        const qualified= pkg ? pkg + '.' + name : name;
        const jsdoc    = extractJsdoc(lines, lineNo);

        // TypeScript interfaces can extend multiple interfaces
        const iHeader   = clean.slice(m.index + m[0].length, openPos >= 0 ? openPos : m.index + m[0].length + 200);
        const iExtends  = /\bextends\s+([\w$.,\s]+)/.exec(iHeader);
        const iBases    = iExtends ? iExtends[1].split(',').map(s => s.trim()).filter(Boolean) : [];
        const cls = { name, qualified, bases: iBases, interfaces: [],
                      decorators: ['interface'], docstring: jsdoc, start: lineNo, end: endLine,
                      fields: extractClassFields(lines, lineNo, endLine) };
        classes.push(cls);
        classMap[name] = { cls, openPos, closePos };
    }

    // ── Methods and functions ─────────────────────────────────────────────────
    // Pattern: [static] [async] [get/set] name([params]) {
    //          OR: [export] [async] function name([params]) {
    //          OR: [export] const name = [async] ([params]) =>
    const FUNC_PATTERNS = [
        // Named function declaration: [export] [async] function foo(params)
        /(?:export\s+(?:default\s+)?)?(?:async\s+)?function\s*\*?\s*([A-Za-z$_][A-Za-z0-9$_]*)\s*\(([^)]*)\)/g,
        // Method shorthand in class: [static] [async] [get|set] foo(params)
        /(?:static\s+)?(?:async\s+)?(?:(?:get|set)\s+)?([A-Za-z$_][A-Za-z0-9$_]*)\s*\(([^)]*)\)\s*(?::\s*\S+\s*)?\{/g,
        // Arrow function: [export] const foo = [async] (params) =>
        /(?:export\s+)?(?:const|let|var)\s+([A-Za-z$_][A-Za-z0-9$_]*)\s*=\s*(?:async\s+)?\(?([^)=]*)\)?\s*=>/g,
    ];

    // Collect all function candidates from clean source
    const funcCandidates = [];
    for (const re of FUNC_PATTERNS) {
        let fm;
        while ((fm = re.exec(clean)) !== null) {
            funcCandidates.push({ name: fm[1], rawParams: fm[2] || '', pos: fm.index });
        }
    }

    // Sort by position
    funcCandidates.sort((a, b) => a.pos - b.pos);

    // Deduplicate by position (multiple patterns may match the same spot)
    const seen = new Set();
    for (const fc of funcCandidates) {
        const key = Math.floor(fc.pos / 5); // coarse dedup bucket
        if (seen.has(key)) continue;
        seen.add(key);

        const name    = fc.name;
        // Skip keywords captured by patterns
        if (BUILTIN_CALLS.has(name) || name === 'constructor') {
            // constructor handled separately
        }
        const lineNo  = lineOf(src, fc.pos);
        const params  = fc.rawParams.split(',').map(p => p.trim().replace(/[=:][^,]*/,'').trim())
                          .filter(p => p && p !== '...');

        // Find which class this belongs to (if any)
        let className = null;
        for (const [cn, { openPos, closePos }] of Object.entries(classMap)) {
            if (fc.pos > openPos && fc.pos < closePos) { className = cn; break; }
        }

        // Find body end
        const bodyOpenPos = clean.indexOf('{', fc.pos + fc.name.length);
        let bodyText = '';
        let endLine  = lineNo;
        if (bodyOpenPos >= 0 && bodyOpenPos < fc.pos + 200) {
            const bodyClosePos = findBlockEnd(clean, bodyOpenPos);
            bodyText = src.slice(bodyOpenPos + 1, bodyClosePos).trim();
            endLine  = lineOf(src, bodyClosePos);
        }

        const jsdoc  = extractJsdoc(lines, lineNo);
        const decs   = extractDecorators(lines, lineNo);
        const isCtor = name === 'constructor' || (className && name === className);
        const calls  = extractCalls(bodyText);

        functions.push({
            name, class_name: className,
            params, decorators: decs,
            docstring: jsdoc,
            body: src.slice(fc.pos, Math.min(fc.pos + 200, src.length)).split('\n')[0].trim(),
            calls, start: lineNo, end: endLine, is_constructor: isCtor,
        });
    }

    // Deduplicate functions by (name, class_name, start)
    const funcSeen = new Set();
    const dedupedFunctions = functions.filter(f => {
        const k = `${f.class_name}|${f.name}|${f.start}`;
        if (funcSeen.has(k)) return false;
        funcSeen.add(k);
        return true;
    });

    return {
        file: filePath,
        package: pkg,
        classes,
        functions: dedupedFunctions,
    };
}

// ── Entry point ───────────────────────────────────────────────────────────────

let results;
if (mode === 'file') {
    const r = extractFile(target, path.dirname(target));
    results = r ? [r] : [];
} else {
    const files = collectFiles(target);
    results = files.map(f => extractFile(f, target)).filter(Boolean);
}

process.stdout.write(JSON.stringify(results));
