package com.pharos.parser;

import com.pharos.parser.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Parser for non-code files: Markdown, plain text, config files, etc.
 *
 * Chunking strategy by file type:
 * - Markdown (.md, .mdx): heading-based sections with breadcrumb context
 * - reStructuredText (.rst, .adoc): underline-based headings
 * - Plain text (.txt): paragraph blocks (double-newline separated)
 * - Structured data (.yaml, .yml, .json, .toml): top-level keys as topics
 * - Everything else: whole file as a single chunk
 *
 * Each chunk is returned as a {@link ParsedMethod} with {@code annotations=["__chunk__"]}.
 * DocumentMapper detects this annotation and assigns {@code docType="chunk"}.
 *
 * Each file also produces a {@link ParsedClass} with {@code kind="document"}
 * representing the file as a whole — for file-level search.
 *
 * Internal Markdown links ([text](./other.md)) are captured as unresolved
 * {@link CallReference}s, making them edges in the knowledge graph.
 */
public class GenericFileParser implements CodeParser {

    private static final Logger log = LoggerFactory.getLogger(GenericFileParser.class);

    /** File extensions handled by this parser. */
    private static final List<String> EXTENSIONS = List.of(
            ".md", ".mdx",
            ".txt",
            ".rst", ".adoc",
            ".html", ".htm",
            ".yaml", ".yml",
            ".json", ".toml",
            ".properties", ".ini", ".conf", ".cfg"
    );

    /** Directory names to skip during project walk. */
    private static final Set<String> SKIP_DIRS = Set.of(
            ".git", ".svn", "node_modules", "target", "build",
            "__pycache__", ".gradle", "dist", ".idea", ".vscode",
            "venv", ".venv"
    );

    /** Max chunk body length before truncation for embeddings. */
    private static final int MAX_CHUNK_BODY = 4_000;

    private final int parseThreads;

    public GenericFileParser() {
        this(1);
    }

    public GenericFileParser(int parseThreads) {
        this.parseThreads = Math.max(1, parseThreads);
    }

    @Override
    public List<String> supportedExtensions() {
        return EXTENSIONS;
    }

    @Override
    public ParsedProject parseProject(Path projectRoot, String projectName) throws IOException {
        // Collect matching files first so they can be dispatched to a thread pool
        List<Path> matchedFiles = new ArrayList<>();
        Files.walkFileTree(projectRoot, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                String name = dir.getFileName() != null ? dir.getFileName().toString() : "";
                return SKIP_DIRS.contains(name)
                        ? FileVisitResult.SKIP_SUBTREE : FileVisitResult.CONTINUE;
            }
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                String name = file.getFileName().toString();
                if (EXTENSIONS.stream().anyMatch(name::endsWith)) matchedFiles.add(file);
                return FileVisitResult.CONTINUE;
            }
        });

        List<ParsedFile> files = new CopyOnWriteArrayList<>();
        if (parseThreads <= 1) {
            for (Path file : matchedFiles) {
                try {
                    files.add(parseFile(file, projectName, projectRoot));
                } catch (Exception e) {
                    log.warn("Failed to parse generic file {}: {}", file, e.getMessage());
                }
            }
        } else {
            // Each file is parsed independently (pure IO + string ops, no shared state)
            ExecutorService pool = Executors.newFixedThreadPool(parseThreads,
                    r -> { Thread t = new Thread(r, "generic-parser"); t.setDaemon(true); return t; });
            try {
                List<Future<ParsedFile>> futures = new ArrayList<>(matchedFiles.size());
                for (Path file : matchedFiles) {
                    futures.add(pool.submit(() -> parseFile(file, projectName, projectRoot)));
                }
                for (Future<ParsedFile> f : futures) {
                    try {
                        files.add(f.get());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Generic parsing interrupted", e);
                    } catch (ExecutionException e) {
                        log.warn("Failed to parse generic file: {}", e.getCause().getMessage());
                    }
                }
            } finally {
                pool.shutdown();
            }
        }
        return new ParsedProject(projectName, projectRoot.toString(), files);
    }

    @Override
    public ParsedFile parseFile(Path file, String projectName) throws IOException {
        return parseFile(file, projectName, file.getParent());
    }

    private ParsedFile parseFile(Path file, String projectName, Path root) throws IOException {
        String content = Files.readString(file);
        String fileName = file.getFileName().toString();
        String fileStem = stem(fileName);

        // Derive package from directory relative to root
        Path relDir;
        try {
            relDir = root.relativize(file.toAbsolutePath().getParent());
        } catch (IllegalArgumentException e) {
            relDir = Path.of("");
        }
        String pkg = relDir.toString().isEmpty() ? ""
                : relDir.toString().replace(FileSystems.getDefault().getSeparator(), ".");
        String qualifiedName = pkg.isEmpty() ? fileStem : pkg + "." + fileStem;

        // Chunk content based on file type
        List<Chunk> chunks;
        if (fileName.endsWith(".md") || fileName.endsWith(".mdx")) {
            chunks = chunkMarkdown(content);
        } else if (fileName.endsWith(".rst") || fileName.endsWith(".adoc")) {
            chunks = chunkRst(content);
        } else if (fileName.endsWith(".txt")) {
            chunks = chunkPlainText(content);
        } else if (fileName.endsWith(".yaml") || fileName.endsWith(".yml")) {
            chunks = chunkYaml(content, fileStem);
        } else {
            chunks = chunkGeneric(content, fileStem);
        }

        String absPath = file.toAbsolutePath().toString();
        String fileDescription = firstHeadingOrParagraph(content, fileName);
        int totalLines = content.split("\n", -1).length;

        // One ParsedClass per file: kind="document"
        ParsedClass fileClass = new ParsedClass(
                projectName, pkg, fileStem, qualifiedName,
                "document", null, List.of(),
                List.of(), "public", false, false,
                fileDescription, absPath, 1, totalLines
        );

        // One ParsedMethod per chunk (annotation __chunk__ signals doc type)
        List<ParsedMethod> methods = new ArrayList<>();
        for (int i = 0; i < chunks.size(); i++) {
            Chunk chunk = chunks.get(i);
            String chunkTitle = (chunk.title() != null && !chunk.title().isBlank())
                    ? sanitizeMethodName(chunk.title()) : "chunk_" + (i + 1);
            String chunkId = ParsedMethod.buildId(projectName, qualifiedName, chunkTitle, List.of());

            // Extract internal links as unresolved call references (knowledge graph edges)
            List<CallReference> links = (fileName.endsWith(".md") || fileName.endsWith(".mdx"))
                    ? extractMarkdownLinks(chunk.content(), qualifiedName + "#" + chunkTitle + "()")
                    : List.of();

            methods.add(new ParsedMethod(
                    chunkId, projectName, pkg,
                    fileStem, qualifiedName, chunkTitle,
                    chunk.breadcrumb(),     // signature = heading breadcrumb path
                    "section",
                    List.of(), List.of(),
                    chunk.content(),        // body = chunk text
                    fileDescription,        // javadoc = file-level description
                    List.of("__chunk__"),   // annotation signals chunk doc type
                    "public",
                    false, false, false, false,
                    List.of(),
                    links,
                    absPath,
                    chunk.startLine(), chunk.endLine()
            ));
        }

        return new ParsedFile(absPath, pkg, List.of(), List.of(fileClass), methods);
    }

    // -----------------------------------------------------------------------
    // Markdown chunking
    // -----------------------------------------------------------------------

    private static final Pattern MD_HEADING = Pattern.compile("^(#{1,6})\\s+(.+)$");

    private List<Chunk> chunkMarkdown(String content) {
        String[] lines = content.split("\n", -1);
        record HeadingPos(int lineIdx, int level, String text) {}
        List<HeadingPos> headings = new ArrayList<>();

        boolean inFence = false;
        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];
            if (line.startsWith("```") || line.startsWith("~~~")) {
                inFence = !inFence;
                continue;
            }
            if (!inFence) {
                Matcher m = MD_HEADING.matcher(line);
                if (m.matches()) {
                    headings.add(new HeadingPos(i, m.group(1).length(), m.group(2).trim()));
                }
            }
        }

        if (headings.isEmpty()) return chunkPlainText(content);

        List<Chunk> chunks = new ArrayList<>();
        String[] breadcrumbStack = new String[7]; // indices 1-6

        for (int h = 0; h < headings.size(); h++) {
            HeadingPos hp = headings.get(h);
            breadcrumbStack[hp.level()] = hp.text();
            // Clear deeper levels
            Arrays.fill(breadcrumbStack, hp.level() + 1, 7, null);

            int startBody = hp.lineIdx() + 1;
            int endBody = (h + 1 < headings.size()) ? headings.get(h + 1).lineIdx() : lines.length;

            String body = joinLines(lines, startBody, endBody).trim();
            String breadcrumb = buildBreadcrumb(breadcrumbStack, hp.level(), hp.text());

            chunks.add(new Chunk(hp.text(), breadcrumb, body, hp.lineIdx() + 1, endBody));
        }

        return chunks;
    }

    private static String buildBreadcrumb(String[] stack, int level, String current) {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i < level; i++) {
            if (stack[i] != null) {
                if (sb.length() > 0) sb.append(" > ");
                sb.append(stack[i]);
            }
        }
        if (sb.length() > 0) sb.append(" > ");
        sb.append(current);
        return sb.toString();
    }

    // -----------------------------------------------------------------------
    // RST / AsciiDoc chunking (underline-based headings)
    // -----------------------------------------------------------------------

    private static final String RST_CHARS = "=-~^\"'`#*+<>";

    private List<Chunk> chunkRst(String content) {
        String[] lines = content.split("\n", -1);
        record HeadingPos(int titleLine, int underLine, String text) {}
        List<HeadingPos> headings = new ArrayList<>();

        for (int i = 1; i < lines.length; i++) {
            String prev = lines[i - 1].trim();
            String curr = lines[i].trim();
            if (!prev.isEmpty() && !curr.isEmpty()
                    && curr.length() >= prev.length()
                    && curr.chars().allMatch(c -> RST_CHARS.indexOf(c) >= 0)
                    && curr.chars().distinct().count() == 1) {
                headings.add(new HeadingPos(i - 1, i, prev));
            }
        }

        if (headings.isEmpty()) return chunkPlainText(content);

        List<Chunk> chunks = new ArrayList<>();
        for (int h = 0; h < headings.size(); h++) {
            HeadingPos hp = headings.get(h);
            int startBody = hp.underLine() + 1;
            int endBody = (h + 1 < headings.size()) ? headings.get(h + 1).titleLine() : lines.length;
            String body = joinLines(lines, startBody, endBody).trim();
            chunks.add(new Chunk(hp.text(), hp.text(), body, hp.titleLine() + 1, endBody));
        }
        return chunks;
    }

    // -----------------------------------------------------------------------
    // Plain text chunking (paragraph-based)
    // -----------------------------------------------------------------------

    private List<Chunk> chunkPlainText(String content) {
        List<Chunk> chunks = new ArrayList<>();
        String[] lines = content.split("\n", -1);
        int i = 0, chunkNum = 1;

        while (i < lines.length) {
            // Skip leading blank lines
            while (i < lines.length && lines[i].isBlank()) i++;
            if (i >= lines.length) break;

            int start = i;
            // Split plain-text paragraphs on single blank line
            while (i < lines.length && (i - start) < 150) {
                if (lines[i].isBlank()) { i++; break; }
                i++;
            }

            String body = joinLines(lines, start, i).trim();
            if (body.isBlank()) continue;

            // Use first line as title
            String firstLine = lines[start].trim();
            String title = firstLine.length() > 60 ? firstLine.substring(0, 57) + "..." : firstLine;
            chunks.add(new Chunk("chunk_" + chunkNum, title, body, start + 1, i));
            chunkNum++;
        }

        if (chunks.isEmpty()) {
            chunks.add(new Chunk("chunk_1", "", content.trim(), 1, lines.length));
        }
        return chunks;
    }

    // -----------------------------------------------------------------------
    // YAML chunking (top-level keys as topic sections)
    // -----------------------------------------------------------------------

    private static final Pattern YAML_TOP_KEY = Pattern.compile("^([a-zA-Z_][a-zA-Z0-9_-]*)\\s*:");

    private List<Chunk> chunkYaml(String content, String fileStem) {
        String[] lines = content.split("\n", -1);
        record KeyPos(int line, String key) {}
        List<KeyPos> keys = new ArrayList<>();
        for (int i = 0; i < lines.length; i++) {
            Matcher m = YAML_TOP_KEY.matcher(lines[i]);
            if (m.find()) keys.add(new KeyPos(i, m.group(1)));
        }
        if (keys.isEmpty()) return chunkGeneric(content, fileStem);

        List<Chunk> chunks = new ArrayList<>();
        for (int k = 0; k < keys.size(); k++) {
            KeyPos kp = keys.get(k);
            int startBody = kp.line();
            int endBody = (k + 1 < keys.size()) ? keys.get(k + 1).line() : lines.length;
            String body = joinLines(lines, startBody, endBody).trim();
            chunks.add(new Chunk(kp.key(), fileStem + " > " + kp.key(), body, startBody + 1, endBody));
        }
        return chunks;
    }

    // -----------------------------------------------------------------------
    // Generic fallback: whole file as one chunk
    // -----------------------------------------------------------------------

    private List<Chunk> chunkGeneric(String content, String fileStem) {
        return List.of(new Chunk(fileStem, fileStem, content.trim(), 1,
                content.split("\n", -1).length));
    }

    // -----------------------------------------------------------------------
    // Markdown link extraction → CallReference (knowledge graph edges)
    // -----------------------------------------------------------------------

    private static final Pattern MD_LINK = Pattern.compile("\\[([^\\]]+)]\\(([^)]+)\\)");

    private List<CallReference> extractMarkdownLinks(String content, String callerFqn) {
        List<CallReference> refs = new ArrayList<>();
        String[] lines = content.split("\n", -1);
        for (int i = 0; i < lines.length; i++) {
            Matcher m = MD_LINK.matcher(lines[i]);
            while (m.find()) {
                String target = m.group(2).trim();
                // Skip external URLs and email links
                if (target.startsWith("http://") || target.startsWith("https://")
                        || target.startsWith("mailto:") || target.startsWith("ftp://")) {
                    continue;
                }
                // Derive simple name from target (strip path, anchor, extension)
                String filePart = target.contains("#") ? target.substring(0, target.indexOf('#')) : target;
                String anchor = target.contains("#") ? target.substring(target.indexOf('#') + 1) : null;
                String simpleName = filePart.isBlank()
                        ? (anchor != null ? anchor : target)
                        : stem(Paths.get(filePart).getFileName().toString());
                refs.add(CallReference.unresolved(callerFqn, simpleName, i + 1));
            }
        }
        return refs;
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static String stem(String filename) {
        int dot = filename.lastIndexOf('.');
        return (dot > 0) ? filename.substring(0, dot) : filename;
    }

    private static String joinLines(String[] lines, int start, int end) {
        StringBuilder sb = new StringBuilder();
        for (int i = start; i < end && i < lines.length; i++) {
            sb.append(lines[i]).append('\n');
        }
        return sb.toString();
    }

    /**
     * Sanitizes a heading text for use as a method name (Lucene document ID component).
     * Replaces special chars with underscores, truncates to 80 chars.
     */
    static String sanitizeMethodName(String heading) {
        String s = heading.replaceAll("[^a-zA-Z0-9_\\-. ]", "_")
                .replaceAll("\\s+", "_")
                .replaceAll("_+", "_")
                .replaceAll("^_|_$", "");
        return s.length() > 80 ? s.substring(0, 80) : s;
    }

    private static String firstHeadingOrParagraph(String content, String fileName) {
        // Try first Markdown heading
        for (String line : content.split("\n", -1)) {
            Matcher m = MD_HEADING.matcher(line);
            if (m.matches()) return m.group(2).trim();
        }
        // First non-blank line
        for (String line : content.split("\n", -1)) {
            if (!line.isBlank()) {
                String t = line.trim();
                return t.length() > 120 ? t.substring(0, 117) + "..." : t;
            }
        }
        return stem(fileName);
    }

    // -----------------------------------------------------------------------
    // Internal record for intermediate chunk representation
    // -----------------------------------------------------------------------

    private record Chunk(String title, String breadcrumb, String content, int startLine, int endLine) {}
}
