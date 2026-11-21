package com.pharos.cluster.writer;

import com.pharos.cluster.ShardId;
import com.pharos.cluster.ShardSource;
import com.pharos.indexer.ProjectIndexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Gives a writer a shard's source tree: the shared directory of a {@code path} source, or a clone
 * kept under {@code <workDir>/<shard>} for a {@code git} source (cloned on first use, fetched before
 * every {@link #prepare}). Git runs as a subprocess with the writer's own environment, which is where
 * credentials come from (credential helper, ssh agent, {@code GIT_ASKPASS}); a prompt never blocks it.
 * Nothing here writes a URL or credential anywhere but the clone's own git config.
 */
public final class SourceCheckout {

    private static final Logger log = LoggerFactory.getLogger(SourceCheckout.class);
    private static final Pattern SHA = Pattern.compile("[0-9a-fA-F]{40}([0-9a-fA-F]{24})?");
    static final Duration DEFAULT_GIT_TIMEOUT = Duration.ofMinutes(10);

    private final Path workDir;
    private final Duration gitTimeout;
    private final String gitExecutable;
    private final Map<ShardId, ShardSource> prepared = new ConcurrentHashMap<>();

    public SourceCheckout(Path workDir) {
        this(workDir, DEFAULT_GIT_TIMEOUT);
    }

    SourceCheckout(Path workDir, Duration gitTimeout) {
        this(workDir, gitTimeout, "git");
    }

    /** Tests only: another git executable (a fake that hangs). */
    SourceCheckout(Path workDir, Duration gitTimeout, String gitExecutable) {
        this.gitExecutable = gitExecutable;
        this.workDir = Objects.requireNonNull(workDir, "workDir").toAbsolutePath().normalize();
        this.gitTimeout = gitTimeout;
    }

    /** Where a git shard's clone lives. */
    public Path cloneDir(ShardId shard) {
        return workDir.resolve(shard.value());
    }

    /**
     * The directory to index: the {@code path} source's directory as it is, or the git clone fetched
     * and checked out (detached) at the source's ref. Remembers the source for {@link #changedSince}.
     *
     * @throws IOException when the directory is missing, or git fails (the message names the shard,
     *                     never the url's credentials, which a {@link ShardSource} cannot hold)
     */
    public Path prepare(ShardId shard, ShardSource source) throws IOException {
        Objects.requireNonNull(shard, "shard");
        Objects.requireNonNull(source, "source");
        Path dir;
        if (source.kind() == ShardSource.Kind.PATH) {
            dir = Path.of(source.location()).toAbsolutePath().normalize();
            if (!Files.isDirectory(dir)) {
                throw new IOException("source path of shard " + shard + " is not a readable directory: " + dir);
            }
        } else {
            dir = checkoutGit(shard, source);
        }
        prepared.put(shard, source);
        return dir;
    }

    /** The commit a git shard's clone is at; empty for a path source or before {@link #prepare}. */
    public Optional<String> checkedOutRevision(ShardId shard) throws IOException {
        ShardSource src = prepared.get(shard);
        if (src == null || src.kind() != ShardSource.Kind.GIT) return Optional.empty();
        return Optional.of(git(cloneDir(shard), "rev-parse", "HEAD").trim());
    }

    /** Whether {@link #prepare} checked out this shard (its source is known here). */
    public boolean isPrepared(ShardId shard) {
        return prepared.containsKey(shard);
    }

    /**
     * The shard's source revision when it differs from {@code lastRevision} (null means never indexed),
     * else empty. Git: {@code git ls-remote} of the source url, no fetch, the clone untouched. Path: a
     * digest of the tree's file names, sizes and modification times. Read-only, so it is safe while a
     * run indexes the checkout. Requires an earlier {@link #prepare} of the shard.
     */
    public Optional<String> changedSince(ShardId shard, String lastRevision) throws IOException {
        ShardSource src = prepared.get(shard);
        if (src == null) throw new IllegalStateException("shard " + shard + " has no prepared source");
        String now = src.kind() == ShardSource.Kind.GIT ? remoteRevision(shard, src) : pathRevision(src);
        return now.equals(lastRevision) ? Optional.empty() : Optional.of(now);
    }

    // ------------------------------------------------------------------ git

    private Path checkoutGit(ShardId shard, ShardSource src) throws IOException {
        Path dir = cloneDir(shard);
        Files.createDirectories(workDir);
        if (!Files.exists(dir)) {
            log.info("Cloning source of shard {}", shard);
            git(workDir, "clone", "--no-checkout", "--quiet", "--", src.location(), shard.value());
        } else if (!Files.isDirectory(dir.resolve(".git"))) {
            throw new IOException("work directory " + dir + " exists but is not a git clone; remove it to let"
                    + " the writer clone shard " + shard + " again");
        } else {
            git(dir, "remote", "set-url", "origin", src.location());
        }
        git(dir, "fetch", "--quiet", "--prune", "--force", "--tags", "origin",
                "+refs/heads/*:refs/remotes/origin/*");
        String rev = resolve(dir, src.ref());
        git(dir, "checkout", "--quiet", "--force", "--detach", rev);
        git(dir, "clean", "--quiet", "-fdx");
        return dir;
    }

    /** A branch (as fetched from origin), a tag, or a commit id, in that order. */
    private String resolve(Path dir, String ref) throws IOException {
        for (String candidate : List.of("refs/remotes/origin/" + ref, "refs/tags/" + ref, ref)) {
            Optional<String> sha = tryGit(dir, "rev-parse", "--verify", "--quiet", candidate + "^{commit}");
            if (sha.isPresent()) return sha.get().trim();
        }
        throw new IOException("ref '" + ref + "' not found in the source of this shard");
    }

    private String remoteRevision(ShardId shard, ShardSource src) throws IOException {
        if (SHA.matcher(src.ref()).matches()) return src.ref().toLowerCase(java.util.Locale.ROOT);
        String ref = src.ref();
        // the url, not the clone's origin: read-only, and independent of a checkout a run is indexing
        Files.createDirectories(workDir);
        String out = git(workDir, "ls-remote", "--", src.location(), ref, ref + "^{}");
        String head = null, tag = null, peeledTag = null;
        for (String line : out.split("\n")) {
            String[] f = line.trim().split("\\s+");
            if (f.length != 2) continue;
            if (f[1].equals("refs/heads/" + ref)) head = f[0];
            else if (f[1].equals("refs/tags/" + ref)) tag = f[0];
            else if (f[1].equals("refs/tags/" + ref + "^{}")) peeledTag = f[0];
        }
        String rev = head != null ? head : peeledTag != null ? peeledTag : tag;
        if (rev == null) throw new IOException("ref '" + ref + "' not found at the remote of shard " + shard);
        return rev;
    }

    private Optional<String> tryGit(Path dir, String... args) throws IOException {
        try {
            return Optional.of(git(dir, args));
        } catch (GitFailed e) {
            return Optional.empty();
        }
    }

    private static final class GitFailed extends IOException {
        GitFailed(String m) { super(m); }
    }

    private String git(Path dir, String... args) throws IOException {
        List<String> cmd = new ArrayList<>();
        cmd.add(gitExecutable);
        cmd.addAll(List.of(args));
        ProcessBuilder pb = new ProcessBuilder(cmd).directory(dir.toFile());
        Map<String, String> env = pb.environment();
        env.put("GIT_TERMINAL_PROMPT", "0");
        env.put("GIT_ALLOW_PROTOCOL", "file:git:http:https:ssh");
        env.put("LC_ALL", "C");
        Process p = pb.start();
        p.getOutputStream().close();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        ByteArrayOutputStream outBuf = new ByteArrayOutputStream();
        Thread errPump = Thread.ofVirtual().start(() -> drain(p.getErrorStream(), err));
        Thread outPump = Thread.ofVirtual().start(() -> drain(p.getInputStream(), outBuf));
        try {
            if (!p.waitFor(gitTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
                p.destroyForcibly();
                p.waitFor(5, TimeUnit.SECONDS);
                // the operation only: the command line may carry the source's url
                throw new IOException("git " + args[0] + " timed out after " + gitTimeout.toMillis() + " ms");
            }
            errPump.join(2000);
            outPump.join(2000);
        } catch (InterruptedException e) {
            p.destroyForcibly();
            Thread.currentThread().interrupt();
            throw new java.io.InterruptedIOException("interrupted while running git " + args[0]);
        }
        byte[] out = outBuf.toByteArray();
        if (p.exitValue() != 0) {
            String msg = err.toString(StandardCharsets.UTF_8).trim();
            // the command line is left out: it may carry the source's url
            throw new GitFailed("git " + args[0] + " failed (exit " + p.exitValue() + ")"
                    + (msg.isEmpty() ? "" : ": " + msg));
        }
        return new String(out, StandardCharsets.UTF_8);
    }

    private static void drain(InputStream in, ByteArrayOutputStream sink) {
        try { in.transferTo(sink); } catch (IOException ignored) { }
    }

    // ----------------------------------------------------------------- path

    private static String pathRevision(ShardSource src) throws IOException {
        Path root = Path.of(src.location()).toAbsolutePath().normalize();
        if (!Files.isDirectory(root)) throw new IOException("source path is not a readable directory: " + root);
        List<String> lines = new ArrayList<>();
        // the index walk's own skip rules, so a change the index would not see is no change
        Files.walkFileTree(root, new java.nio.file.SimpleFileVisitor<>() {
            @Override
            public java.nio.file.FileVisitResult preVisitDirectory(Path dir, java.nio.file.attribute.BasicFileAttributes a) {
                String name = dir.getFileName() != null ? dir.getFileName().toString() : "";
                return !dir.equals(root) && ProjectIndexManager.isSkippedDir(name)
                        ? java.nio.file.FileVisitResult.SKIP_SUBTREE : java.nio.file.FileVisitResult.CONTINUE;
            }

            @Override
            public java.nio.file.FileVisitResult visitFile(Path f, java.nio.file.attribute.BasicFileAttributes a) {
                lines.add(root.relativize(f) + "\t" + a.size() + "\t" + a.lastModifiedTime().toMillis());
                return java.nio.file.FileVisitResult.CONTINUE;
            }
        });
        java.util.Collections.sort(lines);
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            for (String l : lines) md.update((l + "\n").getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(md.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
