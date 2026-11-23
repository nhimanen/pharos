package com.pharos.cluster.writer;

import com.pharos.cluster.ShardId;
import com.pharos.cluster.ShardSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.nio.file.attribute.FileTime;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Git runs against local repos in the temp dir: no network, never ~/.pharos. */
class SourceCheckoutTest {

    @TempDir Path tmp;

    private static String git(Path dir, String... args) throws Exception {
        String[] cmd = new String[args.length + 1];
        cmd[0] = "git";
        System.arraycopy(args, 0, cmd, 1, args.length);
        ProcessBuilder pb = new ProcessBuilder(cmd).directory(dir.toFile()).redirectErrorStream(true);
        pb.environment().put("GIT_CONFIG_GLOBAL", "/dev/null");
        pb.environment().put("GIT_CONFIG_SYSTEM", "/dev/null");
        pb.environment().put("GIT_AUTHOR_NAME", "t");
        pb.environment().put("GIT_AUTHOR_EMAIL", "t@example.org");
        pb.environment().put("GIT_COMMITTER_NAME", "t");
        pb.environment().put("GIT_COMMITTER_EMAIL", "t@example.org");
        Process p = pb.start();
        String out = new String(p.getInputStream().readAllBytes());
        assertThat(p.waitFor()).as("git %s: %s", String.join(" ", args), out).isZero();
        return out.trim();
    }

    /** A bare repo plus a working clone to commit from; returns the bare repo's path. */
    private Path bareRepoWithCommit(Path work) throws Exception {
        Path bare = tmp.resolve("origin.git");
        Files.createDirectories(bare);
        git(bare, "init", "--bare", "--initial-branch=main");
        Files.createDirectories(work);
        git(work, "init", "--initial-branch=main");
        Files.writeString(work.resolve("A.java"), "class A {}\n");
        git(work, "add", ".");
        git(work, "commit", "-m", "one");
        git(work, "push", bare.toString(), "main");
        return bare;
    }

    @Test
    void gitSourceClonesThenFetches() throws Exception {
        Path pusher = tmp.resolve("pusher");
        Path bare = bareRepoWithCommit(pusher);
        SourceCheckout checkout = new SourceCheckout(tmp.resolve("work"));
        ShardId shard = new ShardId("proj");
        ShardSource src = ShardSource.git(bare.toString(), "main");

        Path dir = checkout.prepare(shard, src);
        assertThat(dir).isEqualTo(tmp.resolve("work").resolve("proj").toAbsolutePath());
        assertThat(dir.resolve("A.java")).exists();
        assertThat(dir.resolve("B.java")).doesNotExist();

        Files.writeString(pusher.resolve("B.java"), "class B {}\n");
        git(pusher, "add", ".");
        git(pusher, "commit", "-m", "two");
        git(pusher, "push", bare.toString(), "main");

        assertThat(checkout.prepare(shard, src)).isEqualTo(dir);
        assertThat(dir.resolve("B.java")).exists();
        assertThat(checkout.checkedOutRevision(shard)).contains(git(pusher, "rev-parse", "HEAD"));

        // a tag and a commit id resolve too
        git(pusher, "tag", "v1");
        git(pusher, "push", bare.toString(), "v1");
        checkout.prepare(shard, ShardSource.git(bare.toString(), "v1"));
        checkout.prepare(shard, ShardSource.git(bare.toString(), git(pusher, "rev-parse", "HEAD~1")));
        assertThat(dir.resolve("B.java")).doesNotExist();
        assertThatThrownBy(() -> checkout.prepare(shard, ShardSource.git(bare.toString(), "nope")))
                .isInstanceOf(IOException.class).hasMessageContaining("not found");
    }

    @Test
    void pathSourceUsesDirectory() throws Exception {
        Path shared = Files.createDirectories(tmp.resolve("shared"));
        Files.writeString(shared.resolve("A.java"), "class A {}\n");
        SourceCheckout checkout = new SourceCheckout(tmp.resolve("work"));
        ShardId shard = new ShardId("p");

        assertThat(checkout.prepare(shard, ShardSource.path(shared.toString()))).isEqualTo(shared.toAbsolutePath());
        assertThat(tmp.resolve("work")).doesNotExist();
        assertThatThrownBy(() -> checkout.prepare(shard, ShardSource.path(tmp.resolve("missing").toString())))
                .isInstanceOf(IOException.class).hasMessageContaining("not a readable directory");
    }

    @Test
    void changeDetectionByRevision() throws Exception {
        Path pusher = tmp.resolve("pusher");
        Path bare = bareRepoWithCommit(pusher);
        SourceCheckout checkout = new SourceCheckout(tmp.resolve("work"));
        ShardId shard = new ShardId("g");
        assertThatThrownBy(() -> checkout.changedSince(shard, null)).isInstanceOf(IllegalStateException.class);
        checkout.prepare(shard, ShardSource.git(bare.toString(), "main"));
        String rev1 = git(pusher, "rev-parse", "HEAD");

        assertThat(checkout.changedSince(shard, null)).contains(rev1);
        assertThat(checkout.changedSince(shard, rev1)).isEmpty();

        Files.writeString(pusher.resolve("B.java"), "class B {}\n");
        git(pusher, "add", ".");
        git(pusher, "commit", "-m", "two");
        git(pusher, "push", bare.toString(), "main");
        String rev2 = git(pusher, "rev-parse", "HEAD");
        assertThat(checkout.changedSince(shard, rev1)).contains(rev2);
        assertThat(checkout.changedSince(shard, rev2)).isEmpty();

        // path source: any file change moves the revision
        Path shared = Files.createDirectories(tmp.resolve("shared"));
        Path f = shared.resolve("A.java");
        Files.writeString(f, "class A {}\n");
        ShardId ps = new ShardId("p");
        checkout.prepare(ps, ShardSource.path(shared.toString()));
        String r1 = checkout.changedSince(ps, null).orElseThrow();
        assertThat(checkout.changedSince(ps, r1)).isEmpty();
        Files.setLastModifiedTime(f, FileTime.fromMillis(System.currentTimeMillis() + 10_000));
        Optional<String> r2 = checkout.changedSince(ps, r1);
        assertThat(r2).isPresent().isNotEqualTo(Optional.of(r1));
    }

    @Test
    void refusesUnrelatedWorkDirectory() throws Exception {
        Path bare = bareRepoWithCommit(tmp.resolve("pusher"));
        Files.createDirectories(tmp.resolve("work/proj"));
        assertThatThrownBy(() -> new SourceCheckout(tmp.resolve("work"))
                .prepare(new ShardId("proj"), ShardSource.git(bare.toString(), "main")))
                .isInstanceOf(IOException.class).hasMessageContaining("not a git clone");
    }

    @Test
    void hungGitTimesOutPromptly() throws Exception {
        Path fake = tmp.resolve("fake-git");
        Files.writeString(fake, "#!/bin/sh\nsleep 30\n");
        assertThat(fake.toFile().setExecutable(true)).isTrue();
        SourceCheckout checkout = new SourceCheckout(tmp.resolve("work"),
                java.time.Duration.ofMillis(300), fake.toString());
        long t0 = System.nanoTime();
        assertThatThrownBy(() -> checkout.prepare(new ShardId("h"),
                ShardSource.git("https://example.org/secret-host/r.git", "main")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("timed out")
                .hasMessageNotContaining("secret-host");
        assertThat(java.time.Duration.ofNanos(System.nanoTime() - t0)).isLessThan(java.time.Duration.ofSeconds(10));
    }

    /**
     * A git that delegates to the real one, except that {@code ls-remote} hangs in a child process
     * holding the output pipes (what a {@code git-remote-https} or {@code ssh} helper does on a
     * black-holed remote); the child's pid goes to {@code pidFile}.
     */
    static Path hangingLsRemoteGit(Path dir, Path pidFile) throws IOException {
        Path fake = dir.resolve("hanging-ls-remote-git");
        Files.writeString(fake, "#!/bin/sh\nif [ \"$1\" = ls-remote ]; then\n  sleep 300 &\n  echo $! > '"
                + pidFile + "'\n  wait\nfi\nexec git \"$@\"\n");
        assertThat(fake.toFile().setExecutable(true)).isTrue();
        return fake;
    }

    /** Final review I4: ls-remote has its own short timeout, and the whole process tree is killed. */
    @Test
    void hangingLsRemoteTimesOutAndKillsItsProcessTree() throws Exception {
        Path bare = bareRepoWithCommit(tmp.resolve("pusher"));
        Path pidFile = tmp.resolve("child.pid");
        SourceCheckout checkout = new SourceCheckout(tmp.resolve("work"), Duration.ofSeconds(60),
                Duration.ofMillis(500), hangingLsRemoteGit(tmp, pidFile).toString());
        ShardId shard = new ShardId("dead");
        checkout.prepare(shard, ShardSource.git(bare.toString(), "main")); // clone and fetch: the real git

        long t0 = System.nanoTime();
        assertThatThrownBy(() -> checkout.changedSince(shard, null))
                .isInstanceOf(IOException.class).hasMessageContaining("ls-remote timed out after 500 ms");
        assertThat(Duration.ofNanos(System.nanoTime() - t0)).as("its own timeout, not the 60 s one")
                .isLessThan(Duration.ofSeconds(15));
        long child = Long.parseLong(Files.readString(pidFile).trim());
        long until = System.currentTimeMillis() + 5_000;
        while (ProcessHandle.of(child).map(ProcessHandle::isAlive).orElse(false) && System.currentTimeMillis() < until) {
            Thread.sleep(20);
        }
        assertThat(ProcessHandle.of(child).map(ProcessHandle::isAlive).orElse(false))
                .as("the helper holding the pipes is killed with git").isFalse();
    }

    @Test
    void pathRevisionIgnoresDirectoriesTheIndexSkips() throws Exception {
        Path shared = Files.createDirectories(tmp.resolve("shared"));
        Files.writeString(shared.resolve("A.java"), "class A {}\n");
        Path nm = Files.createDirectories(shared.resolve("node_modules/dep"));
        Path hidden = Files.createDirectories(shared.resolve("sub/.git"));
        SourceCheckout checkout = new SourceCheckout(tmp.resolve("work"));
        ShardId shard = new ShardId("p");
        checkout.prepare(shard, ShardSource.path(shared.toString()));
        String r1 = checkout.changedSince(shard, null).orElseThrow();

        Files.writeString(nm.resolve("x.js"), "1");
        Files.writeString(hidden.resolve("HEAD"), "ref");
        Files.writeString(Files.createDirectories(shared.resolve("target")).resolve("o.class"), "x");
        assertThat(checkout.changedSince(shard, r1)).as("excluded dirs").isEmpty();

        Files.writeString(shared.resolve("B.java"), "class B {}\n");
        assertThat(checkout.changedSince(shard, r1)).isPresent();
    }

    @Test
    void unreadablePathSurfacesAsIOException() throws Exception {
        Path shared = Files.createDirectories(tmp.resolve("shared"));
        Path locked = Files.createDirectories(shared.resolve("locked"));
        Files.writeString(locked.resolve("A.java"), "x");
        SourceCheckout checkout = new SourceCheckout(tmp.resolve("work"));
        ShardId shard = new ShardId("p");
        checkout.prepare(shard, ShardSource.path(shared.toString()));
        Files.setPosixFilePermissions(locked, java.util.Set.of());
        try {
            org.junit.jupiter.api.Assumptions.assumeFalse(Files.isReadable(locked), "running as a user that ignores modes");
            assertThatThrownBy(() -> checkout.changedSince(shard, null)).isInstanceOf(IOException.class);
        } finally {
            Files.setPosixFilePermissions(locked, java.nio.file.attribute.PosixFilePermissions.fromString("rwxr-xr-x"));
        }
        Files.delete(locked.resolve("A.java"));
        Files.delete(shared.resolve("locked"));
        Files.delete(shared);
        checkout.prepare(shard, ShardSource.path(tmp.toString()));
        // the shared directory vanishing is an IOException too
        Path gone = tmp.resolve("gone");
        Files.createDirectory(gone);
        checkout.prepare(shard, ShardSource.path(gone.toString()));
        Files.delete(gone);
        assertThatThrownBy(() -> checkout.changedSince(shard, null)).isInstanceOf(IOException.class);
    }
    /** A credentials directory as Kubernetes mounts a Secret: each key a file (here plain files). */
    private Path secretDir(String name, java.util.Map<String, String> files) throws IOException {
        Path dir = Files.createDirectories(tmp.resolve("git-secrets").resolve(name));
        for (var e : files.entrySet()) Files.writeString(dir.resolve(e.getKey()), e.getValue());
        return dir;
    }

    /** The helper really answers git: {@code git credential fill} gets the files' values, newline-trimmed. */
    @Test
    void httpsCredentialsAreReadFromTheShardsDirectoryByGit() throws Exception {
        Path dir = secretDir("it's", java.util.Map.of("username", "deploy\n", "password", "s3cr3t-t0ken\n"));
        java.util.Map<String, String> env = SourceCheckout.credentialEnv(dir, tmp.resolve("work"));
        assertThat(String.join("\n", env.values())).as("paths only, never a credential").doesNotContain("s3cr3t");
        assertThat(env).doesNotContainKey("GIT_SSH_COMMAND");

        ProcessBuilder pb = new ProcessBuilder("git", "credential", "fill").directory(tmp.toFile());
        pb.environment().put("GIT_CONFIG_GLOBAL", "/dev/null");
        pb.environment().put("GIT_CONFIG_SYSTEM", "/dev/null");
        pb.environment().put("GIT_TERMINAL_PROMPT", "0");
        pb.environment().putAll(env);
        Process p = pb.start();
        p.getOutputStream().write("protocol=https\nhost=example.org\npath=org/repo.git\n\n".getBytes());
        p.getOutputStream().close();
        String out = new String(p.getInputStream().readAllBytes());
        assertThat(p.waitFor()).as(new String(p.getErrorStream().readAllBytes())).isZero();
        assertThat(out).contains("username=deploy\n").contains("password=s3cr3t-t0ken\n");
    }

    @Test
    void sshKeyGivesAGitSshCommandWithThatKeyOnly() throws Exception {
        Path work = tmp.resolve("work");
        Path tofu = secretDir("tofu", java.util.Map.of("ssh-privatekey", "KEY"));
        assertThat(SourceCheckout.credentialEnv(tofu, work)).containsOnlyKeys("GIT_SSH_COMMAND")
                .containsEntry("GIT_SSH_COMMAND", "ssh -i '" + tofu.resolve("ssh-privatekey")
                        + "' -o IdentitiesOnly=yes -o BatchMode=yes -o UserKnownHostsFile='"
                        + work.resolve(".ssh-known-hosts") + "' -o StrictHostKeyChecking=accept-new");
        Path pinned = secretDir("pinned", java.util.Map.of("ssh-privatekey", "KEY", "known_hosts", "h ssh-ed25519 AAA"));
        assertThat(SourceCheckout.credentialEnv(pinned, work).get("GIT_SSH_COMMAND"))
                .contains("-o UserKnownHostsFile='" + pinned.resolve("known_hosts") + "' -o StrictHostKeyChecking=yes");
        assertThat(SourceCheckout.credentialEnv(secretDir("empty", java.util.Map.of()), work)).isEmpty();
        assertThat(SourceCheckout.credentialEnv(tmp.resolve("missing"), work)).as("Secret not mounted").isEmpty();
        assertThat(SourceCheckout.shellQuote("a'b")).isEqualTo("'a'\\''b'");
    }

    @Test
    void onlyTheShardsOwnCredentialsAreOffered() throws Exception {
        Path a = secretDir("a", java.util.Map.of("ssh-privatekey", "KEY"));
        SourceCheckout checkout = new SourceCheckout(tmp.resolve("work"), Duration.ofSeconds(5),
                java.util.Map.of("a", a));
        assertThat(checkout.envFor(new ShardId("a")).get("GIT_SSH_COMMAND")).contains(a.toString());
        assertThat(checkout.envFor(new ShardId("b"))).isEmpty();
    }

    /**
     * Integration carry I3a: gitCredentials is keyed by project; a split project's shards carry keyed ids
     * ({@code p~ring=s01}), so the lookup goes through the project mapping, not the id string.
     */
    @Test
    void aKeyedShardGetsItsProjectsCredentials() throws Exception {
        Path p = secretDir("p", java.util.Map.of("ssh-privatekey", "KEY"));
        SourceCheckout checkout = new SourceCheckout(tmp.resolve("work"), Duration.ofSeconds(5),
                java.util.Map.of("p", p));
        ShardId keyed = ShardId.keyed("p", new java.util.LinkedHashMap<>(java.util.Map.of("ring", "s01")));
        ShardId other = ShardId.keyed("q", new java.util.LinkedHashMap<>(java.util.Map.of("ring", "s01")));
        assertThat(keyed.value()).isEqualTo("p~ring=s01");
        assertThat(checkout.envFor(keyed)).as("no mapping: a keyed id names no project").isEmpty();

        com.pharos.cluster.sharding.ProjectShards map = com.pharos.cluster.sharding.ProjectShards.fromCatalog(
                new com.fasterxml.jackson.databind.ObjectMapper().readTree("""
                        {"shards":[{"id":"p~ring=s01"},{"id":"q~ring=s01"}],
                         "specs":[{"version":1,"project":"p","spec":"[project, ring(path, shards=1)]"},
                                  {"version":2,"project":"q","spec":"[project, ring(path, shards=1)]"}],
                         "projects":[{"name":"p","spec":1,"shards":["p~ring=s01"]},
                                     {"name":"q","spec":2,"shards":["q~ring=s01"]}]}"""),
                java.util.List.of(keyed, other), m -> {});
        checkout.useProjects(map::projectOf);
        assertThat(checkout.envFor(keyed).get("GIT_SSH_COMMAND")).contains(p.toString());
        assertThat(checkout.envFor(other)).as("another project's shard").isEmpty();
        assertThat(checkout.envFor(ShardId.ofProject("p")).get("GIT_SSH_COMMAND")).contains(p.toString());
    }
}
