/*
 * Copyright (C) 2024 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.layerstore;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the package-upload-verify-install flow with fake dmftar, rsync and ssh scripts. The "remote" archive root is a local directory.
 */
public class DmfTarArchiveFromTest extends AbstractTestWithTestDir {
    private static final String ARCHIVE = "1234567890123.dmftar";

    private Path bin;
    private Path remoteRoot;
    private Path packagingDir;
    private Path rsyncFailures;
    private Path rsyncCalls;
    private Path invalidFlag;

    @BeforeEach
    @Override
    public void setUp() throws Exception {
        super.setUp();
        bin = testDir.resolve("bin").toAbsolutePath();
        remoteRoot = testDir.resolve("remote").toAbsolutePath();
        packagingDir = testDir.resolve("packaging").toAbsolutePath();
        rsyncFailures = testDir.resolve("rsync-failures").toAbsolutePath();
        rsyncCalls = testDir.resolve("rsync-calls").toAbsolutePath();
        invalidFlag = testDir.resolve("invalid").toAbsolutePath();
        Files.createDirectories(bin);
        Files.createDirectories(remoteRoot);
        Files.writeString(rsyncFailures, "0");
        Files.writeString(rsyncCalls, "");
        createStagingFileWithContent("a/b.txt", "content");

        script("dmftar", """
            #!/bin/sh
            # usage: dmftar -cf <target> .
            mkdir -p "$2" && cp -r . "$2"
            """);
        script("rsync", """
            #!/bin/sh
            echo "$@" >> %s
            n=$(cat %s)
            if [ "$n" -gt 0 ]; then
              echo $((n - 1)) > %s
              echo "ssh: connect to host dummyhost port 22: Connection refused" >&2
              exit 255
            fi
            for last; do :; done
            src=$(eval echo \\${$(($# - 1))})
            cp -r "$src" "${last#*:}"
            """.formatted(rsyncCalls, rsyncFailures, rsyncFailures));
        script("ssh", """
            #!/bin/sh
            for last; do :; done
            case "$last" in
              "dmftar --verify"*)
                if [ -e %s ]; then echo "checksum failure" >&2; exit 1; fi
                exit 0;;
            esac
            exec sh -c "$last"
            """.formatted(invalidFlag));
    }

    private void script(String name, String content) throws IOException {
        var file = bin.resolve(name);
        Files.writeString(file, content);
        assertThat(file.toFile().setExecutable(true)).isTrue();
    }

    private DmfTarArchive archive(boolean archived, int maxRetries, Duration moratorium) {
        var manager = new MoratoriumManager("Connection refused", moratorium);
        var ssh = new SshRunner(bin.resolve("ssh"), "user", "dummyhost", remoteRoot, 10, manager);
        var rsync = new RsyncRunner(bin.resolve("rsync"), List.of("-av", "--partial", "--append"), "user", "dummyhost", remoteRoot, manager);
        var runner = new DmfTarRunner(bin.resolve("dmftar"), ssh, rsync, "user", "dummyhost", remoteRoot, manager);
        var options = new DmfTarArchiveOptions(packagingDir, maxRetries, Duration.ofMillis(10), true, "checksum failure");
        return new DmfTarArchive(runner, options, ARCHIVE, archived);
    }

    @Test
    public void archiveFrom_should_package_upload_and_install() throws Exception {
        var archive = archive(false, 0, Duration.ofMillis(10));

        archive.archiveFrom(stagingDir);

        assertThat(remoteRoot.resolve(ARCHIVE).resolve("a/b.txt")).hasContent("content");
        assertThat(remoteRoot.resolve(".incoming")).isEmptyDirectory();
        assertThat(packagingDir).isEmptyDirectory();
        assertThat(archive.isArchived()).isTrue();
    }

    @Test
    public void archiveFrom_should_retry_same_rsync_command_and_wait_for_moratorium() throws Exception {
        Files.writeString(rsyncFailures, "2");
        var archive = archive(false, 5, Duration.ofMillis(300));
        var start = System.nanoTime();

        archive.archiveFrom(stagingDir);

        assertThat(Duration.ofNanos(System.nanoTime() - start)).isGreaterThanOrEqualTo(Duration.ofMillis(300));
        var calls = Files.readAllLines(rsyncCalls);
        assertThat(calls).hasSize(3);
        assertThat(calls).containsOnly(calls.get(0));
        assertThat(remoteRoot.resolve(ARCHIVE).resolve("a/b.txt")).hasContent("content");
    }

    @Test
    public void archiveFrom_should_give_up_after_max_retries_and_keep_local_package() throws Exception {
        Files.writeString(rsyncFailures, "100");
        var archive = archive(false, 1, Duration.ofMillis(10));

        assertThatThrownBy(() -> archive.archiveFrom(stagingDir)).isInstanceOf(RemoteCommandException.class);

        assertThat(Files.readAllLines(rsyncCalls)).hasSize(2);
        assertThat(remoteRoot.resolve(ARCHIVE)).doesNotExist();
        assertThat(packagingDir.resolve(ARCHIVE)).exists();
        assertThat(archive.isArchived()).isFalse();
    }

    @Test
    public void archiveFrom_should_reuse_complete_local_package_on_next_attempt() throws Exception {
        Files.writeString(rsyncFailures, "1");
        var failing = archive(false, 0, Duration.ofMillis(10));
        assertThatThrownBy(() -> failing.archiveFrom(stagingDir)).isInstanceOf(RemoteCommandException.class);
        Files.writeString(packagingDir.resolve(ARCHIVE).resolve("a/b.txt"), "marker-from-first-package");
        Thread.sleep(20);

        archive(false, 0, Duration.ofMillis(10)).archiveFrom(stagingDir);

        assertThat(remoteRoot.resolve(ARCHIVE).resolve("a/b.txt")).hasContent("marker-from-first-package");
    }

    @Test
    public void archiveFrom_should_not_retry_and_not_install_invalid_archive() throws Exception {
        Files.writeString(invalidFlag, "");
        var archive = archive(false, 5, Duration.ofMillis(10));

        assertThatThrownBy(() -> archive.archiveFrom(stagingDir)).isInstanceOf(InvalidArchiveException.class);

        assertThat(Files.readAllLines(rsyncCalls)).hasSize(1);
        assertThat(remoteRoot.resolve(ARCHIVE)).doesNotExist();
        assertThat(remoteRoot.resolve(".incoming").resolve(ARCHIVE)).doesNotExist();
        assertThat(packagingDir.resolve(ARCHIVE)).doesNotExist();
    }

    @Test
    public void archiveFrom_should_replace_existing_archive() throws Exception {
        FileUtils.write(remoteRoot.resolve(ARCHIVE).resolve("old.txt").toFile(), "old", "UTF-8");
        var archive = archive(true, 0, Duration.ofMillis(10));

        archive.archiveFrom(stagingDir);

        assertThat(remoteRoot.resolve(ARCHIVE).resolve("a/b.txt")).hasContent("content");
        assertThat(remoteRoot.resolve(ARCHIVE).resolve("old.txt")).doesNotExist();
        assertThat(remoteRoot.resolve(ARCHIVE + ".bak")).doesNotExist();
    }

    @Test
    public void archiveFrom_should_keep_old_archive_when_upload_fails() throws Exception {
        FileUtils.write(remoteRoot.resolve(ARCHIVE).resolve("old.txt").toFile(), "old", "UTF-8");
        Files.writeString(rsyncFailures, "100");
        var archive = archive(true, 0, Duration.ofMillis(10));

        assertThatThrownBy(() -> archive.archiveFrom(stagingDir)).isInstanceOf(RemoteCommandException.class);

        assertThat(remoteRoot.resolve(ARCHIVE).resolve("old.txt")).hasContent("old");
    }
}
