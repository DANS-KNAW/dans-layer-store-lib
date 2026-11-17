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

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ArchiveBackupTest extends AbstractTestWithTestDir {

    @Test
    public void zipArchive_should_restore_backup_on_failure() throws Exception {
        var archiveFile = testDir.resolve("test.zip");
        Files.writeString(archiveFile, "original content");
        var archive = new ZipArchive(archiveFile);

        Files.createDirectories(stagingDir.getParent());
        Files.createDirectory(stagingDir);
        Assumptions.assumeTrue(stagingDir.toFile().setReadable(false), "setReadable not supported");

        try {
            assertThatThrownBy(() -> archive.archiveFrom(stagingDir))
                .isInstanceOf(Exception.class);

            assertThat(Files.readString(archiveFile)).isEqualTo("original content");
            assertThat(Files.exists(testDir.resolve("test.zip.bak"))).isFalse();
        }
        finally {
            if (!stagingDir.toFile().setReadable(true)) {
                System.err.println("Failed to set staging directory readable");
            }
        }
    }

    @Test
    public void tarArchive_should_restore_backup_on_failure() throws Exception {
        var archiveFile = testDir.resolve("test.tar");
        Files.writeString(archiveFile, "original content");
        var archive = new TarArchive(archiveFile);

        Files.createDirectories(stagingDir.getParent());
        Files.createDirectory(stagingDir);
        Assumptions.assumeTrue(stagingDir.toFile().setReadable(false), "setReadable not supported");

        try {
            assertThatThrownBy(() -> archive.archiveFrom(stagingDir))
                .isInstanceOf(Exception.class);

            assertThat(Files.readString(archiveFile)).isEqualTo("original content");
            assertThat(Files.exists(testDir.resolve("test.tar.bak"))).isFalse();
        }
        finally {
            if (!stagingDir.toFile().setReadable(true)) {
                System.err.println("Failed to set staging directory readable");
            }
        }
    }
}
