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

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

public class TarArchiveFileExistsTest extends AbstractTestWithTestDir {

    @Test
    public void should_return_file_existence_in_archive() throws Exception {
        Path archiveFile = testDir.resolve("test.tar");
        TarArchive archive = new TarArchive(archiveFile);

        createStagingFileWithContent("file1", "file1 content");
        createStagingFileWithContent("path/to/file2", "path/to/file2 content");

        // Archive the files
        archive.archiveFrom(stagingDir);

        // Check that the archive file exists
        assertThat(archiveFile).exists();
        assertThat(archive.isArchived()).isTrue();

        // Check that the files are archived
        assertThat(archive.fileExists("file1")).isTrue();
        assertThat(archive.fileExists("path/to/file2")).isTrue();
        assertThat(archive.fileExists("path/to/file3")).isFalse();
    }

    @Test
    public void should_return_false_if_the_archive_does_not_exist() {
        var archive = new TarArchive(Path.of("does-not-exist"));
        assertThat(archive.fileExists("xx")).isFalse();
    }
}
