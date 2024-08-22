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

import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ZipArchiveReadFileTest extends AbstractTestWithTestDir {
    Path zipFile = testDir.resolve("test.zip");

    @Test
    public void should_return_content_of_file_in_archive() throws Exception {
        ZipArchive archive = new ZipArchive(zipFile);

        createStagingFileWithContent("file1", "file1 content");
        createStagingFileWithContent("path/to/file2", "path/to/file2 content");
        createStagingFileWithContent("path/to/file3", "path/to/file3 content");
        createStagingFileWithContent("another/path/to/file2", "another/path/to/file2 content");

        // Archive the files
        archive.archiveFrom(stagingDir);

        // Check that the zip file exists
        assertThat(zipFile).exists();
        assertThat(archive.isArchived()).isTrue();

        // Read the content of a file in the archive
        try (var inputStream = archive.readFile("path/to/file2")) {
            assertThat(inputStream.readAllBytes())
                .isEqualTo("path/to/file2 content".getBytes());
        }
    }

    @Test
    @SuppressWarnings("resource") // for assertThatThrownBy
    public void should_throw_when_reading_file_not_in_archive() throws Exception {
        ZipArchive archive = new ZipArchive(zipFile);

        createStagingFileWithContent("file1", "file1 content");
        createStagingFileWithContent("path/to/file3", "path/to/file3 content");

        // Archive the files
        archive.archiveFrom(stagingDir);

        // Check that the zip file exists
        assertThat(zipFile).exists();
        assertThat(archive.isArchived()).isTrue();

        // Read the content of a file that is not in the archive
        assertThatThrownBy(() -> archive.readFile("path/to/file2"))
            .isInstanceOf(IOException.class)
            .hasMessage("path/to/file2 not found in target/test/ZipArchiveReadFileTest/test.zip");
    }
}
