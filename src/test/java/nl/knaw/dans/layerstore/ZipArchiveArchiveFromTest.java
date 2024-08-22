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

import lombok.SneakyThrows;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Map.entry;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class ZipArchiveArchiveFromTest extends AbstractTestWithTestDir {
    @Test
    public void should_create_zipfile_and_change_status_to_archived() throws Exception {
        var archiveFile = testDir.resolve("test.zip");
        var archive = new ZipArchive(archiveFile);

        createStagingFileWithContent("file1", "file1 content");
        createStagingFileWithContent("path/to/file2", "path/to/file2 content");
        createStagingFileWithContent("path/to/file3", "path/to/file3 content");

        // Archive the files
        archive.archiveFrom(stagingDir);

        // Check that the zip file exists and contains the files and not more than that
        assertThat(archiveFile).exists();
        try (var zip = ZipFile.builder()
            .setFile(archiveFile.toFile())
            .get()) {
            assertThat(Collections.list(zip.getEntries()).stream()
                .map(archiveEntry -> getEntry(archiveEntry, zip))
            ).containsExactlyInAnyOrder(
                entry("file1", "file1 content"),
                entry("path/", ""),
                entry("path/to/", ""),
                entry("path/to/file2", "path/to/file2 content"),
                entry("path/to/file3", "path/to/file3 content")
            );
        }
        assertThat(archive.isArchived()).isTrue();

        // note that LayerImpl.doArchive clears the stagingDir after calling Archive.archiveFrom
        assertThat(stagingDir).isNotEmptyDirectory();
    }

    @SneakyThrows
    private static Map.Entry<String, String> getEntry(ZipArchiveEntry tarArchiveEntry, ZipFile zip) {
        var bytes = zip.getInputStream(tarArchiveEntry)
            .readNBytes((int) tarArchiveEntry.getSize());
        return entry(tarArchiveEntry.getName(), new String(bytes, UTF_8));
    }
}
