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
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarFile;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Map.entry;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class TarArchiveArchiveFromTest extends AbstractTestWithTestDir {

    @Test
    public void should_create_tarfile_and_change_status_to_archived() throws Exception {
        var archiveFile = testDir.resolve("test.tar");
        var archive = new TarArchive(archiveFile);

        createStagingFileWithContent("file1");
        createStagingFileWithContent("path/to/file2");
        createStagingFileWithContent("path/to/file3");

        // Archive the files
        archive.archiveFrom(stagingDir);

        // Check that the tar file exists and contains the files and not more than that
        assertThat(archiveFile).exists();
        try (var tar = new TarFile(archiveFile.toFile())) {
            assertThat(tar.getEntries().stream().map(tarArchiveEntry ->
                getEntry(tarArchiveEntry, tar)
            )).containsExactlyInAnyOrder(
                entry("file1", "file1 content"),
                entry("path/to/file2", "path/to/file2 content"),
                entry("path/to/file3", "path/to/file3 content")
            );
        }
        assertThat(archive.isArchived()).isTrue();

        // note that LayerImpl.doArchive clears the stagingDir after calling Archive.archiveFrom
        assertThat(stagingDir).isNotEmptyDirectory();
    }

    @SneakyThrows
    private static Map.Entry<String, String> getEntry(TarArchiveEntry tarArchiveEntry, TarFile tar) {
        var bytes = tar.getInputStream(tarArchiveEntry)
            .readNBytes((int) tarArchiveEntry.getSize());
        return entry(tarArchiveEntry.getName(), new String(bytes, UTF_8));
    }
}
