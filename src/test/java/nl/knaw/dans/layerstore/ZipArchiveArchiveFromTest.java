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

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static nl.knaw.dans.layerstore.TestUtils.zipFileFrom;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class ZipArchiveArchiveFromTest extends AbstractTestWithTestDir {
    @Test
    public void should_create_zipfile_and_change_status_to_archived() throws Exception {
        var archiveFile = testDir.resolve("test.zip");
        var archive = new ZipArchive(archiveFile);

        createStagingFileWithContent("file1");
        createStagingFileWithContent("path/to/file2");
        createStagingFileWithContent("path/to/file3");

        // Archive the files
        archive.archiveFrom(stagingDir);

        // Check that the zip file exists and contains the files and not more than that
        assertThat(archiveFile).exists();
        try (var zip = zipFileFrom(archiveFile)) {
            assertThat(Collections.list(zip.getEntries()).stream().map(ZipArchiveEntry::getName))
                .containsExactlyInAnyOrder("file1", "path/to/file2", "path/to/file3", "path/", "path/to/");
        }
        assertThat(archive.isArchived()).isTrue();

        // note that LayerImpl.doArchive clears the stagingDir after calling Archive.archiveFrom
        assertThat(stagingDir).isNotEmptyDirectory();
    }
}
