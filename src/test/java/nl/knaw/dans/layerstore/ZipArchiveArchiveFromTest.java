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
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Collections;

import static nl.knaw.dans.layerstore.TestUtils.zipFileFrom;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class ZipArchiveArchiveFromTest extends AbstractTestWithTestDir {
    @Test
    public void should_create_zipfile_and_change_status_to_archived() throws Exception {
        var zipFile = testDir.resolve("test.zip");
        ZipArchive zipArchive = new ZipArchive(zipFile);
        // Create some files to archive
        Path file1 = stagingDir.resolve("file1");
        Path file2 = stagingDir.resolve("path/to/file2");
        Path file3 = stagingDir.resolve("path/to/file3");

        // Write some string content to the files
        String file1Content = "file1 content";
        String file2Content = "file2 content";
        String file3Content = "file3 content";
        FileUtils.forceMkdir(file2.getParent().toFile());
        FileUtils.write(file1.toFile(), file1Content, "UTF-8");
        FileUtils.write(file2.toFile(), file2Content, "UTF-8");
        FileUtils.write(file3.toFile(), file3Content, "UTF-8");

        // Archive the files
        zipArchive.archiveFrom(stagingDir);

        // Check that the zip file exists and contains the files and not more than that
        assertThat(zipFile).exists();
        try (var zf = zipFileFrom(zipFile)) {
            assertThat(zf.getEntry("file1")).isNotNull();
            assertThat(zf.getEntry("path/to/file2")).isNotNull();
            assertThat(zf.getEntry("path/to/file3")).isNotNull();

            // 3 files + 2 directories = 5 entries
            assertThat(Collections.list(zf.getEntries()).size()).isEqualTo(5);
        }

        assertThat(zipArchive.isArchived()).isTrue();
    }
}
