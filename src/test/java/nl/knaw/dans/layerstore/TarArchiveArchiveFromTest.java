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

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class TarArchiveArchiveFromTest extends AbstractTestWithTestDir {

    @Test
    public void should_create_archivefile_and_change_status_to_archived() throws Exception {
        var tarFile = testDir.resolve("test.tar");
        TarArchive tarArchive = new TarArchive(tarFile);

        // Create some files to archive
        var file1 = testDir.resolve("staging/file1");
        var file2 = testDir.resolve("staging/path/to/file2");
        var file3 = testDir.resolve("staging/path/to/file3");

        // Write some string content to the files
        String file1Content = "file1 content";
        String file2Content = "file2 content";
        String file3Content = "file3 content";
        FileUtils.forceMkdir(file2.getParent().toFile());
        FileUtils.write(file1.toFile(), file1Content, "UTF-8");
        FileUtils.write(file2.toFile(), file2Content, "UTF-8");
        FileUtils.write(file3.toFile(), file3Content, "UTF-8");

        // Archive the files
        tarArchive.archiveFrom(testDir.resolve("staging"));

        // Check that the tar file exists and contains the files and not more than that
        assertThat(tarFile).exists();
        Map<String, String> actual = new HashMap<>();
        try (var tf = new TarArchiveInputStream(Files.newInputStream(tarFile))) {
            TarArchiveEntry entry;
            while ((entry = tf.getNextTarEntry()) != null) {
                actual.put(entry.getName(), new String(tf.readNBytes((int) entry.getSize()), StandardCharsets.UTF_8));
            }
        }
        assertThat(actual).containsEntry("file1", file1Content);
        assertThat(actual).containsEntry("path/to/file2", file2Content);
        assertThat(actual).containsEntry("path/to/file3", file3Content);
        assertThat(actual).hasSize(3);
    }
}
