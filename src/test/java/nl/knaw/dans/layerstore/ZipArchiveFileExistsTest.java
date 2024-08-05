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

import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class ZipArchiveFileExistsTest extends AbstractTestWithTestDir {
    @Test
    public void should_return_true_when_file_exists_in_archive() throws Exception {
        var zipFile = testDir.resolve("test.zip");
        ZipArchive zipArchive = new ZipArchive(zipFile);

        createFileWithContent("file1");
        createFileWithContent("file2");
        createFileWithContent("file3");

        // Archive the files
        zipArchive.archiveFrom(stagingDir);

        // Check that the zip file exists
        assertThat(zipFile).exists();
        assertThat(zipArchive.isArchived()).isTrue();

        // Check that the files are unarchived
        assertThat(zipArchive.fileExists("file1")).isTrue();
        assertThat(zipArchive.fileExists("path/to/file2")).isTrue();
        assertThat(zipArchive.fileExists("path/to/file3")).isTrue();
    }

    private void createFileWithContent(String name) throws IOException {
        var file = stagingDir.resolve("path/to/" + name);
        var content = name + " content";
        FileUtils.forceMkdir(file.getParent().toFile());
        FileUtils.write(file.toFile(), content, "UTF-8");
    }
}
