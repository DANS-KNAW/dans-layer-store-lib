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
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class LayerReadFileTest extends AbstractTestWithTestDir {
    @Test
    public void should_read_file_from_staging_dir_if_layer_is_open() throws Exception {
        var layer = new LayerImpl(1, new StagingDir(stagingDir), new ZipArchive(archiveRoot.resolve("test.zip")));
        var testContents = "test file";
        FileUtils.write(stagingDir.resolve("path/to/file1").toFile(), testContents, StandardCharsets.UTF_8);

        try (var inputStream = layer.readFile("path/to/file1")) {
            var actualContents = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
            assertThat(actualContents).isEqualTo(testContents);
        }
    }

    @Test
    public void should_read_file_from_staging_dir_if_layer_is_closed_but_not_archived() throws Exception {
        var layer = new LayerImpl(1, new StagingDir(stagingDir), new ZipArchive(archiveRoot.resolve("test.zip")));
        var testContents = "test file";
        FileUtils.write(stagingDir.resolve("path/to/file1").toFile(), testContents, StandardCharsets.UTF_8);
        layer.close();

        try (var inputStream = layer.readFile("path/to/file1")) {
            var actualContents = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
            assertThat(actualContents).isEqualTo(testContents);
        }
    }

    @Test
    public void should_read_file_from_archive_if_layer_is_closed_and_archived() throws Exception {
        Files.createDirectories(archiveRoot);
        var layer = new LayerImpl(1, new StagingDir(stagingDir), new ZipArchive(archiveRoot.resolve("test.zip")));
        var testContents = "test file";
        FileUtils.write(stagingDir.resolve("path/to/file1").toFile(), testContents, StandardCharsets.UTF_8);
        layer.close();
        layer.archive();

        // Check that the staging directory is gone, so we are sure the file can only be read from the archive.
        assertThat(stagingDir.toFile()).doesNotExist();

        try (var inputStream = layer.readFile("path/to/file1")) {
            var actualContents = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
            assertThat(actualContents).isEqualTo(testContents);
        }
    }
}
