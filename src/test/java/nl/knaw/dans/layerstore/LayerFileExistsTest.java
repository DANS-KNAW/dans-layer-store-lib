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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class LayerFileExistsTest extends AbstractTestWithTestDir {

    @Test
    public void should_return_true_if_file_exists() throws Exception {
        var layer = new LayerImpl(1, stagingDir, new ZipArchive(testDir.resolve("test.zip")));
        createEmptyStagingDirFiles("path/to/file1", "path/to/file2");

        assertThat(layer.fileExists("path/to/file1")).isTrue();
    }

    @Test
    public void should_return_true_if_file_exists_in_archived_layer() throws Exception {
        var layer = new LayerImpl(1, stagingDir, new ZipArchive(testDir.resolve("test.zip")));
        createEmptyStagingDirFiles("path/to/file1", "path/to/file2");

        assertThat(stagingDir.resolve("path/to/file1").toFile()).exists();
        assertThat(stagingDir.resolve("test.zip").toFile()).doesNotExist();
        layer.close();
        layer.archive();

        assertThat(layer.fileExists("path/to/file1")).isTrue();
        assertThat(stagingDir.resolve("path/to/file1").toFile()).doesNotExist();
        assertThat(testDir.resolve("test.zip").toFile()).exists();
        assertThat(layer.isArchived()).isTrue();
    }

    @Test
    public void should_return_false_if_file_existed_in_destroyed_archived_layer() throws Exception {
        var layer = new LayerImpl(1, stagingDir, new ZipArchive(testDir.resolve("test.zip")));
        createEmptyStagingDirFiles("path/to/file1", "path/to/file2");

        layer.close();
        layer.archive();
        if (!testDir.resolve("test.zip").toFile().delete()) {
            throw new Exception("Could not delete test.zip");
        }
        assertThat(layer.fileExists("path/to/file1")).isFalse();
    }

    @Test
    public void should_return_false_if_file_does_not_exist() throws Exception {
        var layer = new LayerImpl(1, stagingDir, new ZipArchive(testDir.resolve("test.zip")));
        createEmptyStagingDirFiles("path/to/file1", "path/to/file2");

        assertThat(layer.fileExists("path/to/file3")).isFalse();
    }
}
