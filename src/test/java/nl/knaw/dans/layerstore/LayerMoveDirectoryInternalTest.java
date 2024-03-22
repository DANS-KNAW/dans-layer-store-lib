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

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class LayerMoveDirectoryInternalTest extends AbstractTestWithTestDir {
    @Test
    public void should_move_directory_from_staging_dir_to_staging_dir_if_layer_is_open() throws Exception {
        var layer = new LayerImpl(1, stagingDir, new ZipArchive(testDir.resolve("test.zip")));
        createEmptyStagingDirFiles("path/to/file1", "path/to/file2");

        layer.moveDirectoryInternal("path/to/", "path/too/");
        assertThat(stagingDir.resolve("path/to")).doesNotExist();
        assertThat(stagingDir.resolve("path/too")).exists();
        assertThat(stagingDir.resolve("path/too/file1")).exists();
        assertThat(stagingDir.resolve("path/too/file2")).exists();
    }

    @Test
    public void should_throw_IllegalStateException_when_layer_is_closed() throws Exception {
        var layer = new LayerImpl(1, stagingDir, new ZipArchive(testDir.resolve("test.zip")));
        // Create a directory with files in it
        if (!stagingDir.resolve("path/to").toFile().mkdirs() ||
            !stagingDir.resolve("path/to/file1").toFile().createNewFile() ||
            !stagingDir.resolve("path/to/file2").toFile().createNewFile()) {
            throw new Exception("Could not create files to move");
        }
        layer.close();

        assertThatThrownBy(() -> layer.moveDirectoryInternal("path/to/", "path/too/"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Layer is closed, but must be open for this operation");
    }

    @Test
    public void should_throw_IllegalArgumentException_if_source_is_outside_staging_dir() {
        var layer = new LayerImpl(1, stagingDir, new ZipArchive(testDir.resolve("test.zip")));
        assertThatThrownBy(() -> layer.moveDirectoryInternal("../path/to/", "path/too/"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Path is outside staging directory");
    }

    @Test
    public void should_throw_IllegalArgumentException_if_destination_is_outside_staging_dir() {
        var layer = new LayerImpl(1, stagingDir, new ZipArchive(testDir.resolve("test.zip")));
        assertThatThrownBy(() -> layer.moveDirectoryInternal("path/to/", "../path/too/"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Path is outside staging directory");
    }
}
