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

public class LayerMoveDirectoryIntoTest extends AbstractTestWithTestDir {
    @Test
    public void should_move_directory_into_staging_dir_when_layer_is_open() throws Exception {
        var inputDir = testDir.resolve("input-roundtrip");

        var layer = new LayerImpl(1, new StagingDir(stagingDir), new ZipArchive(archiveRoot.resolve("test.zip")));

        // Create some files in the input directory
        if (!inputDir.resolve("path/to").toFile().mkdirs() ||
            !inputDir.resolve("path/to/file1").toFile().createNewFile() ||
            !inputDir.resolve("path/to/file2").toFile().createNewFile()) {
            throw new Exception("Could not create files to move");
        }

        // Create the destination parent directory
        if (!stagingDir.resolve("path/to/").toFile().mkdirs()) {
            throw new Exception("Could not create destination directory");
        }

        // Move the files in the first directory
        layer.moveDirectoryInto(inputDir, "path/to/destination");

        // Check that the input directory is gone
        assertThat(inputDir).doesNotExist();

        // Check that the files are now in the staging directory
        assertThat(stagingDir.resolve("path/to/destination/path/to/file1")).exists();
        assertThat(stagingDir.resolve("path/to/destination/path/to/file2")).exists();
        assertThat(stagingDir.resolve("path/to/destination")).isDirectory();
    }

    @Test
    public void should_throw_IllegalStateException_when_layer_is_closed() throws Exception {
        var inputDir = testDir.resolve("input-roundtrip");

        var layer = new LayerImpl(1, new StagingDir(stagingDir), new ZipArchive(archiveRoot.resolve("test.zip")));

        // Create some files in the input directory
        if (!inputDir.resolve("path/to").toFile().mkdirs() ||
            !inputDir.resolve("path/to/file1").toFile().createNewFile() ||
            !inputDir.resolve("path/to/file2").toFile().createNewFile()) {
            throw new Exception("Could not create files to move");
        }

        // Create the destination parent directory
        if (!stagingDir.resolve("path/to/").toFile().mkdirs()) {
            throw new Exception("Could not create destination directory");
        }

        // Move the files in the first directory
        layer.close();

        assertThatThrownBy(() -> layer.moveDirectoryInto(inputDir, "path/to/destination"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Layer is closed, but must be open for this operation");
    }

}
