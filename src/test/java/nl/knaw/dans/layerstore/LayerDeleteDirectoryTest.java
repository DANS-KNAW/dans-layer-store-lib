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

public class LayerDeleteDirectoryTest extends AbstractTestWithTestDir {
    @Test
    public void should_delete_directory_in_staging_dir_when_layer_is_open() throws Exception {
        var stagingDir = testDir.resolve("staging");
        var layer = new LayerImpl(1, stagingDir, new ZipArchive(testDir.resolve("test.zip")));

        // Create a directory with files in it
        if (!stagingDir.resolve("path/to").toFile().mkdirs() ||
            !stagingDir.resolve("path/to/file1").toFile().createNewFile() ||
            !stagingDir.resolve("path/to/file2").toFile().createNewFile()) {
            throw new Exception("Could not create files to delete");
        }

        // And another directory with files in it
        if (!stagingDir.resolve("path/too").toFile().mkdirs() ||
            !stagingDir.resolve("path/too/file1").toFile().createNewFile() ||
            !stagingDir.resolve("path/too/file2").toFile().createNewFile()) {
            throw new Exception("Could not create files in path too");
        }

        // Delete the files in the first directory
        layer.deleteDirectory("path/to");

        // Check that the files in the first directory are gone
        assertThat(stagingDir.resolve("path/to")).doesNotExist();

        // Check that the files in the second directory are still there
        assertThat(stagingDir.resolve("path/too/file1")).exists();
        assertThat(stagingDir.resolve("path/too/file2")).exists();
        assertThat(stagingDir.resolve("path/too")).isDirectory();
    }

    @Test
    public void should_throw_IllegalStateException_when_layer_is_closed() throws Exception {
        var stagingDir = testDir.resolve("staging");
        var layer = new LayerImpl(1, stagingDir, new ZipArchive(testDir.resolve("test.zip")));
        // Create a directory with files in it
        if (!stagingDir.resolve("path/to").toFile().mkdirs() ||
            !stagingDir.resolve("path/to/file1").toFile().createNewFile() ||
            !stagingDir.resolve("path/to/file2").toFile().createNewFile()) {
            throw new Exception("Could not create files to delete");
        }

        // And another directory with files in it
        if (!stagingDir.resolve("path/too").toFile().mkdirs() ||
            !stagingDir.resolve("path/too/file1").toFile().createNewFile() ||
            !stagingDir.resolve("path/too/file2").toFile().createNewFile()) {
            throw new Exception("Could not create files in path too");
        }
        layer.close();

        assertThatThrownBy(() -> layer.deleteDirectory("path/to"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Layer is closed, but must be open for this operation");
    }

    @Test
    public void should_throw_IllegalArgumentException_when_path_is_null() throws Exception {
        var layer = new LayerImpl(1, testDir.resolve("staging"), new ZipArchive(testDir.resolve("test.zip")));
        assertThatThrownBy(() -> layer.deleteDirectory(null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Path cannot be null");
    }

    // validatePath is tested enough in LayerImplDeleteFilesTest
}
