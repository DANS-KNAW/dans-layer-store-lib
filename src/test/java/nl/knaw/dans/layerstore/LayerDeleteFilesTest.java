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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class LayerDeleteFilesTest extends AbstractTestWithTestDir {
    @Test
    public void should_delete_files_in_staging_dir_if_layer_is_open() throws Exception {
        var layer = new LayerImpl(1, stagingDir, new ZipArchive(testDir.resolve("test.zip")));
        createEmptyStagingDirFiles("path/to/file1", "path/to/file2");

        layer.deleteFiles(List.of("path/to/file1", "path/to/file2"));
        assertThat(stagingDir.resolve("path/to/file1")).doesNotExist();
        assertThat(stagingDir.resolve("path/to/file2")).doesNotExist();
    }

    @Test
    public void should_throw_IllegalStateException_if_layer_is_closed() {
        var layer = new LayerImpl(1, stagingDir, new ZipArchive(testDir.resolve("test.zip")));
        layer.close();
        assertThatThrownBy(() -> layer.deleteFiles(List.of("path/to/file1", "path/to/file2")))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Layer is closed, but must be open for this operation");
    }

    @Test
    public void should_throw_IllegalArgumentException_if_path_is_null() {
        var layer = new LayerImpl(1, stagingDir, new ZipArchive(testDir.resolve("test.zip")));
        assertThatThrownBy(() -> layer.deleteFiles(null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Paths cannot be null");
    }

    @Test
    public void should_throw_IllegalArgumentException_if_path_contains_null() throws Exception {
        var layer = new LayerImpl(1, stagingDir, new ZipArchive(testDir.resolve("test.zip")));

        if (!stagingDir.resolve("path/to").toFile().mkdirs() ||
            !stagingDir.resolve("path/to/file1").toFile().createNewFile()) {
            throw new Exception("Could not create files to delete");
        }
        var paths = new ArrayList<String>();
        paths.add("path/to/file1");
        paths.add(null);

        assertThatThrownBy(() -> layer.deleteFiles(paths))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Path cannot be null");

    }
}
