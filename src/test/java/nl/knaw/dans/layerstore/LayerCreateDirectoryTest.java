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

import java.nio.file.Files;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class LayerCreateDirectoryTest extends AbstractTestWithTestDir {
    @Test
    public void should_create_directories_in_staging_dir_if_layer_is_open() throws Exception {
        // Given
        Files.createDirectories(stagingDir);
        var layer = new LayerImpl(1, stagingDir, new ZipArchive(archiveDir.resolve("test.zip")));

        // When
        layer.createDirectory("path/to/directory");

        // Then
        assertThat(stagingDir.resolve("path/to/directory")).exists();
    }

    @Test
    public void should_throw_IllegalStateException_if_layer_is_closed() throws Exception {
        // Given
        Files.createDirectories(stagingDir);
        var layer = new LayerImpl(1, stagingDir, new ZipArchive(archiveDir.resolve("test.zip")));
        layer.close();

        // When / Then
        assertThatThrownBy(() -> layer.createDirectory("path/to/directory"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Layer is closed, but must be open for this operation");
    }

    @Test
    public void should_throw_IllegalArgumentException_if_path_is_null() throws Exception {
        // Given
        Files.createDirectories(stagingDir);
        var layer = new LayerImpl(1, stagingDir, new ZipArchive(archiveDir.resolve("test.zip")));

        // When / Then
        assertThatThrownBy(() -> layer.createDirectory(null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Path cannot be null");
    }

    @Test
    public void should_throw_IllegalArgumentException_if_path_is_blank() throws Exception {
        // Given
        Files.createDirectories(stagingDir);
        var layer = new LayerImpl(1, stagingDir, new ZipArchive(archiveDir.resolve("test.zip")));

        // When / Then
        assertThatThrownBy(() -> layer.createDirectory(" "))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Path cannot be blank");
    }

    @Test
    public void should_throw_IllegalArgumentException_if_path_is_outside_staging_dir() throws Exception {
        // Given
        Files.createDirectories(stagingDir);
        var layer = new LayerImpl(1, stagingDir, new ZipArchive(archiveDir.resolve("test.zip")));

        // When / Then
        assertThatThrownBy(() -> layer.createDirectory("path/to/../../../directory"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Path is outside staging directory");
    }
}
