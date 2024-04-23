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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LayerArchiveTest extends AbstractTestWithTestDir {
    @Test
    public void throws_IllegalStateException_when_layer_is_closed() {
        var layer = new LayerImpl(1, stagingDir, new ZipArchive(archiveDir.resolve("test.zip")));
        assertThrows(IllegalStateException.class, layer::archive);
    }

    @Test
    public void throws_IllegalStateException_when_layer_is_already_archived() throws IOException {
        Files.createDirectories(archiveDir);
        var layer = new LayerImpl(1, stagingDir, new ZipArchive(archiveDir.resolve("test.zip")));
        layer.close();
        layer.archive();
        assertThrows(IllegalStateException.class, layer::archive);
    }

    @Test
    public void throws_RuntimeException_caused_by_an_IOException() throws IOException {
        var layer = new LayerImpl(1, stagingDir, new ZipArchive(archiveDir.resolve("test.zip")));
        layer.close();

        var cause = assertThrows(RuntimeException.class, layer::archive).getCause();
        assertThat(cause).isInstanceOf(NoSuchFileException.class);
        // misleading message: the problem is that the LayerArchiveTest dir does not exist
        assertThat(cause.getMessage()).isEqualTo("target/test/LayerArchiveTest/layer_archive/test.zip");
    }

    @Test
    public void removes_staging_dir() throws IOException {
        Files.createDirectories(archiveDir);
        var layer = new LayerImpl(1, stagingDir, new ZipArchive(archiveDir.resolve("test.zip")));
        assertThat(stagingDir).doesNotExist();
        createEmptyStagingDirFiles("path/to/file1");
        assertThat(stagingDir).exists();
        layer.close();

        layer.archive();
        assertThat(stagingDir).doesNotExist();
    }
}
