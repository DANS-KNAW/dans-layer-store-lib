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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class LayeredItemStoreMoveDirectoryInternalTest extends AbstractLayerDatabaseTest {

    @BeforeEach
    public void prepare() throws Exception {
        Files.createDirectories(stagingDir);
    }

    @Test
    public void should_move_dir_with_file() throws Exception {
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir));
        var layeredStore = new LayeredItemStore(dao, layerManager);

        layeredStore.createDirectory("a/b/c/d");
        layeredStore.createDirectory("a/b/e/f");
        layeredStore.writeFile("a/b/e/f/test.txt", toInputStream("Hello world!"));

        layeredStore.moveDirectoryInternal("a/b/e/f", "a/b/c/d/x");

        var layerDir = stagingDir.resolve(String.valueOf(layerManager.getTopLayer().getId()));
        assertThat(layerDir.resolve("a/b/c/d/x/test.txt")).exists();
        assertThat(layerDir.resolve("a/b/e")).exists();
    }

    @Test
    public void should_not_move_dir_with_files_in_other_layer() throws Exception {
        Files.createDirectories(archiveDir); // without this, a stack trace is logged from an archiving thread
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir));
        var layeredStore = new LayeredItemStore(dao, layerManager);

        layeredStore.createDirectory("a/b/c/d");
        layeredStore.createDirectory("a/b/e/f");
        layeredStore.writeFile("a/b/e/f/test.txt", toInputStream("Hello world!"));
        layerManager.newTopLayer();


        assertThatThrownBy(() -> layeredStore.moveDirectoryInternal("a/b/e/f", "a/b/c/d/x")).
            isInstanceOf(RuntimeException.class)
            .hasMessage("Cannot moveDirectoryInternal because the following items are in multiple layers: [a/b/e/f/test.txt]");
    }
}
