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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class LayeredItemStoreDeleteDirectoryTest extends AbstractLayerDatabaseTest {

    @BeforeEach
    public void prepare() throws Exception {
        Files.createDirectories(stagingDir);
    }

    @Test
    public void should_not_delete_a_directory_with_content_in_another_layer() throws Exception {
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir));
        var layeredStore = new LayeredItemStore(dao, layerManager);
        Files.createDirectories(archiveDir);
        layeredStore.createDirectory("a/b/c/d");
        layerManager.newTopLayer();

        assertThatThrownBy(() -> layeredStore.deleteDirectory("a/b/c")).
            isInstanceOf(RuntimeException.class)
            .hasMessage("Cannot deleteDirectory because the following items are in multiple layers: [a/b/c/d]");
    }

    @Test
    public void should_delete_empty_directory() throws Exception {
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir));
        var layeredStore = new LayeredItemStore(dao, layerManager);
        layeredStore.createDirectory("a/b/c/d");

        layeredStore.deleteDirectory("a/b/c");


        // directory is removed from the stagingDir
        var layerDir = stagingDir.resolve(Path.of(String.valueOf((layerManager.getTopLayer().getId()))));
        assertThat(layerDir.resolve("a/b")).isEmptyDirectory();

        assumeNotYetFixed("TODO: files are not removed from the database (the code above shows coverage)");
        assertThat(layeredStore.listRecursive("a").stream().map(Item::getPath))
            .containsExactlyInAnyOrder("a", "a/b");
    }

    @Test
    public void should_delete_directory_with_plain_file() throws Exception {
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir));
        var layeredStore = new LayeredItemStore(dao, layerManager);
        layeredStore.createDirectory("a/b/c/d");
        layeredStore.writeFile("a/b/c/test.txt", toInputStream("Hello world!"));

        layeredStore.deleteDirectory("a/b");

        // files are removed from the stagingDir
        var layerDir = stagingDir.resolve(Path.of(String.valueOf((layerManager.getTopLayer().getId()))));
        assertThat(layerDir.resolve("a")).isEmptyDirectory();

        assumeNotYetFixed("TODO: files are not removed from the database (the code above shows coverage)");
        assertThat(layeredStore.listRecursive("a").stream().map(Item::getPath))
            .containsExactlyInAnyOrder("a");
    }
}