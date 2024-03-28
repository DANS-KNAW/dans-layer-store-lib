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
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class LayeredItemStoreMoveDirectoryIntoTest extends AbstractLayerDatabaseTest {
    private static class StoreTxtContent extends NoopDatabaseBackedContentManager {
        @Override
        public boolean test(String path) {
            return path.endsWith(".txt");
        }
    }

    @BeforeEach
    public void prepare() throws Exception {
        Files.createDirectories(stagingDir);
        FileUtils.write(testDir.resolve("x/y/test1.txt").toFile(), "Hello world!", "UTF-8");
        FileUtils.write(testDir.resolve("x/test2.txt").toFile(), "Hello again!", "UTF-8");
    }

    @Test
    public void should_add_directory_structure_and_files() throws Exception {
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir));
        var layeredStore = new LayeredItemStore(dao, layerManager, new StoreTxtContent());
        layeredStore.createDirectory("a/b");

        layeredStore.moveDirectoryInto(testDir.resolve("x"), "a/b/c");

        assertThat(layeredStore.listRecursive("a").stream().map(Item::getPath))
            .containsExactlyInAnyOrder("a/b", "a/b/c/", "a/b/c/test2.txt", "a/b/c/y", "a/b/c/y/test1.txt");
        assertThat(stagingDir
            .resolve(String.valueOf(layerManager.getTopLayer().getId()))
            .resolve("a/b/c/y/test1.txt")
        ).exists();

        // assert content is in DB
        Files.createDirectories(archiveDir);
        layerManager.getTopLayer().close();
        layerManager.getTopLayer().archive();
        assertThat(stagingDir).isEmptyDirectory();
        try (var inputStream = layeredStore.readFile("a/b/c/y/test1.txt")) {
            Assertions.assertThat(inputStream).hasContent("Hello world!");
        }
    }

    @Test
    public void should_create_parent_in_top_layer() throws Exception {
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir));
        var layeredStore = new LayeredItemStore(dao, layerManager);
        layeredStore.createDirectory("a/b");
        Files.createDirectories(archiveDir);
        layerManager.newTopLayer();
        layeredStore.deleteDirectory("a/b");

        layeredStore.moveDirectoryInto(testDir.resolve("x"), "a/b/c");

        assertThat(layeredStore.listRecursive("a").stream().map(Item::getPath))
            .containsExactlyInAnyOrder("a/b", "a/b/c/", "a/b/c/test2.txt", "a/b/c/y", "a/b/c/y/test1.txt");
    }

    @Test
    public void should_throw_parent_of_destination_does_not_exists() {
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir));
        var layeredStore = new LayeredItemStore(dao, layerManager);

        assertThatThrownBy(() -> layeredStore.moveDirectoryInto(testDir.resolve("x"), "a/b/c")).
            isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Parent of destination does not exist: a/b");
    }

    @Test
    public void should_throw_destination_exists() throws Exception {
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir));
        var layeredStore = new LayeredItemStore(dao, layerManager);

        layeredStore.createDirectory("a/b/c");

        assertThatThrownBy(() -> layeredStore.moveDirectoryInto(testDir.resolve("x"), "a")).
            isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Destination already exists: a");
    }
}
