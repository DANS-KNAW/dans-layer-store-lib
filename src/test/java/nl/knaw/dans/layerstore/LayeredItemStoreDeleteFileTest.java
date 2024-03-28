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
import java.nio.file.NoSuchFileException;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class LayeredItemStoreDeleteFileTest extends AbstractLayerDatabaseTest {

    @BeforeEach
    public void prepare() throws Exception {
        Files.createDirectories(stagingDir);
    }

    @Test
    public void should_not_delete_a_file_in_a_closed_layer() throws Exception {
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir));
        var layeredStore = new LayeredItemStore(dao, layerManager);
        Files.createDirectories(archiveDir);
        layeredStore.createDirectory("a/b/c/d");
        layeredStore.writeFile("a/b/c/d/test1.txt", toInputStream("Hello world!"));
        layeredStore.writeFile("a/b/c/test2.txt", toInputStream("Hello again!"));
        var firstLayer = layerManager.getTopLayer();
        layerManager.newTopLayer();
        layeredStore.writeFile("test32.txt", toInputStream("Hello once more!"));

        assertFalse(firstLayer.isOpen());
        assertThatThrownBy(() -> layeredStore.deleteFiles(List.of("a/b/c/d/test1.txt", "a/b/c/test2.txt")))
            .isInstanceOf(NoSuchFileException.class)
            .hasMessageContaining("/a/b/c/d/test1.txt");
    }

    @Test
    public void should_delete_files_from_the_top_layer() throws Exception {
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir));
        var layeredStore = new LayeredItemStore(dao, layerManager);
        layeredStore.createDirectory("a/b/c/d");
        layeredStore.writeFile("a/b/c/d/test1.txt", toInputStream("Hello world!"));
        layeredStore.writeFile("a/b/c/test2.txt", toInputStream("Hello world!"));

        layeredStore.deleteFiles(List.of("a/b/c/d/test1.txt", "a/b/c/test2.txt"));

        assertThat(layeredStore.listRecursive("a").stream().map(Item::getPath))
            .containsExactlyInAnyOrder( "a/b", "a/b/c", "a/b/c/d");
    }
}