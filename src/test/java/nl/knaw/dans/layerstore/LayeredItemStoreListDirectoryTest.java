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

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class LayeredItemStoreListDirectoryTest extends AbstractLayerDatabaseTest {

    @BeforeEach
    public void prepare() throws Exception {
        Files.createDirectories(stagingDir);
    }

    @Test
    public void should_return_a_shallow_list() throws Exception {
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir));
        var layeredStore = new LayeredItemStore(dao, layerManager);
        Files.createDirectories(archiveDir);
        layeredStore.createDirectory("a/b/c/d/e/f");
        layeredStore.writeFile("a/b/c/d/test1.txt", toInputStream("Hello world!"));
        layeredStore.writeFile("a/b/c/test2.txt", toInputStream("Hello again!"));

        var result = layeredStore.listDirectory("a/b/c").stream().map(Item::getPath);

        assertThat(result).containsExactlyInAnyOrder("a/b/c/d", "a/b/c/test2.txt");
        //more by LayerDatabaseListDirectoryTest
    }
}