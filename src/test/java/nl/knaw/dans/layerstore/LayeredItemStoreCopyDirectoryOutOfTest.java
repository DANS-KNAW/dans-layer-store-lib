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

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class LayeredItemStoreCopyDirectoryOutOfTest extends AbstractLayerDatabaseTest {

    @BeforeEach
    public void prepare() throws Exception {
        Files.createDirectories(stagingDir);
    }

    @Test
    public void should_copy_an_empty_child_directory() throws Exception {
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir));
        var layeredStore = new LayeredItemStore(dao, layerManager);
        Files.createDirectories(archiveDir);
        layeredStore.createDirectory("a/b/c/d/e");
        layeredStore.writeFile("a/b/c/d/test1.txt", toInputStream("Hello world!"));
        layeredStore.writeFile("a/b/c/test2.txt", toInputStream("Hello again!"));
        var destination = testDir.resolve("copy");

        layeredStore.copyDirectoryOutOf("a/b/c/d", destination);

        assertThat(destination.resolve("a/b/c/d/e")).isEmptyDirectory();
    }

    @Test
    public void should_not_copy_an_empty_source_directory() throws Exception {
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir));
        var layeredStore = new LayeredItemStore(dao, layerManager);
        Files.createDirectories(archiveDir);
        layeredStore.createDirectory("a/b/c/d/e");
        layeredStore.writeFile("a/b/c/d/test1.txt", toInputStream("Hello world!"));
        layeredStore.writeFile("a/b/c/test2.txt", toInputStream("Hello again!"));
        Path destination = testDir.resolve("copy");

        layeredStore.copyDirectoryOutOf("a/b/c/d/e", destination);

        assertThat(destination).doesNotExist();
    }

    @Test
    public void should_copy_the_files_of_a_source_which_has_no_child_directories() throws Exception {
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir));
        var layeredStore = new LayeredItemStore(dao, layerManager);
        Files.createDirectories(archiveDir);
        layeredStore.createDirectory("a/b/c/d");
        layeredStore.writeFile("a/b/c/d/test1.txt", toInputStream("Hello world!"));
        layeredStore.writeFile("a/b/c/d/test2.txt", toInputStream("Hello world!"));
        layeredStore.writeFile("a/b/c/test3.txt", toInputStream("Hello again!"));
        Path destination = testDir.resolve("copy");

        layeredStore.copyDirectoryOutOf("a/b/c/d", destination);

        assertThat(destination.resolve("a/b/c/d/test1.txt")).exists();
        assertThat(destination.resolve("a/b/c/d/test2.txt")).exists();
    }
}