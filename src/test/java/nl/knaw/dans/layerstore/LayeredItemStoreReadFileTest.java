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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.io.IOUtils.toInputStream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class LayeredItemStoreReadFileTest extends AbstractLayerDatabaseTest {
    private static class StoreTxtContent extends NoopDatabaseBackedContentManager {
        @Override
        public boolean test(String path) {
            return path.endsWith(".txt");
        }
    }

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        Files.createDirectories(archiveRoot);
    }

    @Test
    public void should_read_content_from_stagingRoot() throws Exception {
        var layerManager = new LayerManagerImpl(stagingRoot, new ZipArchiveProvider(archiveRoot), new DirectLayerArchiver());
        var layeredStore = new LayeredItemStore(db, layerManager);
        layeredStore.newTopLayer();

        var testContent = "Hello world!";
        layeredStore.writeFile("test.txt", toInputStream(testContent, UTF_8));

        try (var inputStream = layeredStore.readFile("test.txt")) {
            assertThat(inputStream).hasContent(testContent);
        }
    }

    @Test
    public void should_read_content_from_database_if_filter_applies() throws Exception {
        var layerManager = new LayerManagerImpl(stagingRoot, new ZipArchiveProvider(archiveRoot), new DirectLayerArchiver());
        var layeredStore = new LayeredItemStore(db, layerManager, new StoreTxtContent());
        layeredStore.newTopLayer();

        var testContent = "Hello world!";
        layeredStore.writeFile("test.txt", toInputStream(testContent, UTF_8));

        try (var inputStream = layeredStore.readFile("test.txt")) {
            assertThat(inputStream).hasContent(testContent);
        }
    }

    @Test
    public void should_throw_is_a_directory() throws Exception {
        var layerManager = new LayerManagerImpl(stagingRoot, new ZipArchiveProvider(archiveRoot), new DirectLayerArchiver());
        var layeredStore = new LayeredItemStore(db, layerManager, new StoreTxtContent());
        layeredStore.newTopLayer();

        layeredStore.createDirectory("a/b/c");

        assertThatThrownBy(() -> layeredStore.readFile("a/b")).
            isInstanceOf(IOException.class)
            .hasMessage("Path is a directory: a/b");
    }

    @Test
    public void should_throw_no_such_file() throws IOException {
        var layerManager = new LayerManagerImpl(stagingRoot, new ZipArchiveProvider(archiveRoot), new DirectLayerArchiver());
        var layeredStore = new LayeredItemStore(db, layerManager, new StoreTxtContent());
        layeredStore.newTopLayer();

        assertThatThrownBy(() -> layeredStore.readFile("some.txt")).
            isInstanceOf(NoSuchFileException.class);
    }
}
