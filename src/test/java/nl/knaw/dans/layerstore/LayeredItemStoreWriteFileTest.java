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

import io.dropwizard.util.DirectExecutorService;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.io.IOUtils.toInputStream;
import static org.assertj.core.api.Assertions.assertThat;

public class LayeredItemStoreWriteFileTest extends AbstractLayerDatabaseTest {
    private static class StoreTxtContent extends NoopDatabaseBackedContentManager {
        @Override
        public boolean test(String path) {
            return path.endsWith(".txt");
        }
    }

    @Test
    public void should_write_file_to_staging_dir_when_layer_is_open() throws Exception {
        Files.createDirectories(stagingDir);
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir), new DirectExecutorService());
        var layeredStore = new LayeredItemStore(db, layerManager);

        var testContent = "Hello world!";
        layeredStore.writeFile("test.txt", toInputStream(testContent, UTF_8));

        var topLayer = layerManager.getTopLayer();

        var path = stagingDir.resolve(Long.toString(topLayer.getId())).resolve("test.txt");
        assertThat(path).exists();
        assertThat(path).usingCharset(StandardCharsets.UTF_8).hasContent(testContent);
    }

    @Test
    public void should_write_copy_of_content_to_database_if_filter_applies() throws Exception {
        Files.createDirectories(stagingDir);
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir), new DirectExecutorService());
        var layeredStore = new LayeredItemStore(db, layerManager, new StoreTxtContent());

        var testContent = "Hello world!";
        layeredStore.writeFile("test.txt", toInputStream(testContent, UTF_8));

        // Check that the file content is in the database
        db.getAllRecords().toList().forEach(itemRecord -> {
            if (itemRecord.getPath().equals("test.txt")) {
                assertThat(itemRecord.getContent()).isEqualTo(testContent.getBytes(UTF_8));
            }
        });
    }

    @Test
    public void should_overwrite_content_in_the_database_if_filter_applies() throws Exception {
        Files.createDirectories(stagingDir);
        var layerManager = new LayerManagerImpl(stagingDir, new TarArchiveProvider(archiveDir), new DirectExecutorService());
        var layeredStore = new LayeredItemStore(db, layerManager, new StoreTxtContent());

        layeredStore.writeFile("test.txt", toInputStream("Hello world!", UTF_8));
        layeredStore.writeFile("test.txt", toInputStream("Hello again!", UTF_8));

        // Check that the file content is in the database
        db.getAllRecords().toList().forEach(itemRecord -> {
            if (itemRecord.getPath().equals("test.txt")) {
                assertThat(itemRecord.getContent()).isEqualTo("Hello again!".getBytes(UTF_8));
            }
        });

    }

    @Test
    public void should_add_copies_to_the_database_if_filter_applies() throws Exception {
        Files.createDirectories(stagingDir);
        Files.createDirectories(archiveDir);
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir), new DirectExecutorService());
        var layeredStore = new LayeredItemStore(db, layerManager, new StoreTxtContent());

        layeredStore.writeFile("test.txt", toInputStream("Hello world!", UTF_8));
        layerManager.newTopLayer();
        layeredStore.writeFile("test.txt", toInputStream("Hello again!", UTF_8));
        layerManager.newTopLayer();
        layeredStore.writeFile("test.txt", toInputStream("Hello once more!", UTF_8));

        // Check that the file contents are in the database
        var list = db.getAllRecords().map(itemRecord ->
            new String((itemRecord.getContent()), StandardCharsets.UTF_8)
        );
        assertThat(list).containsExactlyInAnyOrder("Hello world!", "Hello once more!", "Hello again!");
    }

}
