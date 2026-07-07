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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static java.nio.charset.StandardCharsets.UTF_8;
import static nl.knaw.dans.layerstore.Item.Type.Directory;
import static org.apache.commons.io.IOUtils.toInputStream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class LayeredItemStoreWriteFileTest extends AbstractLayerDatabaseTest {
    private static class StoreTxtContent extends NoopDatabaseBackedContentManager {
        @Override
        public boolean test(String path) {
            return path.endsWith(".txt");
        }
    }

    @Test
    public void should_write_file_to_staging_dir_when_layer_is_open() throws Exception {
        var layerManager = new LayerManagerImpl(stagingRoot, new ZipArchiveProvider(archiveRoot), new DirectLayerArchiver());
        var layeredStore = new LayeredItemStore(db, layerManager);
        layeredStore.newTopLayer();

        var testContent = "Hello world!";
        layeredStore.writeFile("test.txt", toInputStream(testContent, UTF_8));

        var topLayer = layerManager.getTopLayer();

        var path = stagingRoot.resolve(Long.toString(topLayer.getId())).resolve("test.txt");
        assertThat(path).exists();
        assertThat(path).usingCharset(StandardCharsets.UTF_8).hasContent(testContent);
    }

    @Test
    public void should_write_copy_of_content_to_database_if_filter_applies() throws Exception {
        var layerManager = new LayerManagerImpl(stagingRoot, new ZipArchiveProvider(archiveRoot), new DirectLayerArchiver());
        var layeredStore = new LayeredItemStore(db, layerManager, new StoreTxtContent());
        layeredStore.newTopLayer();

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
        var layerManager = new LayerManagerImpl(stagingRoot, new TarArchiveProvider(archiveRoot), new DirectLayerArchiver());
        var layeredStore = new LayeredItemStore(db, layerManager, new StoreTxtContent());
        layeredStore.newTopLayer();

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
    public void should_succeed_when_file_has_no_parent_path() throws Exception {
        var layerManager = new LayerManagerImpl(stagingRoot, new ZipArchiveProvider(archiveRoot), new DirectLayerArchiver());
        var layeredStore = new LayeredItemStore(db, layerManager);
        layeredStore.newTopLayer();

        // root-level path has no parent, so the parent-existence check is skipped entirely
        layeredStore.writeFile("root-file.txt", toInputStream("content", UTF_8));

        var topLayer = layerManager.getTopLayer();
        assertThat(stagingRoot.resolve(Long.toString(topLayer.getId())).resolve("root-file.txt")).exists();
    }

    @Test
    public void should_succeed_when_parent_directory_exists_in_item_store() throws Exception {
        var layerManager = new LayerManagerImpl(stagingRoot, new ZipArchiveProvider(archiveRoot), new DirectLayerArchiver());
        var layeredStore = new LayeredItemStore(db, layerManager);
        layeredStore.newTopLayer();

        // pre-populate the database with a directory record for the parent
        addToDb(layerManager.getTopLayer().getId(), "subdir", Directory);

        layeredStore.writeFile("subdir/child.txt", toInputStream("content", UTF_8));

        var topLayer = layerManager.getTopLayer();
        assertThat(stagingRoot.resolve(Long.toString(topLayer.getId())).resolve("subdir/child.txt")).exists();
    }

    @Test
    public void should_throw_when_parent_directory_does_not_exist_in_item_store() throws Exception {
        var layerManager = new LayerManagerImpl(stagingRoot, new ZipArchiveProvider(archiveRoot), new DirectLayerArchiver());
        var layeredStore = new LayeredItemStore(db, layerManager);
        layeredStore.newTopLayer();

        // "nonexistent" is not registered in the item store database
        assertThatThrownBy(() -> layeredStore.writeFile("nonexistent/child.txt", toInputStream("content", UTF_8)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("nonexistent");
    }

    @Test
    public void should_add_copies_to_the_database_if_filter_applies() throws Exception {
        Files.createDirectories(archiveRoot);
        var layerManager = new LayerManagerImpl(stagingRoot, new ZipArchiveProvider(archiveRoot), new DirectLayerArchiver());
        var layeredStore = new LayeredItemStore(db, layerManager, new StoreTxtContent());
        layeredStore.newTopLayer();

        layeredStore.writeFile("test.txt", toInputStream("Hello world!", UTF_8));
        layerManager.newTopLayer();
        layeredStore.writeFile("test.txt", toInputStream("Hello again!", UTF_8));
        layerManager.newTopLayer();
        layeredStore.writeFile("test.txt", toInputStream("Hello once more!", UTF_8));

        // Check that the file contents are in the database
        var list = db.getAllRecords()
            .filter(itemRecord -> itemRecord.getPath().equals("test.txt")) // N.B. necessary to filter out the top directory item record
            .map(itemRecord ->
                new String((itemRecord.getContent()), StandardCharsets.UTF_8)
            );
        assertThat(list).containsExactlyInAnyOrder("Hello world!", "Hello once more!", "Hello again!");
    }

}
