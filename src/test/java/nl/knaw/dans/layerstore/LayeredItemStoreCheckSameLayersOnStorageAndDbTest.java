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

import nl.knaw.dans.layerstore.Item.Type;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.io.IOUtils.toInputStream;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class LayeredItemStoreCheckSameLayersOnStorageAndDbTest extends AbstractLayerDatabaseTest {

    @Test
    public void should_pass_if_store_and_database_are_empty() throws Exception {
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir), new DirectLayerArchiver());
        var layeredItemStore = new LayeredItemStore(db, layerManager);
        Files.createDirectories(archiveDir);
        assertThatCode(layeredItemStore::checkSameLayersOnStorageAndDb).doesNotThrowAnyException();
    }

    @Test
    public void should_pass_if_store_and_database_are_in_sync() throws Exception {
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir), new DirectLayerArchiver());
        var layeredStore = new LayeredItemStore(db, layerManager);
        var layeredItemStore = new LayeredItemStore(db, layerManager);
        Files.createDirectories(archiveDir);
        layeredStore.createDirectory("a/b/c/d");
        layeredStore.writeFile("a/b/c/d/test1.txt", toInputStream("Hello world!", UTF_8));
        layeredStore.writeFile("a/b/c/test2.txt", toInputStream("Hello again!", UTF_8));
        assertThatCode(layeredItemStore::checkSameLayersOnStorageAndDb).doesNotThrowAnyException();
    }

    @Test
    public void should_fail_if_db_contains_layer_not_found_on_storage() throws Exception {
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir), new DirectLayerArchiver());
        var layeredStore = new LayeredItemStore(db, layerManager);
        var layeredItemStore = new LayeredItemStore(db, layerManager);
        Files.createDirectories(archiveDir);
        layeredStore.createDirectory("a/b/c/d");
        layeredStore.writeFile("a/b/c/d/test1.txt", toInputStream("Hello world!", UTF_8));
        layeredStore.writeFile("a/b/c/test2.txt", toInputStream("Hello again!", UTF_8));
        // Simulate a manual change in the database that is not reflected in the store
        var nonExistentLayerId = layerManager.getTopLayer().getId() + 1;
        daoTestExtension.inTransaction(() -> {
            var itemRecord = ItemRecord.builder()
                .path("a/b/c/d/test1.txt")
                .type(Type.File)
                .layerId(nonExistentLayerId)
                .build();
            db.saveRecords(itemRecord);
        });
        // Expect an IllegalStateException to be thrown
        assertThatThrownBy(layeredItemStore::checkSameLayersOnStorageAndDb)
            .hasMessageContaining("Layer IDs are inconsistent between database and storage. Missing on storage: [" + nonExistentLayerId + "]");
    }

    @Test
    public void should_fail_if_staging_dir_contains_layer_not_found_in_db() throws Exception {
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir), new DirectLayerArchiver());
        var layeredStore = new LayeredItemStore(db, layerManager);
        var layeredItemStore = new LayeredItemStore(db, layerManager);
        Files.createDirectories(archiveDir);
        layeredStore.createDirectory("a/b/c/d");
        layeredStore.writeFile("a/b/c/d/test1.txt", toInputStream("Hello world!", UTF_8));
        layeredStore.writeFile("a/b/c/test2.txt", toInputStream("Hello again!", UTF_8));
        // Simulate a manual change in the store that is not reflected in the database
        var nonExistentLayerId = layerManager.getTopLayer().getId() + 1;
        var newStagingDir = stagingDir.resolve(Long.toString(nonExistentLayerId));
        Files.createDirectories(newStagingDir);
        assertThatThrownBy(layeredItemStore::checkSameLayersOnStorageAndDb)
            .hasMessageContaining("Layer IDs are inconsistent between database and storage. Missing in database: [" + nonExistentLayerId + "]");
    }

    @Test
    public void should_fail_if_archive_dir_contains_layer_not_found_in_db() throws Exception {
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir), new DirectLayerArchiver());
        var layeredStore = new LayeredItemStore(db, layerManager);
        var layeredItemStore = new LayeredItemStore(db, layerManager);
        Files.createDirectories(archiveDir);
        layeredStore.createDirectory("a/b/c/d");
        layeredStore.writeFile("a/b/c/d/test1.txt", toInputStream("Hello world!", UTF_8));
        layeredStore.writeFile("a/b/c/test2.txt", toInputStream("Hello again!", UTF_8));
        // Simulate a manual change in the store that is not reflected in the database
        var nonExistentLayerId = layerManager.getTopLayer().getId() + 1;
        var newArchivedLayer = archiveDir.resolve(Long.toString(nonExistentLayerId) + ".zip");
        Files.createFile(newArchivedLayer);
        assertThatThrownBy(layeredItemStore::checkSameLayersOnStorageAndDb)
            .hasMessageContaining("Layer IDs are inconsistent between database and storage. Missing in database: [" + nonExistentLayerId + "]");
    }

    @Test
    public void should_pass_if_multiple_layers_present_and_store_in_sync_with_db() throws Exception {
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir), new DirectLayerArchiver());
        var layeredStore = new LayeredItemStore(db, layerManager);
        var layeredItemStore = new LayeredItemStore(db, layerManager);
        Files.createDirectories(archiveDir);
        layeredStore.createDirectory("a/b/c/d");
        layeredStore.writeFile("a/b/c/d/test1.txt", toInputStream("Hello world!", UTF_8));
        layeredStore.writeFile("a/b/c/test2.txt", toInputStream("Hello again!", UTF_8));
        layerManager.newTopLayer();
        layeredStore.createDirectory("x/y/z");
        layeredStore.writeFile("x/y/z/file1.txt", toInputStream("Layer 2 file", UTF_8));
        layerManager.newTopLayer();
        layeredStore.createDirectory("m/n");
        layeredStore.writeFile("m/n/file2.txt", toInputStream("Layer 3 file", UTF_8));
        assertThatCode(layeredItemStore::checkSameLayersOnStorageAndDb).doesNotThrowAnyException();
    }

    @Test
    public void should_fail_and_report_all_inconsistencies() throws Exception {
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir), new DirectLayerArchiver());
        var layeredStore = new LayeredItemStore(db, layerManager);
        var layeredItemStore = new LayeredItemStore(db, layerManager);
        Files.createDirectories(archiveDir);
        layeredStore.createDirectory("a/b/c/d");
        layeredStore.writeFile("a/b/c/d/test1.txt", toInputStream("Hello world!", UTF_8));
        layeredStore.writeFile("a/b/c/test2.txt", toInputStream("Hello again!", UTF_8));
        // Simulate a manual change in the database that is not reflected in the store
        var nonExistentLayerIdInStore1 = layerManager.getTopLayer().getId() + 1;
        var nonExistentLayerIdInStore2 = layerManager.getTopLayer().getId() + 2;
        daoTestExtension.inTransaction(() -> {
            var itemRecord1 = ItemRecord.builder()
                .path("a/b/c/d/test1.txt")
                .type(Type.File)
                .layerId(nonExistentLayerIdInStore1)
                .build();
            var itemRecord2 = ItemRecord.builder()
                .path("a/b/c/test2.txt")
                .type(Type.File)
                .layerId(nonExistentLayerIdInStore2)
                .build();
            db.saveRecords(itemRecord1, itemRecord2);
        });
        // Simulate a manual change in the store that is not reflected in the database
        var nonExistentLayerIdInDb1 = layerManager.getTopLayer().getId() + 3;
        var nonExistentLayerIdInDb2 = layerManager.getTopLayer().getId() + 4;
        var newStagingDir = stagingDir.resolve(Long.toString(nonExistentLayerIdInDb1));
        Files.createDirectories(newStagingDir);
        var newArchivedLayer = archiveDir.resolve(Long.toString(nonExistentLayerIdInDb2) + ".zip");
        Files.createFile(newArchivedLayer);
        // Expect an IllegalStateException to be thrown
        assertThatThrownBy(layeredItemStore::checkSameLayersOnStorageAndDb)
            .hasMessageContaining("Layer IDs are inconsistent between database and storage. Missing in database: [" + nonExistentLayerIdInDb1 + ", "
                + nonExistentLayerIdInDb2 + "] Missing on storage: [" + nonExistentLayerIdInStore1 + ", " + nonExistentLayerIdInStore2 + "]");

    }
}
