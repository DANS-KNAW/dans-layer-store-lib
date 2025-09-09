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
        // Given
        Files.createDirectories(archiveDir);
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir), new DirectLayerArchiver());
        var layeredItemStore = new LayeredItemStore(db, layerManager);

        // When / Then
        assertThatCode(layeredItemStore::checkSameLayersOnStorageAndDb).doesNotThrowAnyException();
    }

    @Test
    public void should_pass_when_store_and_database_are_in_sync() throws Exception {
        // Given
        Files.createDirectories(archiveDir);
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir), new DirectLayerArchiver());
        var layeredItemStore = new LayeredItemStore(db, layerManager);
        layerManager.newTopLayer();
        var anotherInstance = new LayeredItemStore(db, layerManager);

        layeredItemStore.createDirectory("a/b/c/d");
        layeredItemStore.writeFile("a/b/c/d/test1.txt", toInputStream("Hello world!", UTF_8));
        layeredItemStore.writeFile("a/b/c/test2.txt", toInputStream("Hello again!", UTF_8));

        // When / Then
        assertThatCode(anotherInstance::checkSameLayersOnStorageAndDb).doesNotThrowAnyException();
    }

    @Test
    public void should_fail_when_database_contains_layer_missing_on_storage() throws Exception {
        // Given
        Files.createDirectories(archiveDir);
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir), new DirectLayerArchiver());
        var layeredItemStore = new LayeredItemStore(db, layerManager);
        layeredItemStore.newTopLayer();
        var checker = new LayeredItemStore(db, layerManager);

        layeredItemStore.createDirectory("a/b/c/d");
        layeredItemStore.writeFile("a/b/c/d/test1.txt", toInputStream("Hello world!", UTF_8));
        layeredItemStore.writeFile("a/b/c/test2.txt", toInputStream("Hello again!", UTF_8));

        var nonExistentLayerId = layerManager.getTopLayer().getId() + 1;
        daoTestExtension.inTransaction(() -> {
            var itemRecord = ItemRecord.builder()
                .path("a/b/c/d/test1.txt")
                .type(Type.File)
                .layerId(nonExistentLayerId)
                .build();
            db.saveRecords(itemRecord);
        });

        // When / Then
        assertThatThrownBy(checker::checkSameLayersOnStorageAndDb)
            .hasMessageContaining("Layer IDs are inconsistent between database and storage. Missing on storage: [" + nonExistentLayerId + "]");
    }

    @Test
    public void should_fail_when_staging_contains_layer_missing_in_database() throws Exception {
        // Given
        Files.createDirectories(archiveDir);
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir), new DirectLayerArchiver());
        var layeredItemStore = new LayeredItemStore(db, layerManager);
        layeredItemStore.newTopLayer();
        var checker = new LayeredItemStore(db, layerManager);

        layeredItemStore.createDirectory("a/b/c/d");
        layeredItemStore.writeFile("a/b/c/d/test1.txt", toInputStream("Hello world!", UTF_8));
        layeredItemStore.writeFile("a/b/c/test2.txt", toInputStream("Hello again!", UTF_8));

        var nonExistentLayerId = layerManager.getTopLayer().getId() + 1;
        var newStagingDir = stagingDir.resolve(Long.toString(nonExistentLayerId));
        Files.createDirectories(newStagingDir);

        // When / Then
        assertThatThrownBy(checker::checkSameLayersOnStorageAndDb)
            .hasMessageContaining("Layer IDs are inconsistent between database and storage. Missing in database: [" + nonExistentLayerId + "]");
    }

    @Test
    public void should_fail_when_archive_contains_layer_missing_in_database() throws Exception {
        // Given
        Files.createDirectories(archiveDir);
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir), new DirectLayerArchiver());
        var layeredItemStore = new LayeredItemStore(db, layerManager);
        layeredItemStore.newTopLayer();
        var checker = new LayeredItemStore(db, layerManager);

        layeredItemStore.createDirectory("a/b/c/d");
        layeredItemStore.writeFile("a/b/c/d/test1.txt", toInputStream("Hello world!", UTF_8));
        layeredItemStore.writeFile("a/b/c/test2.txt", toInputStream("Hello again!", UTF_8));

        var nonExistentLayerId = layerManager.getTopLayer().getId() + 1;
        var newArchivedLayer = archiveDir.resolve(Long.toString(nonExistentLayerId) + ".zip");
        Files.createFile(newArchivedLayer);

        // When / Then
        assertThatThrownBy(checker::checkSameLayersOnStorageAndDb)
            .hasMessageContaining("Layer IDs are inconsistent between database and storage. Missing in database: [" + nonExistentLayerId + "]");
    }

    @Test
    public void should_pass_when_multiple_layers_exist_and_store_is_in_sync_with_database() throws Exception {
        // Given
        Files.createDirectories(archiveDir);
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir), new DirectLayerArchiver());
        var layeredItemStore = new LayeredItemStore(db, layerManager);
        layerManager.newTopLayer();
        var checker = new LayeredItemStore(db, layerManager);

        layeredItemStore.createDirectory("a/b/c/d");
        layeredItemStore.writeFile("a/b/c/d/test1.txt", toInputStream("Hello world!", UTF_8));
        layeredItemStore.writeFile("a/b/c/test2.txt", toInputStream("Hello again!", UTF_8));
        layerManager.newTopLayer();
        layeredItemStore.createDirectory("x/y/z");
        layeredItemStore.writeFile("x/y/z/file1.txt", toInputStream("Layer 2 file", UTF_8));
        layerManager.newTopLayer();
        layeredItemStore.createDirectory("m/n");
        layeredItemStore.writeFile("m/n/file2.txt", toInputStream("Layer 3 file", UTF_8));

        // When / Then
        assertThatCode(checker::checkSameLayersOnStorageAndDb).doesNotThrowAnyException();
    }

    @Test
    public void should_fail_and_report_all_missing_layers_on_both_sides() throws Exception {
        // Given
        Files.createDirectories(archiveDir);
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir), new DirectLayerArchiver());
        var layeredItemStore = new LayeredItemStore(db, layerManager);
        layerManager.newTopLayer();
        var checker = new LayeredItemStore(db, layerManager);

        layeredItemStore.createDirectory("a/b/c/d");
        layeredItemStore.writeFile("a/b/c/d/test1.txt", toInputStream("Hello world!", UTF_8));
        layeredItemStore.writeFile("a/b/c/test2.txt", toInputStream("Hello again!", UTF_8));

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

        var nonExistentLayerIdInDb1 = layerManager.getTopLayer().getId() + 3;
        var nonExistentLayerIdInDb2 = layerManager.getTopLayer().getId() + 4;
        var newStagingDir = stagingDir.resolve(Long.toString(nonExistentLayerIdInDb1));
        Files.createDirectories(newStagingDir);
        var newArchivedLayer = archiveDir.resolve(Long.toString(nonExistentLayerIdInDb2) + ".zip");
        Files.createFile(newArchivedLayer);

        // When / Then
        assertThatThrownBy(checker::checkSameLayersOnStorageAndDb)
            .hasMessageContaining("Layer IDs are inconsistent between database and storage. Missing in database: [" + nonExistentLayerIdInDb1 + ", "
                + nonExistentLayerIdInDb2 + "] Missing on storage: [" + nonExistentLayerIdInStore1 + ", " + nonExistentLayerIdInStore2 + "]");
    }
}
