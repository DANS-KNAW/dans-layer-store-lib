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

import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class LayeredItemStoreArchiveLayerDirectlyTest extends AbstractLayerDatabaseTest {

    @Test
    public void should_archive_layer_without_involving_the_layer_archiver() throws Exception {
        Files.createDirectories(archiveRoot);
        var archiverCalls = new AtomicInteger();
        LayerArchiver countingArchiver = (layerId, overwrite) -> {
            archiverCalls.incrementAndGet();
        };
        var layerManager = new LayerManagerImpl(stagingRoot, new ZipArchiveProvider(archiveRoot), countingArchiver, true);
        var layeredItemStore = new LayeredItemStore(db, layerManager);
        var layerId = layeredItemStore.newTopLayer();
        layeredItemStore.closeLayer(layerId);
        assertThat(layeredItemStore.isLayerArchived(layerId)).isFalse();

        layeredItemStore.archiveLayerDirectly(layerId, false);

        assertThat(archiverCalls).hasValue(0);
        assertThat(layeredItemStore.isLayerArchived(layerId)).isTrue();
    }

    @Test
    public void should_not_overwrite_existing_archive_when_overwrite_is_false() throws Exception {
        Files.createDirectories(archiveRoot);
        var layerManager = new LayerManagerImpl(stagingRoot, new ZipArchiveProvider(archiveRoot), null, true);
        var layeredItemStore = new LayeredItemStore(db, layerManager);
        var layerId = layeredItemStore.newTopLayer();
        layeredItemStore.closeLayer(layerId);
        var existingArchive = archiveRoot.resolve(layerId + ".zip");
        Files.writeString(existingArchive, "existing");

        assertThatThrownBy(() -> layeredItemStore.archiveLayerDirectly(layerId, false))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("already archived");
        assertThat(existingArchive).hasContent("existing");
    }
}
