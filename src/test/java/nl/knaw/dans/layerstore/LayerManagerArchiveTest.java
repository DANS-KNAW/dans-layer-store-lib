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

import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LayerManagerArchiveTest extends AbstractTestWithTestDir {
    @Test
    void should_throw_when_layer_id_does_not_exist() throws IOException {
        var layerManager = new LayerManagerImpl(stagingRoot, new ZipArchiveProvider(archiveRoot), new DirectLayerArchiver());

        assertThatThrownBy(() -> layerManager.archive(1234567890123L, false))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("No layer found with id 1234567890123");
    }

    @Test
    void should_archive_layer_by_id() throws Exception {
        Files.createDirectories(archiveRoot);
        var layerManager = new LayerManagerImpl(stagingRoot, new ZipArchiveProvider(archiveRoot), new DirectLayerArchiver());
        layerManager.newTopLayer();
        var layer = layerManager.getTopLayer();
        layer.close();

        layerManager.archive(layer.getId(), false);

        assertThat(archiveRoot.resolve(layer.getId() + ".zip")).exists();
    }

    @Test
    void should_reject_already_archived_layer_when_overwrite_is_false() throws Exception {
        Files.createDirectories(archiveRoot);
        var layerManager = new LayerManagerImpl(stagingRoot, new ZipArchiveProvider(archiveRoot), new DirectLayerArchiver());
        layerManager.newTopLayer();
        var layer = layerManager.getTopLayer();
        layer.close();
        layerManager.archive(layer.getId(), false);

        assertThatThrownBy(() -> layerManager.archive(layer.getId(), false))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("already archived");
    }

    @Test
    void should_delegate_archive_action_with_layer_id_to_custom_archiver() throws Exception {
        Files.createDirectories(archiveRoot);
        var delegated = new AtomicBoolean();
        var customArchiver = (LayerArchiver) (layerId, archiveAction) -> {
            assertThat(layerId).isPositive();
            delegated.set(true);
            archiveAction.run();
        };
        var layerManager = new LayerManagerImpl(stagingRoot, new ZipArchiveProvider(archiveRoot), customArchiver);
        layerManager.newTopLayer();
        var layer = layerManager.getTopLayer();
        layer.close();

        layerManager.archive(layer.getId(), false);

        assertThat(delegated).isTrue();
        assertThat(archiveRoot.resolve(layer.getId() + ".zip")).exists();
    }
}
