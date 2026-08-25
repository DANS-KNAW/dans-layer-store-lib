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

public class LayerManagerArchiveTest extends AbstractTestWithTestDir {

    @Test
    public void should_throw_when_layer_id_does_not_exist() throws IOException {
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveRoot), new DirectLayerArchiver());

        assertThatThrownBy(() -> layerManager.archive(1234567890123L, false))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("No layer found with id 1234567890123");
    }

    @Test
    public void should_archive_layer_by_id() throws Exception {
        Files.createDirectories(archiveRoot);
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveRoot), new DirectLayerArchiver());
        layerManager.newTopLayer();
        var topLayer = layerManager.getTopLayer();
        long topLayerId = topLayer.getId();
        topLayer.close();

        layerManager.archive(topLayerId, false);

        assertThat(archiveRoot.resolve(topLayerId + ".zip")).exists();
    }

    @Test
    public void should_throw_when_already_archived_and_overwrite_false() throws Exception {
        Files.createDirectories(archiveRoot);
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveRoot), new DirectLayerArchiver());
        layerManager.newTopLayer();
        var topLayer = layerManager.getTopLayer();
        long topLayerId = topLayer.getId();
        topLayer.close();
        layerManager.archive(topLayerId, false);

        assertThatThrownBy(() -> layerManager.archive(topLayerId, false))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("already archived");
    }

    @Test
    public void should_delegate_to_custom_layer_archiver_with_layer_id() throws Exception {
        Files.createDirectories(archiveRoot);
        var delegated = new AtomicBoolean(false);
        var customArchiver = new LayerArchiver() {

            @Override
            public void archive(long layerId, Runnable archiveAction) {
                delegated.set(true);
                archiveAction.run();
            }
        };

        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveRoot), customArchiver);
        layerManager.newTopLayer();
        var topLayer = layerManager.getTopLayer();
        long topLayerId = topLayer.getId();
        topLayer.close();

        layerManager.archive(topLayerId, false);

        assertThat(delegated.get()).isTrue();
        assertThat(archiveRoot.resolve(topLayerId + ".zip")).exists();
    }
}
