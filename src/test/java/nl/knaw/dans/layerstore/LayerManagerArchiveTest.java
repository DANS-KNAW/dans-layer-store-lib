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
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LayerManagerArchiveTest extends AbstractTestWithTestDir {
    @Test
    void should_throw_when_layer_id_does_not_exist() throws IOException {
        var layerManager = new LayerManagerImpl(stagingRoot, new ZipArchiveProvider(archiveRoot));

        assertThatThrownBy(() -> layerManager.archive(1234567890123L, false))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("No layer found with id 1234567890123");
    }

    @Test
    void should_archive_layer_by_id() throws Exception {
        createArchiveRoot();
        var layerManager = new LayerManagerImpl(stagingRoot, new ZipArchiveProvider(archiveRoot));
        layerManager.newTopLayer();
        var layer = layerManager.getTopLayer();
        layer.close();

        layerManager.archive(layer.getId(), false);

        assertThat(archiveRoot.resolve(layer.getId() + ".zip")).exists();
    }

    @Test
    void should_reject_already_archived_layer_when_overwrite_is_false() throws Exception {
        createArchiveRoot();
        var layerManager = new LayerManagerImpl(stagingRoot, new ZipArchiveProvider(archiveRoot));
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
        createArchiveRoot();
        var delegated = new AtomicBoolean();
        var customArchiver = (LayerArchiver) (layerId, overwrite) -> {
            assertThat(layerId).isPositive();
            delegated.set(true);
        };
        var layerManager = new LayerManagerImpl(stagingRoot, new ZipArchiveProvider(archiveRoot), customArchiver, true);
        layerManager.newTopLayer();
        var layer = layerManager.getTopLayer();
        layer.close();

        layerManager.archive(layer.getId(), false);

        assertThat(delegated).isTrue();
    }

    @Test
    void should_pass_overwrite_flag_to_archiver() throws Exception {
        createArchiveRoot();
        var received = new java.util.ArrayList<Boolean>();
        var archiver = new LayerArchiver() {
            @Override
            public void archive(long layerId, boolean overwrite) {
                received.add(overwrite);
            }
        };
        var layerManager = new LayerManagerImpl(stagingRoot, new ZipArchiveProvider(archiveRoot), archiver, true);
        layerManager.newTopLayer();
        var layer = layerManager.getTopLayer();
        layer.close();

        layerManager.archive(layer.getId(), true);
        layerManager.archive(layer.getId(), false);

        assertThat(received).containsExactly(true, false);
    }
}
