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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.io.IOUtils.toInputStream;
import static org.assertj.core.api.Assertions.assertThat;

public class LayerStaleInstanceStateTest extends AbstractTestWithTestDir {

    private LayerManagerImpl newManagerWithClosedLayer() throws Exception {
        createArchiveRoot();
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveRoot), (id, overwrite) -> { }, true);
        layerManager.newTopLayer();
        layerManager.getTopLayer().writeFile("test.txt", toInputStream("Hello", UTF_8));
        layerManager.getTopLayer().close();
        layerManager.newTopLayer();
        return layerManager;
    }

    @Test
    public void getState_of_instance_obtained_before_archiving_completes_returns_archived() throws Exception {
        var layerManager = newManagerWithClosedLayer();
        var layerId = layerManager.listLayerIds().get(0);
        var poller = layerManager.getLayer(layerId); // sees archived=false when created
        var worker = layerManager.getLayer(layerId);
        assertThat(poller.getState()).isEqualTo(LayerState.CLOSED);

        worker.archive(false);

        assertThat(poller.getState()).isEqualTo(LayerState.ARCHIVED);
    }

    @Test
    public void getState_of_stale_closed_instance_returns_open_after_other_instance_reopens() throws Exception {
        var layerManager = newManagerWithClosedLayer();
        var layerId = layerManager.listLayerIds().get(0);
        var stale = layerManager.getLayer(layerId);
        layerManager.getLayer(layerId).reopen();

        assertThat(stale.getState()).isEqualTo(LayerState.OPEN);
    }

    @Test
    public void archiving_leaves_no_deleting_directory() throws Exception {
        var layerManager = newManagerWithClosedLayer();
        var layerId = layerManager.listLayerIds().get(0);
        layerManager.getLayer(layerId).archive(false);

        try (var files = Files.list(stagingDir)) {
            assertThat(files.map(p -> p.getFileName().toString())).noneMatch(n -> n.endsWith(".deleting"));
        }
    }
}
