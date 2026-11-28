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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.io.IOUtils.toInputStream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class LayeredItemStoreReopenLayerTest extends AbstractLayerDatabaseTest {

    private LayeredItemStore createStore(ArchiveProvider archiveProvider, LayerArchiver layerArchiver) throws Exception {
        createArchiveRoot();
        return new LayeredItemStore(db, new LayerManagerImpl(stagingRoot, archiveProvider, layerArchiver, true));
    }

    @Test
    public void should_reopen_archived_layer_while_top_layer_stays_writable() throws Exception {
        var store = createStore(new ZipArchiveProvider(archiveRoot), null);
        var oldLayerId = store.newTopLayer();
        store.writeFile("file1", toInputStream("old content", UTF_8));
        var topLayerId = store.newTopLayer();
        assertThat(store.getLayerState(oldLayerId)).isEqualTo(LayerState.ARCHIVED);

        store.reopenLayer(oldLayerId);

        assertThat(store.getLayerState(oldLayerId)).isEqualTo(LayerState.OPEN);
        assertThat(stagingRoot.resolve(oldLayerId + "/file1")).hasContent("old content");
        assertThat(store.getTopLayerId()).isEqualTo(topLayerId);
        store.writeFile("file2", toInputStream("new content", UTF_8));
        assertThat(stagingRoot.resolve(topLayerId + "/file2")).hasContent("new content");
        assertThat(stagingRoot.resolve(oldLayerId + "/file2")).doesNotExist();
    }

    @Test
    public void should_reopen_closed_layer() throws Exception {
        // An archiver that does nothing leaves the old top layer CLOSED
        var store = createStore(new ZipArchiveProvider(archiveRoot), (layerId, overwrite) -> {
        });
        var oldLayerId = store.newTopLayer();
        store.newTopLayer();
        assertThat(store.getLayerState(oldLayerId)).isEqualTo(LayerState.CLOSED);

        store.reopenLayer(oldLayerId);

        assertThat(store.getLayerState(oldLayerId)).isEqualTo(LayerState.OPEN);
    }

    @Test
    public void should_refuse_to_reopen_top_layer() throws Exception {
        var store = createStore(new ZipArchiveProvider(archiveRoot), null);
        var topLayerId = store.newTopLayer();

        assertThatThrownBy(() -> store.reopenLayer(topLayerId))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("top layer");
        assertThat(store.getLayerState(topLayerId)).isEqualTo(LayerState.OPEN);
    }

    @Test
    public void should_refuse_to_reopen_layer_that_is_already_open() throws Exception {
        var store = createStore(new ZipArchiveProvider(archiveRoot), null);
        var oldLayerId = store.newTopLayer();
        store.newTopLayer();
        store.reopenLayer(oldLayerId);

        assertThatThrownBy(() -> store.reopenLayer(oldLayerId))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("must be in state CLOSED or ARCHIVED");
    }

    @Test
    public void should_throw_for_unknown_layer() throws Exception {
        var store = createStore(new ZipArchiveProvider(archiveRoot), null);
        store.newTopLayer();

        assertThatThrownBy(() -> store.reopenLayer(123L)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void should_refuse_concurrent_reopen_of_same_layer_and_serve_reads_from_archive_meanwhile() throws Exception {
        var archiveProvider = new ControllableZipArchiveProvider(archiveRoot);
        var store = createStore(archiveProvider, null);
        var oldLayerId = store.newTopLayer();
        store.writeFile("file1", toInputStream("old content", UTF_8));
        store.newTopLayer();
        archiveProvider.holdUpUnarchive = true;

        var firstReopen = CompletableFuture.runAsync(() -> {
            try {
                store.reopenLayer(oldLayerId);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        assertThat(archiveProvider.unarchiveStarted.await(10, TimeUnit.SECONDS)).isTrue();

        assertThatThrownBy(() -> store.reopenLayer(oldLayerId))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("already being reopened");
        assertThat(store.getLayerState(oldLayerId)).isEqualTo(LayerState.ARCHIVED);
        try (var in = store.readFile("file1")) {
            assertThat(in).hasContent("old content");
        }

        archiveProvider.unarchiveMayProceed.countDown();
        firstReopen.get(10, TimeUnit.SECONDS);
        assertThat(store.getLayerState(oldLayerId)).isEqualTo(LayerState.OPEN);
    }

    @Test
    public void should_allow_new_reopen_after_failed_one() throws Exception {
        var archiveProvider = new ControllableZipArchiveProvider(archiveRoot);
        var store = createStore(archiveProvider, null);
        var oldLayerId = store.newTopLayer();
        store.newTopLayer();
        // Make the unarchiving fail by corrupting the archive file
        var archiveFile = archiveRoot.resolve(oldLayerId + ".zip");
        var archiveBytes = Files.readAllBytes(archiveFile);
        Files.write(archiveFile, new byte[] { 1, 2, 3 });

        assertThatThrownBy(() -> store.reopenLayer(oldLayerId)).isInstanceOf(Exception.class);
        assertThat(store.getLayerState(oldLayerId)).isEqualTo(LayerState.ARCHIVED);

        Files.write(archiveFile, archiveBytes);
        store.reopenLayer(oldLayerId);
        assertThat(store.getLayerState(oldLayerId)).isEqualTo(LayerState.OPEN);
    }

    @Test
    public void should_delegate_recall_to_archive_of_archived_layer() throws Exception {
        var archiveProvider = new ControllableZipArchiveProvider(archiveRoot);
        var store = createStore(archiveProvider, null);
        var oldLayerId = store.newTopLayer();
        store.newTopLayer();
        archiveProvider.recalled = false;

        store.requestLayerRecall(oldLayerId);

        assertThat(archiveProvider.recallRequests).hasValue(1);
        assertThat(store.isLayerRecalled(oldLayerId)).isFalse();
        archiveProvider.recalled = true;
        assertThat(store.isLayerRecalled(oldLayerId)).isTrue();
    }

    @Test
    public void should_not_request_recall_for_layer_that_is_not_archived() throws Exception {
        var archiveProvider = new ControllableZipArchiveProvider(archiveRoot);
        var store = createStore(archiveProvider, null);
        var topLayerId = store.newTopLayer();
        archiveProvider.recalled = false;

        store.requestLayerRecall(topLayerId);

        assertThat(archiveProvider.recallRequests).hasValue(0);
        assertThat(store.isLayerRecalled(topLayerId)).isTrue();
    }
}
