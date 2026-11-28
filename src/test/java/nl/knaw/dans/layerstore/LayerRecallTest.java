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

import static org.assertj.core.api.Assertions.assertThat;

public class LayerRecallTest extends AbstractTestWithTestDir {
    private static final long LAYER_ID = 1234567890123L;

    @Test
    public void should_forward_recall_to_archive_when_archived() throws Exception {
        createArchiveRoot();
        var archiveProvider = new ControllableZipArchiveProvider(archiveRoot);
        var layer = new LayerImpl(LAYER_ID, new StagingDir(stagingDir), archiveProvider.createArchive(LAYER_ID), () -> archiveProvider.exists(LAYER_ID));
        createEmptyStagingFiles("file1");
        layer.close();
        layer.archive(false);
        archiveProvider.recalled = false;

        layer.requestRecall();

        assertThat(archiveProvider.recallRequests).hasValue(1);
        assertThat(layer.isRecalled()).isFalse();
        archiveProvider.recalled = true;
        assertThat(layer.isRecalled()).isTrue();
    }

    @Test
    public void should_ignore_recall_when_not_archived() throws Exception {
        createArchiveRoot();
        var archiveProvider = new ControllableZipArchiveProvider(archiveRoot);
        var layer = new LayerImpl(LAYER_ID, new StagingDir(stagingDir), archiveProvider.createArchive(LAYER_ID), () -> archiveProvider.exists(LAYER_ID));
        createStagingDir();
        layer.close();
        archiveProvider.recalled = false;

        layer.requestRecall();

        assertThat(archiveProvider.recallRequests).hasValue(0);
        assertThat(layer.isRecalled()).isTrue();
    }

    @Test
    public void should_report_recalled_by_default_for_archives_that_are_always_available() throws Exception {
        createArchiveRoot();
        var archiveFile = archiveRoot.resolve(LAYER_ID + ".zip");
        var layer = new LayerImpl(LAYER_ID, new StagingDir(stagingDir), new ZipArchive(archiveFile), () -> archiveFile.toFile().exists());
        createEmptyStagingFiles("file1");
        layer.close();
        layer.archive(false);

        layer.requestRecall();

        assertThat(layer.getState()).isEqualTo(LayerState.ARCHIVED);
        assertThat(layer.isRecalled()).isTrue();
    }
}
