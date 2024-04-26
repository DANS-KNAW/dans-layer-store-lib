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

import static java.lang.Thread.sleep;
import static nl.knaw.dans.layerstore.TestUtils.assumeNotYetFixed;
import static nl.knaw.dans.layerstore.TestUtils.toInputStream;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class LayerManagerGetLayerTest extends AbstractTestWithTestDir {

    @Test
    public void should_throw_when_layer_id_does_not_exist() throws IOException {
        // Given
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir));

        // When
        assertThatThrownBy(() -> layerManager.getLayer(1234567890123L))
            // Then
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("No layer found with id 1234567890123");

    }

    @Test
    public void should_find_garbage_in_stagingDir_created_after_creating_LayerManagerImpl_object() throws IOException {
        // Given
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir));
        Files.createDirectories(stagingDir.resolve("0"));

        //When
        var layer = layerManager.getLayer(0L);

        // Then
        assertThat(layer.getId()).isNotEqualTo(layerManager.getTopLayer().getId());
    }

    @Test
    public void should_find_a_top_layer_with_content() throws IOException {
        // Given
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir));
        var topLayer = layerManager.getTopLayer();
        topLayer.writeFile("test.txt", toInputStream("Hello world!"));

        // When
        var layer = layerManager.getLayer(topLayer.getId());

        // Then
        assertThat(layer.getId()).isEqualTo(layerManager.getTopLayer().getId());
    }

    @Test
    public void should_find_an_empty_top_layer() throws IOException {
        // Given
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir));
        long topLayerId = layerManager.getTopLayer().getId();
        assertThat(stagingDir.resolve(String.valueOf(topLayerId))).doesNotExist();

        // When
        var layer = layerManager.getLayer(topLayerId);

        // Then
        assertThat(layer.getId()).isEqualTo(layerManager.getTopLayer().getId());
    }

    @Test
    public void should_find_an_empty_archived_layer() throws Exception {
        // Given
        Files.createDirectories(archiveDir);
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir));
        var initialLayerId = layerManager.getTopLayer().getId();
        assertThat(stagingDir).isEmptyDirectory(); // no content in the current layer
        assertThat(archiveDir).isEmptyDirectory(); // nothing archived yet
        sleep(1L); // to make sure to get a layer with another time stamp
        layerManager.newTopLayer();
        assertThat(initialLayerId).isNotEqualTo(layerManager.getTopLayer().getId());
        assertThat(stagingDir).isEmptyDirectory();
        Thread.sleep(1L); // wait for creation of the zip file, strangely not required when running the test stand alone
        assertThat(archiveDir).isNotEmptyDirectory();

        // When
        var layer = layerManager.getLayer(initialLayerId);
        // Then

        assertThat(layer.getId()).isNotEqualTo(layerManager.getTopLayer().getId());
        assertThat(archiveDir).isNotEmptyDirectory();
    }

    @Test
    public void should_have_status_archived_for_the_found_layer() throws Exception {
        // Given
        Files.createDirectories(archiveDir);
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir));
        layerManager.getTopLayer().writeFile("test.txt", toInputStream("Hello world!"));
        Layer initialLayer = layerManager.getTopLayer();
        var initialLayerId = initialLayer.getId();
        sleep(1L); // to make sure to get a layer with another time stamp
        layerManager.newTopLayer();
        Thread.sleep(1L); // wait for creation of the zip file, strangely not required when running the test stand alone

        // When
        var layer = layerManager.getLayer(initialLayerId);

        // Then
        assertThat(initialLayerId).isEqualTo(layer.getId());
        assertThat(archiveDir).isNotEmptyDirectory();
        assertThat(archiveDir.resolve(initialLayerId + ".zip")).exists();
        assumeNotYetFixed("The layer is archived, but neither the object of the initial top layer, nor the new layer object know it.");
        assertFalse(initialLayer.isArchived());
        assertFalse(layer.isArchived());
    }

}
