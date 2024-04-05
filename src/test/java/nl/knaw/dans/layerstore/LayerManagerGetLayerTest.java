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

import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;

import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LayerManagerGetLayerTest extends AbstractTestWithTestDir {

    @Test
    public void should_throw_when_layer_id_does_not_exist() throws IOException {
        // Given
        var layerManager = new LayerManagerImpl(testDir, new ZipArchiveProvider(archiveDir));

        // When
        assertThatThrownBy(() -> layerManager.getLayer(1234567890123L))
            // Then
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("No layer found with id 1234567890123");

    }

    @Test
    public void should_find_a_directory_assumed_to_be_garbage_by_the_constructor() throws IOException {
        // Given
        Files.createDirectories(testDir.resolve("0"));
        var layerManager = new LayerManagerImpl(testDir, new ZipArchiveProvider(archiveDir));
        assertThat(layerManager.getTopLayer().getId()).isNotEqualTo(0L);

        //When
        var layer = layerManager.getLayer(0L);

        // Then
        assertThat(layer.getId()).isNotEqualTo(layerManager.getTopLayer().getId());
    }

    @Test
    public void should_find_a_top_layer_with_content() throws IOException {
        // Given
        var layerManager = new LayerManagerImpl(testDir, new ZipArchiveProvider(archiveDir));
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
        var layerManager = new LayerManagerImpl(testDir, new ZipArchiveProvider(archiveDir));
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
        var layerManager = new LayerManagerImpl(testDir, new ZipArchiveProvider(archiveDir));
        var initialLayerId = layerManager.getTopLayer().getId();
        assertThat(stagingDir).doesNotExist(); // no content in the current layer
        sleep(1L); // to make sure to get a layer with another time stamp
        layerManager.newTopLayer();
        AssertionsForClassTypes.assertThat(initialLayerId).isNotEqualTo(layerManager.getTopLayer().getId());
        assertThat(archiveDir).isEmptyDirectory();

        // When
        try {
            // an assertThatThrownBy archives the empty layer in time, and we would not get the exception
            layerManager.getLayer(initialLayerId);
        }
        catch (IllegalArgumentException e) {
            // Then
            assertThat(e).hasMessageContaining("No layer found with id " + initialLayerId);
            assertThat(archiveDir).isNotEmptyDirectory(); // apparently the empty layer was archived after returning from getLayer
        }

        // When again
        try {
            var layer = layerManager.getLayer(initialLayerId);
            // Then
            assertThat(layer.getId()).isNotEqualTo(layerManager.getTopLayer().getId());
            assertThat(archiveDir).isNotEmptyDirectory();

            assumeNotYetFixed("Race condition or logic? Zip file exists at second attempt of getLayer (or when called in an assertThatThrownBy).");
        }
        catch (IllegalArgumentException e) {
            // Then
            assertThat(e).hasMessageContaining("No layer found with id " + initialLayerId);
            assumeNotYetFixed("Getting here proves the race condition. Usually the second attempt returns the layer then we don't get here.");
        }
    }

    @Test
    public void should_have_status_archived_for_the_found_layer() throws Exception {
        // Given
        Files.createDirectories(archiveDir);
        var layerManager = new LayerManagerImpl(testDir, new ZipArchiveProvider(archiveDir));
        layerManager.getTopLayer().writeFile("test.txt", toInputStream("Hello world!"));
        Layer initialLayer = layerManager.getTopLayer();
        var initialLayerId = initialLayer.getId();
        sleep(1L); // to make sure to get a layer with another time stamp
        layerManager.newTopLayer();
        assertFalse(initialLayer.isArchived()); // TODO confusing, doesn't change when archived

        // When
        var layer1 = layerManager.getLayer(initialLayerId);

        // Then
        assertThat(initialLayerId).isEqualTo(layer1.getId());
        assumeNotYetFixed("different results when running the test stand alone or in a suite");
        assertThat(archiveDir).isEmptyDirectory();
        assertFalse(layer1.isArchived());

        // When again
        var layer2 = layerManager.getLayer(initialLayerId);

        // Then
        assertThat(initialLayerId).isEqualTo(layer2.getId());
        assertThat(archiveDir).isNotEmptyDirectory();
        assertTrue(layer2.isArchived());
    }

}
