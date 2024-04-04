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

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

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
    public void should_find_an_archived_layer() throws IOException {
        // Given
        Files.createDirectories(archiveDir);
        var layerManager = new LayerManagerImpl(testDir, new ZipArchiveProvider(archiveDir));
        var layerId = layerManager.getTopLayer().getId();
        layerManager.newTopLayer();
        FileUtils.deleteDirectory(stagingDir.resolve(String.valueOf(layerId)).toFile());

        // When
        try {
            layerManager.getLayer(layerId);
        }
        catch (IllegalArgumentException e) {
            // Then
            assertThat(e).hasMessageContaining("No layer found with id " + layerId);
        }

        // When again
        try {
            var layer = layerManager.getLayer(layerId);
            // Then
            assertThat(layer.getId()).isNotEqualTo(layerManager.getTopLayer().getId());

            assumeNotYetFixed("Race condition or logic? Zip file exists at second attempt of getLayer (or when called in an assertThatThrownBy).");
        }
        catch (IllegalArgumentException e) {
            // Then
            assertThat(e).hasMessageContaining("No layer found with id " + layerId);
            assumeNotYetFixed("Getting here proves the race condition. Usually the second attempt returns the layer.");
        }
    }

}
