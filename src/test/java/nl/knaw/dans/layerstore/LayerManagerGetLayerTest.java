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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.io.IOUtils.toInputStream;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class LayerManagerGetLayerTest extends AbstractTestWithTestDir {

    @Test
    public void should_throw_when_layer_id_does_not_exist() throws IOException {
        // Given
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveRoot), new DirectLayerArchiver());

        // When
        assertThatThrownBy(() -> layerManager.getLayer(1234567890123L))
            // Then
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("No layer found with id 1234567890123");

    }

    @Test
    public void should_find_a_top_layer_with_content() throws IOException {
        // Given
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveRoot), new DirectLayerArchiver());
        var topLayer = layerManager.newTopLayer();
        topLayer.writeFile("test.txt", toInputStream("Hello world!", UTF_8));

        // When
        var layer = layerManager.getLayer(topLayer.getId());

        // Then
        assertThat(layer.getId()).isEqualTo(layerManager.getTopLayer().getId());
    }

    @Test
    public void should_find_an_empty_top_layer() throws IOException {
        // Given
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveRoot), new DirectLayerArchiver());
        long topLayerId = layerManager.newTopLayer().getId();

        // When
        var layer = layerManager.getLayer(topLayerId);

        // Then
        assertThat(layer.getId()).isEqualTo(layerManager.getTopLayer().getId());
    }

    @Test
    public void should_find_an_empty_archived_layer() throws Exception {
        // Given
        Files.createDirectories(archiveRoot);
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveRoot), new DirectLayerArchiver());
        var initialLayerId = layerManager.newTopLayer().getId();
        assertThat(archiveRoot).isEmptyDirectory(); // nothing archived yet

        // When
        layerManager.newTopLayer();

        // Then
        assertThat(initialLayerId)
            .withFailMessage("The initial top layer should have a different id than the new top layer")
            .isNotEqualTo(layerManager.getTopLayer().getId());
        assertThat(archiveRoot).isNotEmptyDirectory();
    }

    @Test
    public void should_have_status_archived_for_the_found_layer() throws Exception {
        // Given
        Files.createDirectories(archiveRoot);
        var layerManager = new LayerManagerImpl(stagingDir, new TarArchiveProvider(archiveRoot), new DirectLayerArchiver());
        layerManager.newTopLayer().writeFile("test.txt", toInputStream("Hello world!", UTF_8));
        Layer initialLayer = layerManager.getTopLayer();
        var initialLayerId = initialLayer.getId();
        layerManager.newTopLayer(); // This will archive the top layer

        // When
        var layer = layerManager.getLayer(initialLayerId); // Get the archived layer

        // Then
        assertThat(initialLayerId).isEqualTo(layer.getId());
        assertThat(archiveRoot).isNotEmptyDirectory();
        assertThat(archiveRoot.resolve(initialLayerId + ".tar")).exists();

        // Both represent the same layer
        assertThat(initialLayer.isArchived()).isTrue();
        assertThat(layer.isArchived()).isTrue();
    }

}
