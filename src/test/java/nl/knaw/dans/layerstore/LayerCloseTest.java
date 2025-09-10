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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class LayerCloseTest extends AbstractTestWithTestDir {

    @Test
    public void throws_IllegalStateException_when_layer_is_already_closed() throws Exception {
        // Given
        var layer = new LayerImpl(1, stagingDir, new ZipArchive(archiveDir.resolve("test.zip")));
        Files.createDirectories(stagingDir);
        layer.close();

        // When / Then
        assertThatThrownBy(layer::close)
            .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void should_close_layer() throws Exception {
        // Given
        var layer = new LayerImpl(1, stagingDir, new ZipArchive(archiveDir.resolve("test.zip")));
        Files.createDirectories(stagingDir);

        // When
        layer.close();

        // Then
        assertThat(layer.isClosed()).isTrue();
    }

}
