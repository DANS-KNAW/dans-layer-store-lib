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

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class LayerManagerConstructorTest extends AbstractLayerDatabaseTest {

    @Test
    public void should_have_a_not_existing_directory_as_top_layer() {
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir));

        var topLayer = layerManager.getTopLayer();
        assertThat(stagingDir).isEmptyDirectory();
        assertThat(stagingDir.resolve(String.valueOf(topLayer.getId()))).doesNotExist();
    }

    @Test
    public void should_use_the_existing_directory_as_top_layer() throws Exception {
        Files.createDirectories(stagingDir.resolve("45"));
        Files.createDirectories(stagingDir.resolve("123"));
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir));
        var topLayerId = layerManager.getTopLayer().getId();
        assertThat(topLayerId).isEqualTo(123L);
    }

    @Test
    public void should_throw_a_NumberFormatException_for_the_existing_alphanumeric_directory() throws Exception {
        Files.createDirectories(stagingDir.resolve("12345"));
        Files.createDirectories(stagingDir.resolve("abc"));

        assertThatThrownBy(() -> new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir))).
            isInstanceOf(NumberFormatException.class)
            .hasMessage("For input string: \"abc\"");
    }

    @Test
    public void should_throw_a_NumberFormatException_for_the_existing_directory_with_a_dot_in_the_name() throws Exception {
        Files.createDirectories(stagingDir.resolve("1.2"));

        assertThatThrownBy(() -> new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir))).
            isInstanceOf(NumberFormatException.class)
            .hasMessage("For input string: \"1.2\"");
    }
}
