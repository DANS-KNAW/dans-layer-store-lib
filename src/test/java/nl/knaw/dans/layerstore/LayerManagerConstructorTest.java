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

import io.dropwizard.util.DirectExecutorService;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class LayerManagerConstructorTest extends AbstractLayerDatabaseTest {

    @Test
    public void should_create_the_staging_root() throws IOException {
        assertThat(stagingDir).doesNotExist();
        new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir), new DirectLayerArchiver());
        assertThat(stagingDir).isEmptyDirectory();
    }

    @Test
    public void should_use_the_existing_staging_directory() throws IOException {
        var existingLayerId = 1234567890123L;
        Files.createDirectories(stagingDir.resolve(String.valueOf(existingLayerId)));

        var topLayerId = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir), new DirectLayerArchiver())
            .getTopLayer().getId();
        assertThat(topLayerId).isEqualTo(existingLayerId);
    }

    @Test
    public void should_throw_on_a_regular_file_in_the_staging_root() throws IOException {
        Files.createDirectories(stagingDir);
        var invalidFileBetweenLayerDirs = "1234567890123";
        Files.createFile(stagingDir.resolve(invalidFileBetweenLayerDirs));

        assertThatThrownBy(() -> new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir), new DirectLayerArchiver()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Not a directory: target/test/LayerManagerConstructorTest/layer_staging/" + invalidFileBetweenLayerDirs);
    }

    @Test
    public void should_throw_on_staging_directory_name_that_is_too_short_to_be_a_recent_timestamp() throws IOException {
        var existingLayerDir = "123456789012";
        Files.createDirectories(stagingDir.resolve(existingLayerDir));

        assertThatThrownBy(() -> new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir), new DirectLayerArchiver()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Not a timestamp: target/test/LayerManagerConstructorTest/layer_staging/" + existingLayerDir);
    }

    @Test
    public void should_throw_on_staging_directory_name_that_contains_letters() throws IOException {
        var invalidDirBetweenLayerDirs = "abcdefghijklmnopqrstuvwxyz";
        Files.createDirectories(stagingDir.resolve(invalidDirBetweenLayerDirs));

        assertThatThrownBy(() -> new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir), new DirectLayerArchiver()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Not a timestamp: target/test/LayerManagerConstructorTest/layer_staging/" + invalidDirBetweenLayerDirs);
    }

    @Test
    public void should_throw_on_staging_directory_name_that_contains_decimal_point() throws IOException {
        var invalidDirBetweenLayerDirs = "1.2";
        Files.createDirectories(stagingDir.resolve(invalidDirBetweenLayerDirs));

        assertThatThrownBy(() -> new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir), new DirectLayerArchiver()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Not a timestamp: target/test/LayerManagerConstructorTest/layer_staging/" + invalidDirBetweenLayerDirs);
    }
}
