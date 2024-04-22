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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class LayerManagerConstructorTest extends AbstractLayerDatabaseTest {

    @Test
    public void should_create_the_staging_root() throws IOException {
        assertThat(stagingDir).doesNotExist();
        new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir));
        assertThat(stagingDir).isEmptyDirectory();
    }

    @Test
    public void should_use_the_existing_staging_directory() throws IOException {
        Files.createDirectories(stagingDir.resolve("1234567890123"));

        var topLayerId = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir))
            .getTopLayer().getId();
        assertThat(topLayerId).isEqualTo(1234567890123L);
    }

    @Test
    public void should_throw_on_a_regular_file_in_the_staging_root() throws IOException {
        Files.createDirectories(stagingDir);
        Files.createFile(stagingDir.resolve("1234567890123"));

        assertThatThrownBy ( () -> new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir)))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Not a directory: target/test/LayerManagerConstructorTest/layer_staging/1234567890123");
    }

    @Test
    public void should_throw_on_staging_directory_name_that_is_too_short_to_be_a_recent_timestamp() throws IOException {
        Files.createDirectories(stagingDir.resolve("123456789012"));

        assertThatThrownBy ( () -> new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir)))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Not a timestamp: target/test/LayerManagerConstructorTest/layer_staging/123456789012");
    }

    @Test
    public void should_throw_on_staging_directory_name_that_contains_letters() throws IOException {
        Files.createDirectories(stagingDir.resolve("abcdefghijklmnopqrstuvwxyz"));

        assertThatThrownBy ( () -> new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir)))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Not a timestamp: target/test/LayerManagerConstructorTest/layer_staging/abcdefghijklmnopqrstuvwxyz");
    }

    @Test
    public void should_throw_on_staging_directory_name_that_contains_decimal_point() throws IOException {
        Files.createDirectories(stagingDir.resolve("1.2"));

        assertThatThrownBy ( () -> new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir)))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Not a timestamp: target/test/LayerManagerConstructorTest/layer_staging/1.2");
    }
}
