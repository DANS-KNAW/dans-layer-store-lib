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

public class ArchiveRootValidationTest extends AbstractTestWithTestDir {

    @Test
    public void zip_archive_provider_should_throw_on_illegal_files() throws IOException {
        Files.createDirectories(archiveRoot);
        Files.createFile(archiveRoot.resolve("illegal.txt"));
        var provider = new ZipArchiveProvider(archiveRoot);

        assertThatThrownBy(provider::validateRoot)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Archive root")
            .hasMessageContaining("contains illegal files")
            .hasMessageContaining("illegal.txt");
    }

    @Test
    public void zip_archive_provider_should_throw_on_directory() throws IOException {
        Files.createDirectories(archiveRoot);
        Files.createDirectories(archiveRoot.resolve("1234567890123.zip"));
        var provider = new ZipArchiveProvider(archiveRoot);

        assertThatThrownBy(provider::validateRoot)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Archive root")
            .hasMessageContaining("contains illegal files")
            .hasMessageContaining("1234567890123.zip");
    }

    @Test
    public void zip_archive_provider_should_throw_on_invalid_timestamp() throws IOException {
        Files.createDirectories(archiveRoot);
        Files.createFile(archiveRoot.resolve("123.zip"));
        var provider = new ZipArchiveProvider(archiveRoot);

        assertThatThrownBy(provider::validateRoot)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Archive root")
            .hasMessageContaining("contains illegal files")
            .hasMessageContaining("123.zip");
    }

    @Test
    public void zip_archive_provider_should_pass_on_valid_files() throws IOException {
        Files.createDirectories(archiveRoot);
        Files.createFile(archiveRoot.resolve("1234567890123.zip"));
        Files.createFile(archiveRoot.resolve("9876543210987.zip"));
        var provider = new ZipArchiveProvider(archiveRoot);

        provider.validateRoot();
    }

    @Test
    public void tar_archive_provider_should_throw_on_illegal_files() throws IOException {
        Files.createDirectories(archiveRoot);
        Files.createFile(archiveRoot.resolve("illegal.txt"));
        var provider = new TarArchiveProvider(archiveRoot);

        assertThatThrownBy(provider::validateRoot)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Archive root")
            .hasMessageContaining("contains illegal files")
            .hasMessageContaining("illegal.txt");
    }

    @Test
    public void tar_archive_provider_should_pass_on_valid_files() throws IOException {
        Files.createDirectories(archiveRoot);
        Files.createFile(archiveRoot.resolve("1234567890123.tar"));
        var provider = new TarArchiveProvider(archiveRoot);

        provider.validateRoot();
    }
}
