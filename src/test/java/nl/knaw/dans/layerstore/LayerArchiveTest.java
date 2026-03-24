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

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.zip.ZipFile;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class LayerArchiveTest extends AbstractCapturingTest {
    @Test
    public void throws_IllegalStateException_when_layer_is_not_closed() throws Exception {
        // Given
        Files.createDirectories(stagingDir);
        var layer = new LayerImpl(1, new StagingDir(stagingDir), new ZipArchive(archiveRoot.resolve("test.zip")));

        // When / Then
        assertThatThrownBy(() -> layer.archive(false))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("must be in state CLOSED");
    }

    @Test
    public void throws_IllegalStateException_when_layer_is_already_archived() throws IOException {
        // Given
        Files.createDirectories(archiveRoot);
        Files.createDirectories(stagingDir);
        var layer = new LayerImpl(1, new StagingDir(stagingDir), new ZipArchive(archiveRoot.resolve("test.zip")));
        layer.close();
        layer.archive(false);

        // When / Then
        assertThatThrownBy(() -> layer.archive(false))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("must be in state CLOSED");
    }

    @Test
    public void throws_IllegalArgumentException_when_overwrite_is_false_and_archive_exists() throws IOException {
        // Given
        Files.createDirectories(archiveRoot);
        Files.createDirectories(stagingDir);
        var archiveFile = archiveRoot.resolve("test.zip");
        Files.createFile(archiveFile);
        var layer = new LayerImpl(1, new StagingDir(stagingDir), new ZipArchive(archiveFile));
        layer.close();

        // When / Then
        assertThatThrownBy(() -> layer.archive(false))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("already archived");
    }

    @Test
    public void overwrites_archive_when_overwrite_is_true() throws IOException {
        // Given
        Files.createDirectories(archiveRoot);
        Files.createDirectories(stagingDir);
        var archiveFile = archiveRoot.resolve("test.zip");
        Files.writeString(archiveFile, "OLD CONTENT");
        var layer = new LayerImpl(1, new StagingDir(stagingDir), new ZipArchive(archiveFile));
        // Write some files to staging dir
        Files.writeString(stagingDir.resolve("file1.txt"), "NEW CONTENT", StandardCharsets.UTF_8);

        layer.close();

        // When
        layer.archive(true);

        // Then
        assertThat(archiveFile).exists();

        // Read the one entry from the zip file
        try (var zipFile = new ZipFile(archiveFile.toFile())) {
            var entries = Collections.list(zipFile.entries());
            assertThat(entries).hasSize(1);
            assertThat(entries.get(0).getName()).isEqualTo("file1.txt");
            assertThat(IOUtils.toString(zipFile.getInputStream(entries.get(0)), StandardCharsets.UTF_8)).contains("NEW CONTENT");
        }
    }

    @Test
    public void throws_RuntimeException_if_archive_root_does_not_exist() throws Exception {
        // Given
        // NOTE: archiveDir is not created
        var testZip = archiveRoot.resolve("test.zip");
        Files.createDirectories(stagingDir);
        var layer = new LayerImpl(1, new StagingDir(stagingDir), new ZipArchive(testZip));
        layer.close();

        // When / Then
        assertThatThrownBy(() -> layer.archive(false))
            .isInstanceOf(RuntimeException.class)
            .hasRootCauseInstanceOf(NoSuchFileException.class)
            .hasRootCauseMessage(testZip.toString());
        // misleading message: the actual problem is that archiveDir does not exist
        var contentString = stdout.toString();
        assertThat(contentString).contains(MessageFormat.format("java.nio.file.NoSuchFileException: {0}", testZip));
    }

    @Test
    public void removes_staging_dir() throws IOException {
        // Given
        Files.createDirectories(archiveRoot);
        Files.createDirectories(stagingDir);
        var layer = new LayerImpl(1, new StagingDir(stagingDir), new ZipArchive(archiveRoot.resolve("test.zip")));
        createEmptyStagingDirFiles("path/to/file1");
        layer.close();

        // When
        layer.archive(false);

        // Then
        assertThat(stagingDir).doesNotExist();
    }
}
