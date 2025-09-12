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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class StagingDirTest extends AbstractTestWithTestDir {

    @Test
    public void should_not_create_staging_dir() {
        var stagingDir = new StagingDir(testDir, 1234567890123L);
        assertThat(stagingDir.getPath()).doesNotExist();
        assertThat(stagingDir.getId()).isEqualTo(1234567890123L);
        assertThat(stagingDir.isStaged()).isFalse();
        assertThat(stagingDir.isClosed()).isTrue();
        assertThat(stagingDir.isOpen()).isFalse();
    }

    @Test
    public void should_parse_id_from_open_dir_name() throws IOException {
        var dir = testDir.resolve("1234567890123");
        Files.createDirectories(dir);

        var stagingDir = new StagingDir(dir);

        assertThat(stagingDir.getPath()).exists().isDirectory();
        assertThat(stagingDir.getId()).isEqualTo(1234567890123L);
        assertThat(stagingDir.isStaged()).isTrue();
        assertThat(stagingDir.isOpen()).isTrue();
        assertThat(stagingDir.isClosed()).isFalse();
    }

    @Test
    public void should_parse_id_from_closed_dir_name() throws IOException {
        var dir = testDir.resolve("1234567890123.closed");
        Files.createDirectories(dir);

        var stagingDir = new StagingDir(dir);

        assertThat(stagingDir.getPath()).exists().isDirectory();
        assertThat(stagingDir.getId()).isEqualTo(1234567890123L);
        assertThat(stagingDir.isStaged()).isTrue();
        assertThat(stagingDir.isClosed()).isTrue();
        assertThat(stagingDir.isOpen()).isFalse();
    }

    @Test
    public void constructor_with_root_and_id_uses_open_when_closed_missing() throws IOException {
        var open = testDir.resolve("1234567890123");
        Files.createDirectories(open);

        var stagingDir = new StagingDir(testDir, 1234567890123L);

        assertThat(stagingDir.getPath()).isEqualTo(open);
        assertThat(stagingDir.isStaged()).isTrue();
        assertThat(stagingDir.isOpen()).isTrue();
    }

    @Test
    public void close_moves_open_directory_to_closed() throws IOException {
        var open = testDir.resolve("1234567890123");
        Files.createDirectories(open);
        var stagingDir = new StagingDir(open);

        stagingDir.close();

        assertThat(open).doesNotExist();
        assertThat(testDir.resolve("1234567890123.closed")).exists().isDirectory();
        assertThat(stagingDir.isClosed()).isTrue();
        assertThat(stagingDir.isOpen()).isFalse();
    }

    @Test
    public void open_renames_closed_to_open_when_open_missing() throws IOException {
        var closed = testDir.resolve("1234567890123.closed");
        Files.createDirectories(closed);
        var stagingDir = new StagingDir(closed);

        stagingDir.open();

        assertThat(closed).doesNotExist();
        assertThat(testDir.resolve("1234567890123")).exists().isDirectory();
        assertThat(stagingDir.isOpen()).isTrue();
        assertThat(stagingDir.isClosed()).isFalse();
    }

    @Test
    public void checkOpen_throws_when_closed() throws IOException {
        var closed = testDir.resolve("1234567890123.closed");
        Files.createDirectories(closed);
        var stagingDir = new StagingDir(closed);

        assertThatThrownBy(stagingDir::checkOpen)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Layer is closed");
    }

    @Test
    public void checkClosed_throws_when_open() throws IOException {
        var open = testDir.resolve("1234567890123");
        Files.createDirectories(open);
        var stagingDir = new StagingDir(open);

        assertThatThrownBy(stagingDir::checkClosed)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Layer is open");
    }

    @Test
    public void constructor_rejects_invalid_names() {
        var notNumeric = testDir.resolve("abc");
        var wrongSuffix = testDir.resolve("1234567890123.notclosed");
        assertThatThrownBy(() -> new StagingDir(notNumeric))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid layer name");
        assertThatThrownBy(() -> new StagingDir(wrongSuffix))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid layer name");
    }

    @Test
    public void constructor_with_root_and_id_throws_when_both_open_and_closed_exist() throws IOException {
        var open = testDir.resolve("1234567890123");
        var closed = testDir.resolve("1234567890123.closed");
        Files.createDirectories(open);
        Files.createDirectories(closed);

        assertThatThrownBy(() -> new StagingDir(testDir, 1234567890123L))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("both open and closed")
            .hasMessageContaining("1234567890123");
    }

    @Test
    public void path_constructor_throws_when_open_and_closed_exist_and_closed_is_passed() throws IOException {
        var open = testDir.resolve("1234567890123");
        var closed = testDir.resolve("1234567890123.closed");
        Files.createDirectories(open);
        Files.createDirectories(closed);

        assertThatThrownBy(() -> new StagingDir(closed))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("both open and closed")
            .hasMessageContaining("1234567890123");
    }

    @Test
    public void path_constructor_throws_when_closed_exists_and_open_is_passed() throws IOException {
        var closed = testDir.resolve("1234567890123.closed");
        Files.createDirectories(closed);

        var open = testDir.resolve("1234567890123");
        // Do not create 'open' dir; validate that passing open path is rejected when sibling '.closed' exists
        assertThatThrownBy(() -> new StagingDir(open))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("is closed")
            .hasMessageContaining("1234567890123");
    }

    @Test
    public void constructor_rejects_regular_file_path() throws IOException {
        var file = testDir.resolve("1234567890123");
        Files.createDirectories(testDir);
        Files.writeString(file, "not a directory");

        assertThatThrownBy(() -> new StagingDir(file))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Not a directory");
    }

    @Test
    public void open_on_unstaged_closed_path_updates_internal_path_and_does_not_throw() throws IOException {
        // Given: neither open nor closed directory exists
        var closed = testDir.resolve("1234567890123.closed");
        var open = testDir.resolve("1234567890123");
        assertThat(closed).doesNotExist();
        assertThat(open).doesNotExist();
        var stagingDir = new StagingDir(closed);

        // When
        stagingDir.open();

        // Then: an internal path points to an open variant, still not staged, and no exception
        assertThat(stagingDir.getPath()).isEqualTo(open);
        assertThat(stagingDir.isStaged()).isFalse();
        // Since not staged, it's considered "closed" by definition
        assertThat(stagingDir.isClosed()).isTrue();
        assertThat(stagingDir.isOpen()).isFalse();
    }

    @Test
    public void open_on_unstaged_open_path_is_noop_and_does_not_throw() throws IOException {
        // Given: construct via root+id when nothing exists -> path points to open, not staged
        var open = testDir.resolve("1234567890123");
        assertThat(open).doesNotExist();
        var stagingDir = new StagingDir(testDir, 1234567890123L);
        assertThat(stagingDir.getPath()).isEqualTo(open);
        assertThat(stagingDir.isStaged()).isFalse();

        // When
        stagingDir.open(); // should not throw; should keep pointing to the same open path

        // Then
        assertThat(stagingDir.getPath()).isEqualTo(open);
        assertThat(stagingDir.isStaged()).isFalse();
        // Not staged => reported as closed
        assertThat(stagingDir.isClosed()).isTrue();
        assertThat(stagingDir.isOpen()).isFalse();
    }
}
