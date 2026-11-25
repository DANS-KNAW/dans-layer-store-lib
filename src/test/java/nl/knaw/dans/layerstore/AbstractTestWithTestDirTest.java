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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AbstractTestWithTestDirTest extends AbstractTestWithTestDir {
    @Test
    void creates_empty_directories_without_removing_existing_content() throws Exception {
        createStagingRoot();
        createStagingDir();
        createArchiveRoot();
        assertThat(stagingRoot).isDirectory();
        assertThat(stagingDir).isEmptyDirectory();
        assertThat(archiveRoot).isEmptyDirectory();

        writeStagingFile("existing", "staging");
        writeArchiveFile("existing", "archive");
        createStagingRoot();
        createStagingDir();
        createArchiveRoot();
        assertThat(Files.readString(stagingDir.resolve("existing"))).isEqualTo("staging");
        assertThat(Files.readString(archiveRoot.resolve("existing"))).isEqualTo("archive");
    }

    @Test
    void creates_nested_files_in_staging_and_archive() throws Exception {
        writeStagingFile("nested/content", "staging");
        writeArchiveFile("nested/content", "archive");
        createEmptyStagingFiles("nested/empty", "another");
        createEmptyArchiveFiles("nested/empty", "another");

        assertThat(Files.readString(stagingDir.resolve("nested/content"))).isEqualTo("staging");
        assertThat(Files.readString(archiveRoot.resolve("nested/content"))).isEqualTo("archive");
        assertThat(stagingDir.resolve("nested/empty")).isEmptyFile();
        assertThat(stagingDir.resolve("another")).isEmptyFile();
        assertThat(archiveRoot.resolve("nested/empty")).isEmptyFile();
        assertThat(archiveRoot.resolve("another")).isEmptyFile();
        assertThatThrownBy(() -> createEmptyArchiveFiles("another"))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Could not create archive file: another");
    }
}