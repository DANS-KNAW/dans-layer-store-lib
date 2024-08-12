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

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.FileUtils;
import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.jupiter.api.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

import static nl.knaw.dans.layerstore.TestUtils.assumeNotYetFixed;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class TarArchiveUnarchiveToTest extends AbstractTestWithTestDir {
    @Test
    public void should_unarchive_tarfile() throws Exception {
        var tarFile = testDir.resolve("test.tar");
        var tarArchive = new TarArchive(tarFile);
        // Create some files to archive
        var file1 = stagingDir.resolve("file1");
        var file2 = stagingDir.resolve("path/to/file2");
        var file3 = stagingDir.resolve("path/to/file3");

        // Write some string content to the files
        FileUtils.forceMkdir(file2.getParent().toFile());
        FileUtils.write(file1.toFile(), "file1 content", "UTF-8");
        FileUtils.write(file2.toFile(), "file2 content", "UTF-8");
        FileUtils.write(file3.toFile(), "file3 content", "UTF-8");
        Files.createDirectories(testDir.resolve("layer_staging"));

        // Archive the files
        tarArchive.archiveFrom(stagingDir);

        // Check that the tar file exists
        assertThat(tarFile).exists();
        AssertionsForClassTypes.assertThat(tarArchive.isArchived()).isTrue();

        // Unarchive the files
        var unarchived = testDir.resolve("unarchived");
        tarArchive.unarchiveTo(unarchived);

        // Check that the files are unarchived
        assertThat(unarchived.resolve("file1")).exists();
        assertThat(unarchived.resolve("path/to/file2")).exists();
        assertThat(unarchived.resolve("path/to/file3")).exists();
    }

    @Test
    public void should_unarchive_tarfile_with_empty_directory() throws Exception {
        var tarFile = testDir.resolve("test.tar");
        var tarArchive = new TarArchive(tarFile);
        // Create an empty directory to archive
        Path emptyDir = testDir.resolve("staging/emptyDir");
        FileUtils.forceMkdir(emptyDir.toFile());
        Files.createDirectories(testDir.resolve("layer_staging"));

        // Archive the empty directory
        tarArchive.archiveFrom(stagingDir);

        // Unarchive the files
        tarArchive.unarchiveTo(testDir.resolve("unarchived"));
        assertThat(emptyDir).exists();
    }

    @Test
    public void should_report_zip_slip() throws Exception {
        var tarFile = testDir.resolve("test.tar");
        var archive = new TarArchive(tarFile);
        // Create some files to archive
        var file1 = stagingDir.resolve("file1");

        // Write some string content to the files
        FileUtils.write(file1.toFile(), "file1 content", "UTF-8");
        Files.createDirectories(testDir.resolve("layer_staging"));

        // Archive the files
        archive.archiveFrom(stagingDir);

        // Check that the tar file exists
        assertThat(tarFile).exists();
        AssertionsForClassTypes.assertThat(archive.isArchived()).isTrue();

        // add malicious file to the archive
        try (var zip = new TarArchiveOutputStream(new FileOutputStream(tarFile.toFile()))) {
            var maliciousDir = testDir.resolve("violating/path");
            Files.createDirectories(maliciousDir);
            var entry = new TarArchiveEntry(maliciousDir, "../" + maliciousDir);
            zip.putArchiveEntry(entry);
            zip.closeArchiveEntry();
        }

        // Unarchive the files
        var unarchived = testDir.resolve("unarchived");
        assertThatThrownBy(() -> archive.unarchiveTo(unarchived))
            .hasCauseInstanceOf(IOException.class)
            .hasRootCauseMessage("Detected Zip Slip: ../target/test/TarArchiveUnarchiveToTest/violating/path/ in target/test/TarArchiveUnarchiveToTest/test.tar");
    }

    @Test
    public void should_throw_exception_when_unarchiving_non_existing_tarfile() {
        var tarFile = testDir.resolve("non-existing.tar");
        var tarArchive = new TarArchive(tarFile);
        assertThat(tarFile).doesNotExist();
        assertThat(tarArchive.isArchived()).isFalse();
        assertThatThrownBy(() -> tarArchive.unarchiveTo(testDir.resolve("unarchived")))
            .isInstanceOf(RuntimeException.class)
            .hasMessage("Could not unarchive target/test/TarArchiveUnarchiveToTest/non-existing.tar")
            .hasCauseInstanceOf(NoSuchFileException.class)
            .hasRootCauseMessage("target/test/TarArchiveUnarchiveToTest/non-existing.tar");

    }

    @Test
    public void should_throw_exception_when_unarchiving_to_non_empty_directory() throws Exception {
        var tarFile = testDir.resolve("test.tar");
        var tarArchive = new TarArchive(tarFile);
        // Create a files to archive
        Files.createDirectories(testDir.resolve("staging"));
        Files.createDirectories(testDir.resolve("layer_staging"));
        Files.writeString(testDir.resolve("staging/file1"), "file1 content");
        tarArchive.archiveFrom(stagingDir);

        Files.createDirectories(testDir.resolve("unarchived/content"));

        assumeNotYetFixed("unarchiveTo does not check if the target directory exists");
        assertThatThrownBy(() -> tarArchive.unarchiveTo(testDir.resolve("unarchived")));
    }
}
