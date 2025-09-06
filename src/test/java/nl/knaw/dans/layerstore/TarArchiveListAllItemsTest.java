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

import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;

public class TarArchiveListAllItemsTest extends AbstractTestWithTestDir {

    @Test
    public void iterates_only_root_in_empty_tar_archive() throws Exception {
        // Given
        var tarFile = testDir.resolve("empty.tar");
        Files.createDirectories(testDir.resolve("emptyDir"));
        try (var out = Files.newOutputStream(tarFile);
            var tos = new TarArchiveOutputStream(out)) {
            tos.finish();
        }

        var tarArchive = new TarArchive(tarFile);
        var iterator = tarArchive.listAllItems();

        // When
        var actualPaths = new HashSet<String>();
        while (iterator.hasNext()) {
            actualPaths.add(iterator.next().getPath());
        }

        // Then
        assertThat(actualPaths).containsExactly("");
    }

    @Test
    public void iterates_all_items_in_tar_archive() throws Exception {
        // Given
        var tarFile = testDir.resolve("test.tar");
        var tempDir = testDir.resolve("tarInput");
        Files.createDirectories(tempDir);
        FileUtils.writeStringToFile(tempDir.resolve("a/b/c/d/test1.txt").toFile(), "Hello world!", StandardCharsets.UTF_8);
        FileUtils.writeStringToFile(tempDir.resolve("a/b/c/test2.txt").toFile(), "Hello again!", StandardCharsets.UTF_8);
        FileUtils.writeStringToFile(tempDir.resolve("a/b/test3.txt").toFile(), "Hello once more!", StandardCharsets.UTF_8);
        FileUtils.writeStringToFile(tempDir.resolve("a/test4.txt").toFile(), "Hello for the last time!", StandardCharsets.UTF_8);
        var tarArchive = new TarArchive(tarFile);
        tarArchive.archiveFrom(tempDir);
        var iterator = tarArchive.listAllItems();

        // When
        var actualPaths = new HashSet<String>();
        while (iterator.hasNext()) {
            actualPaths.add(iterator.next().getPath());
        }

        // Then
        assertThat(actualPaths).containsExactlyInAnyOrder(
            "",
            "a",
            "a/b",
            "a/b/c",
            "a/b/c/d",
            "a/b/c/d/test1.txt",
            "a/b/c/test2.txt",
            "a/b/test3.txt",
            "a/test4.txt"
        );
    }

}
