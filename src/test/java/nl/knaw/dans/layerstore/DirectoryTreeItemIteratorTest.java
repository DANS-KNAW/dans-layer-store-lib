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

import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;

public class DirectoryTreeItemIteratorTest extends AbstractTestWithTestDir {

    @Test
    public void should_yield_only_starting_dir_if_it_is_empty() throws Exception {
        // Given
        var iterator = new DirectoryTreeItemIterator(testDir);

        // When - Then
        var actualPaths = new HashSet<String>();
        while (iterator.hasNext()) {
            actualPaths.add(iterator.next().getPath());
        }
        assertThat(actualPaths).containsExactly("");
    }

    @Test
    public void should_yield_files_and_directories_in_a_directory_tree() throws Exception {
        // Given
        createStagingFileWithContent("a/b/c/d/test1.txt", "Hello world!");
        createStagingFileWithContent("a/b/c/test2.txt", "Hello again!");
        createStagingFileWithContent("a/b/test3.txt", "Hello once more!");
        createStagingFileWithContent("a/test4.txt", "Hello for the last time!");
        var iterator = new DirectoryTreeItemIterator(stagingDir);

        // When - Then
        var actualPaths = new HashSet<String>();
        while (iterator.hasNext()) {
            actualPaths.add(iterator.next().getPath());
        }
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
