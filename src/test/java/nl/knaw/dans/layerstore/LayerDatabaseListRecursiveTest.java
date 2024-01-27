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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class LayerDatabaseListRecursiveTest extends AbtractLayerDatabaseTest {
    @Test
    public void listRecursive_should_return_empty_list_if_nothing_found() throws Exception {
        assertThat(dao.listRecursive("")).asList().isEmpty();
    }

    @Test
    public void listRecursive_should_return_list_of_items_in_root_folder_and_subfolders_if_parameter_is_empty_string() throws Exception {
        addToDb(1L, "subdir", Item.Type.Directory);
        addToDb(1L, "file1", Item.Type.File);
        addToDb(1L, "file2", Item.Type.File);
        addToDb(2L, "subdir/file3", Item.Type.Directory);

        assertThat(dao.listRecursive("")).asList()
            .usingRecursiveFieldByFieldElementComparatorIgnoringFields("generatedId")
            .containsExactlyInAnyOrder(
                ItemRecord.builder()
                    .layerId(1L)
                    .path("subdir")
                    .type(Item.Type.Directory)
                    .build(),
                ItemRecord.builder()
                    .layerId(1L)
                    .path("file1")
                    .type(Item.Type.File)
                    .build(),
                ItemRecord.builder()
                    .layerId(1L)
                    .path("file2")
                    .type(Item.Type.File)
                    .build(),
                ItemRecord.builder()
                    .layerId(2L)
                    .path("subdir/file3")
                    .type(Item.Type.Directory)
                    .build()
            );
    }

    @Test
    public void listRecursive_should_return_list_of_items_in_subdir_folder_if_parameter_path_to_that_folder() throws Exception {
        addToDb(1L, "subdir", Item.Type.Directory);
        addToDb(1L, "subdir/file1", Item.Type.File);
        addToDb(1L, "subdir/file2", Item.Type.File);
        addToDb(2L, "subdir/subsubdir", Item.Type.Directory);
        addToDb(2L, "subdir/subsubdir/file3", Item.Type.File);

        assertThat(dao.listRecursive("subdir")).asList()
            .usingRecursiveFieldByFieldElementComparatorIgnoringFields("generatedId")
            .containsExactlyInAnyOrder(
                ItemRecord.builder()
                    .layerId(1L)
                    .path("subdir/file1")
                    .type(Item.Type.File)
                    .build(),
                ItemRecord.builder()
                    .layerId(1L)
                    .path("subdir/file2")
                    .type(Item.Type.File)
                    .build(),
                ItemRecord.builder()
                    .layerId(2L)
                    .path("subdir/subsubdir")
                    .type(Item.Type.Directory)
                    .build(),
                ItemRecord.builder()
                    .layerId(2L)
                    .path("subdir/subsubdir/file3")
                    .type(Item.Type.File)
                    .build()
            );
    }

    @Test
    public void listRecursive_should_return_list_of_items_in_subdir_folder_if_parameter_path_to_that_folder_and_subfolders() throws Exception {
        addToDb(1L, "subdir", Item.Type.Directory);
        addToDb(1L, "subdir/file1", Item.Type.File);
        addToDb(1L, "subdir/file2", Item.Type.File);
        addToDb(2L, "subdir/subsubdir", Item.Type.Directory);
        addToDb(2L, "subdir/subsubdir/file3", Item.Type.File);

        assertThat(dao.listRecursive("subdir/subsubdir")).asList()
            .usingRecursiveFieldByFieldElementComparatorIgnoringFields("generatedId")
            .containsExactlyInAnyOrder(
                ItemRecord.builder()
                    .layerId(2L)
                    .path("subdir/subsubdir/file3")
                    .type(Item.Type.File)
                    .build()
            );
    }

    // Return the item from the latest layer if it exists in multiple layers
    @Test
    public void listRecursive_should_return_latest_version_of_file_if_it_exists_in_multiple_layers() throws Exception {
        addToDb(1L, "dir1", Item.Type.Directory);
        addToDb(1L, "dir1/file1", Item.Type.File);
        addToDb(2L, "dir1", Item.Type.Directory);
        addToDb(2L, "dir1/file2", Item.Type.File);
        addToDb(3L, "dir1", Item.Type.Directory);
        addToDb(3L, "dir1/file1", Item.Type.File); // Overwrites file1 from layer 1

        assertThat(dao.listRecursive("dir1")).asList()
            .usingRecursiveFieldByFieldElementComparatorIgnoringFields("generatedId")
            .containsExactlyInAnyOrder(
                ItemRecord.builder()
                    .layerId(3L)
                    .path("dir1/file1")
                    .type(Item.Type.File)
                    .build(),
                ItemRecord.builder()
                    .layerId(2L)
                    .path("dir1/file2")
                    .type(Item.Type.File)
                    .build()
            );
    }

}
