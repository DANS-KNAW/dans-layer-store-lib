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

import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;

import static nl.knaw.dans.layerstore.Item.Type;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class LayerDatabaseListDirectoryTest extends AbstractLayerDatabaseTest {

    @Test
    public void should_return_empty_list_when_no_records_in_db() throws Exception {
        assertThat(db.listDirectory("")).asList().isEmpty();
    }

    @Test
    public void should_return_empty_list_when_no_items_in_directory() throws Exception {
        addToDb(1L, "dir", Type.Directory);
        assertThat(db.listDirectory("dir")).asList().isEmpty();
    }

    @Test
    public void should_return_one_item_when_one_item_in_root_directory() throws Exception {
        var record = addToDb(1L, "item", Type.Directory);
        assertThat(db.listDirectory("")).asList()
            .usingRecursiveFieldByFieldElementComparatorIgnoringFields("generatedId")
            .containsExactlyInAnyOrder(record);
    }

    @Test
    public void should_return_one_item_when_one_item_in_directory() throws Exception {
        addToDb(1L, "dir", Type.Directory);
        var record = addToDb(1L, "dir/file", Type.File);
        assertThat(db.listDirectory("dir")).asList()
            .usingRecursiveFieldByFieldElementComparatorIgnoringFields("generatedId")
            .containsExactlyInAnyOrder(record);
    }

    @Test
    public void should_return_two_items_when_two_items_in_directory() throws Exception {
        addToDb(1L, "dir", Type.Directory);
        var record1 = addToDb(1L, "dir/file1", Type.File);
        var record2 = addToDb(1L, "dir/file2", Type.File);
        assertThat(db.listDirectory("dir")).asList()
            .usingRecursiveFieldByFieldElementComparatorIgnoringFields("generatedId")
            .containsExactlyInAnyOrder(record1, record2);
    }

    @Test
    public void should_return_two_items_when_two_items_in_root_directory() throws Exception {
        var record1 = addToDb(1L, "file1", Type.File);
        var record2 = addToDb(1L, "file2", Type.File);
        assertThat(db.listDirectory("")).asList()
            .usingRecursiveFieldByFieldElementComparatorIgnoringFields("generatedId")
            .containsExactlyInAnyOrder(record1, record2);
    }

    @Test
    public void should_return_one_item_when_item_has_records_in_multiple_layers() throws Exception {
        addToDb(1L, "dir", Type.Directory);
        addToDb(2L, "dir", Type.Directory);
        var record = addToDb(1L, "dir/file", Type.File);
        addToDb(2L, "dir/file", Type.File);
        assertThat(db.listDirectory("dir")).asList()
            .usingRecursiveFieldByFieldElementComparatorIgnoringFields("generatedId")
            .containsExactlyInAnyOrder(record);
    }

    @Test
    void should_return_one_root_folder_item_when_item_has_records_in_multiple_layers() throws Exception {
        addToDb(1L, "file1.txt", Type.File);
        addToDb(2L, "file1.txt", Type.File);
        assertThat(db.listDirectory("")).asList()
            .usingRecursiveFieldByFieldElementComparatorIgnoringFields("generatedId")
            .containsExactlyInAnyOrder(new Item("file1.txt", Type.File));
    }

    @Test
    public void should_return_one_item_when_item_has_grandchildren() throws Exception {
        addToDb(1L, "dir", Type.Directory);
        var record = addToDb(1L, "dir/subdir", Type.Directory);
        addToDb(1L, "dir/subdir/subfile", Type.File);
        assertThat(db.listDirectory("dir")).asList()
            .usingRecursiveFieldByFieldElementComparatorIgnoringFields("generatedId")
            .containsExactlyInAnyOrder(record);
    }

    @Test
    public void should_not_regard_file_starting_with_same_string_as_child_of_directory() throws Exception {
        addToDb(1L, "dir", Type.Directory);
        addToDb(1L, "dir/file_in_dir", Type.File);
        addToDb(1L, "dir_file_outside_dir", Type.File);
        assertThat(db.listDirectory("dir")).asList()
            .usingRecursiveFieldByFieldElementComparatorIgnoringFields("generatedId")
            .containsExactlyInAnyOrder(new Item("dir/file_in_dir", Type.File));
    }

    @Test
    public void should_throw_NotDirectoryException_when_path_is_not_a_directory() throws Exception {
        addToDb(1L, "dir", Type.Directory);
        addToDb(1L, "dir/file", Type.File);
        assertThatThrownBy(() -> db.listDirectory("dir/file"))
            .isInstanceOf(NotDirectoryException.class)
            .hasMessage("Not a directory: dir/file");
    }

    @Test
    public void should_throw_NoSuchFileException_when_path_does_not_exist() throws Exception {
        addToDb(1L, "dir", Type.Directory);
        addToDb(1L, "dir/file", Type.File);
        assertThatThrownBy(() -> db.listDirectory("dir/does-not-exist"))
            .isInstanceOf(NoSuchFileException.class)
            .hasMessage("No such directory: dir/does-not-exist");
    }

}
