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

public class LayerDatabaseDirectoryIsEmptyTest extends AbstractLayerDatabaseTest {

    @Test
    public void should_return_true_when_no_records_in_db() throws Exception {
        assertThat(db.directoryIsEmpty("")).isTrue();
    }

    @Test
    public void should_return_true_when_no_items_in_directory() throws Exception {
        addToDb(1L, "dir", Type.Directory);
        assertThat(db.directoryIsEmpty("dir")).isTrue();
    }

    @Test
    public void should_return_false_when_one_item_in_root_directory() throws Exception {
        addToDb(1L, "item", Type.Directory);
        assertThat(db.directoryIsEmpty("")).isFalse();
    }

    @Test
    public void should_return_false_when_one_item_in_directory() throws Exception {
        addToDb(1L, "dir", Type.Directory);
        addToDb(1L, "dir/file", Type.File);
        assertThat(db.directoryIsEmpty("dir")).isFalse();
    }

    @Test
    public void should_return_false_when_item_has_grandchildren() throws Exception {
        addToDb(1L, "dir", Type.Directory);
        addToDb(1L, "dir/subdir", Type.Directory);
        addToDb(1L, "dir/subdir/subfile", Type.File);
        assertThat(db.directoryIsEmpty("dir")).isFalse();
    }

    @Test
    public void should_not_regard_file_starting_with_same_string_as_child_of_directory() throws Exception {
        addToDb(1L, "dir", Type.Directory);
        addToDb(1L, "dir_file_outside_dir", Type.File);
        assertThat(db.directoryIsEmpty("dir")).isTrue();
    }

    @Test
    public void should_throw_NotDirectoryException_when_path_is_not_a_directory() {
        addToDb(1L, "dir", Type.Directory);
        addToDb(1L, "dir/file", Type.File);
        assertThatThrownBy(() -> db.directoryIsEmpty("dir/file"))
            .isInstanceOf(NotDirectoryException.class)
            .hasMessage("Not a directory: dir/file");
    }

    @Test
    public void should_throw_NoSuchFileException_when_path_does_not_exist() {
        addToDb(1L, "dir", Type.Directory);
        addToDb(1L, "dir/file", Type.File);
        assertThatThrownBy(() -> db.directoryIsEmpty("dir/does-not-exist"))
            .isInstanceOf(NoSuchFileException.class)
            .hasMessage("No such directory: dir/does-not-exist");
    }
}
