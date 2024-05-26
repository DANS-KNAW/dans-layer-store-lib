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

public class LayerDatabaseGetRecordsByPathTest extends AbstractLayerDatabaseTest {

    @Test
    public void should_return_empty_list_when_no_records_exist() {
        var result = daoTestExtension.inTransaction(() -> db.getRecordsByPath("file1.txt"));
        assertThat(result).asList().isEmpty();
    }

    @Test
    public void should_return_empty_list_when_no_records_exist_for_path() {
        addToDb(1L, "file1.txt", Item.Type.File);
        addToDb(2L, "file2.txt", Item.Type.File);
        addToDb(3L, "file3.txt", Item.Type.File);
        var result = daoTestExtension.inTransaction(() -> db.getRecordsByPath("file4.txt"));
        assertThat(result).asList().isEmpty();
    }

    @Test
    public void should_return_one_record_when_one_record_exists_for_path() {
        var record = addToDb(1L, "file1.txt", Item.Type.File);
        addToDb(2L, "file2.txt", Item.Type.File);
        addToDb(3L, "file3.txt", Item.Type.File);
        var result = daoTestExtension.inTransaction(() -> db.getRecordsByPath("file1.txt"));
        assertThat(result).asList()
            .usingRecursiveFieldByFieldElementComparatorIgnoringFields("generatedId")
            .containsExactly(record);
    }

    @Test
    public void should_return_multiple_records_when_multiple_records_exist_for_path() {
        var record1 = addToDb(1L, "file1.txt", Item.Type.File);
        var record2 = addToDb(2L, "file1.txt", Item.Type.File);
        var record3 = addToDb(3L, "file1.txt", Item.Type.File);
        var result = daoTestExtension.inTransaction(() -> db.getRecordsByPath("file1.txt"));
        assertThat(result).asList()
            .usingRecursiveFieldByFieldElementComparatorIgnoringFields("generatedId")
            .containsExactlyInAnyOrder(record1, record2, record3);
    }

    @Test
    public void should_not_return_records_with_other_paths() {
        addToDb(1L, "dir1", Item.Type.Directory);
        addToDb(1L, "dir1/file1.txt", Item.Type.File);
        var record = addToDb(2L, "dir1/file2.txt", Item.Type.File);
        addToDb(2L, "dir2", Item.Type.Directory);
        addToDb(3L, "dir2/file3.txt", Item.Type.File);
        var result = daoTestExtension.inTransaction(() -> db.getRecordsByPath("dir1/file2.txt"));
        assertThat(result).asList()
            .containsExactly(record);
    }

}
