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

import static nl.knaw.dans.layerstore.Item.Type;
import static nl.knaw.dans.layerstore.Item.Type.Directory;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class LayerDatabaseDeleteByIdsTest extends AbtractLayerDatabaseTest {

    @Test
    public void should_accept_empty_list() {
        addToDb(1L, "path", Directory);
        daoTestExtension.inTransaction(() -> dao.deleteRecordsById());
        // Check that the record is still there
        assertThat(dao.getAllRecords().toList()).asList().hasSize(1);
    }

    @Test
    public void should_delete_one_record() {
        var record = addToDb(1L, "path", Directory);
        daoTestExtension.inTransaction(() -> dao.deleteRecordsById(record.getGeneratedId()));
        assertThat(dao.getAllRecords().toList()).asList().isEmpty();
    }

    @Test
    public void should_delete_two_records() {
        var record1 = addToDb(1L, "path1", Directory);
        var record2 = addToDb(2L, "path2", Type.File);
        var notDeletedRecord = addToDb(3L, "path3", Directory);
        daoTestExtension.inTransaction(() -> dao.deleteRecordsById(record1.getGeneratedId(), record2.getGeneratedId()));
        assertThat(dao.getAllRecords().toList()).asList()
            .usingRecursiveFieldByFieldElementComparatorIgnoringFields("generatedId")
            .containsExactlyInAnyOrder(notDeletedRecord);
    }

}
