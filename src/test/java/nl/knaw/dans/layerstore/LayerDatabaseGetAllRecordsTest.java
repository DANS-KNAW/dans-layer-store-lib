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
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class LayerDatabaseGetAllRecordsTest extends AbtractLayerDatabaseTest {

    @Test
    public void should_return_empty_list_when_database_is_empty() {
        assertThat(dao.getAllRecords().toList()).asList().isEmpty();
    }

    @Test
    public void should_return_one_record() {
        var record = addToDb(1L, "path", Type.Directory);
        assertThat(dao.getAllRecords().toList()).asList()
            .usingRecursiveFieldByFieldElementComparatorIgnoringFields("generatedId")
            .containsExactlyInAnyOrder(record);
    }

    @Test
    public void should_return_ten_records() {
        var record1 = addToDb(1L, "path1", Type.Directory);
        var record2 = addToDb(2L, "path2", Type.File);
        var record3 = addToDb(3L, "path3", Type.Directory);
        var record4 = addToDb(4L, "path4", Type.File);
        var record5 = addToDb(5L, "path5", Type.Directory);
        var record6 = addToDb(6L, "path6", Type.File);
        var record7 = addToDb(7L, "path7", Type.Directory);
        var record8 = addToDb(8L, "path8", Type.File);
        var record9 = addToDb(9L, "path9", Type.Directory);
        var record10 = addToDb(10L, "path10", Type.File);
        assertThat(dao.getAllRecords().toList()).asList()
            .usingRecursiveFieldByFieldElementComparatorIgnoringFields("generatedId")
            .containsExactlyInAnyOrder(record1, record2, record3, record4, record5, record6, record7, record8, record9, record10);
    }
}
