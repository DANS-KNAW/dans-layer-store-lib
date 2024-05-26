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
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class LayerDatabaseSaveRecordsTest extends AbstractLayerDatabaseTest {

    @Test
    public void should_accept_empty_list() {
        db.saveRecords();
        assertThat(db.getAllRecords().toList()).asList().isEmpty();
    }

    @Test
    public void should_accept_one_record() {
        var record = ItemRecord.builder()
            .layerId(1L)
            .path("path")
            .type(Type.Directory)
            .build();
        daoTestExtension.inTransaction(() -> db.saveRecords(record));
        assertThat(db.getAllRecords())
            .usingRecursiveFieldByFieldElementComparatorIgnoringFields("generatedId")
            .containsExactlyInAnyOrder(record);
    }

    @Test
    public void should_accept_two_records() {
        var record1 = ItemRecord.builder()
            .layerId(1L)
            .path("path1")
            .type(Type.Directory)
            .build();
        var record2 = ItemRecord.builder()
            .layerId(2L)
            .path("path2")
            .type(Type.Directory)
            .build();
        daoTestExtension.inTransaction(() -> db.saveRecords(record1, record2));
        assertThat(db.getAllRecords())
            .usingRecursiveFieldByFieldElementComparatorIgnoringFields("generatedId")
            .containsExactlyInAnyOrder(record1, record2);
    }
}
