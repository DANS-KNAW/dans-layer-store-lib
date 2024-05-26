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

import javax.persistence.OptimisticLockException;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LayerDatabaseAddDirectoryTest extends AbstractLayerDatabaseTest {

    @Test
    public void should_add_item_records_for_directories() {
        daoTestExtension.inTransaction(() -> db.addDirectory(1L, "root/child/grandchild"));
        assertThat(db.getAllRecords().toList())
            .usingRecursiveFieldByFieldElementComparatorIgnoringFields("generatedId")
            .containsExactlyInAnyOrder(
                ItemRecord.builder()
                    .layerId(1L)
                    .path("")
                    .type(Item.Type.Directory)
                    .build(),
                ItemRecord.builder()
                    .layerId(1L)
                    .path("root")
                    .type(Item.Type.Directory)
                    .build(),
                ItemRecord.builder()
                    .layerId(1L)
                    .path("root/child")
                    .type(Item.Type.Directory)
                    .build(),
                ItemRecord.builder()
                    .layerId(1L)
                    .path("root/child/grandchild")
                    .type(Item.Type.Directory)
                    .build()
            );
    }

    @Test
    public void should_not_add_item_records_if_they_already_exist_in_the_same_layer() {
        var newRecords = daoTestExtension.inTransaction(() -> db.addDirectory(1L, "root/child/grandchild"));
        assertThat(newRecords)
            .usingRecursiveFieldByFieldElementComparatorIgnoringFields("generatedId")
            .contains(
                ItemRecord.builder()
                    .layerId(1L)
                    .path("")
                    .type(Item.Type.Directory)
                    .build(),
                ItemRecord.builder()
                    .layerId(1L)
                    .path("root")
                    .type(Item.Type.Directory)
                    .build(),
                ItemRecord.builder()
                    .layerId(1L)
                    .path("root/child")
                    .type(Item.Type.Directory)
                    .build(),
                ItemRecord.builder()
                    .layerId(1L)
                    .path("root/child/grandchild")
                    .type(Item.Type.Directory)
                    .build()
            );
        newRecords = daoTestExtension.inTransaction(() -> db.addDirectory(1L, "root/child/grandchild"));
        // No new directories should have been added
        assertThat(newRecords)
            .isEmpty();

        // Check that the directories were added, ignoring the generatedId
        assertThat(db.getAllRecords().toList())
            .usingRecursiveFieldByFieldElementComparatorIgnoringFields("generatedId")
            .containsExactlyInAnyOrder(
                ItemRecord.builder()
                    .layerId(1L)
                    .path("")
                    .type(Item.Type.Directory)
                    .build(),
                ItemRecord.builder()
                    .layerId(1L)
                    .path("root")
                    .type(Item.Type.Directory)
                    .build(),
                ItemRecord.builder()
                    .layerId(1L)
                    .path("root/child")
                    .type(Item.Type.Directory)
                    .build(),
                ItemRecord.builder()
                    .layerId(1L)
                    .path("root/child/grandchild")
                    .type(Item.Type.Directory)
                    .build()
            );
    }

    @Test
    public void should_add_item_records_even_if_directories_already_exist_in_another_layer() {
        daoTestExtension.inTransaction(() -> db.addDirectory(1L, "root/child/grandchild"));
        daoTestExtension.inTransaction(() -> db.addDirectory(2L, "root/child/grandchild"));
        // Check that the directories were added, ignoring the generatedId
        assertThat(db.getAllRecords().toList())
            .usingRecursiveFieldByFieldElementComparatorIgnoringFields("generatedId")
            .containsExactlyInAnyOrder(
                // Note, that each layer contains the root directory
                ItemRecord.builder()
                    .layerId(1L)
                    .path("")
                    .type(Item.Type.Directory)
                    .build(),
                ItemRecord.builder()
                    .layerId(2L)
                    .path("")
                    .type(Item.Type.Directory)
                    .build(),
                ItemRecord.builder()
                    .layerId(1L)
                    .path("root")
                    .type(Item.Type.Directory)
                    .build(),
                ItemRecord.builder()
                    .layerId(1L)
                    .path("root/child")
                    .type(Item.Type.Directory)
                    .build(),
                ItemRecord.builder()
                    .layerId(1L)
                    .path("root/child/grandchild")
                    .type(Item.Type.Directory)
                    .build(),
                ItemRecord.builder()
                    .layerId(2L)
                    .path("root")
                    .type(Item.Type.Directory)
                    .build(),
                ItemRecord.builder()
                    .layerId(2L)
                    .path("root/child")
                    .type(Item.Type.Directory)
                    .build(),
                ItemRecord.builder()
                    .layerId(2L)
                    .path("root/child/grandchild")
                    .type(Item.Type.Directory)
                    .build()
            );
    }

    @Test
    public void should_throw_not_found_generatedId() {
        var e = assertThrows(OptimisticLockException.class, () ->
                daoTestExtension.inTransaction(() -> {
                    var record = ItemRecord.builder()
                            .layerId(1L)
                            .path("app")
                            .type(Item.Type.Directory)
                            .generatedId(21L) // not in DB
                            .build();
                    db.saveRecords(record);
                })
        );
        assertThat(e.getMessage()).contains("actual row count: 0; expected: 1");
    }

    @Test
    public void should_update_path() {
        addToDb(1L, "james", Item.Type.Directory);
        var r = db.getRecordsByPath("james").get(0);
        daoTestExtension.inTransaction(() -> {
            r.setPath("bond");
            db.saveRecords(r);
         });
        assertThat(db.getAllRecords().toList())
                .containsExactlyInAnyOrder(
                        ItemRecord.builder()
                                .layerId(1L)
                                .path("bond")
                                .type(Item.Type.Directory)
                                .generatedId(r.getGeneratedId())
                                .build()
                );

    }

    @Test
    public void should_throw_an_IllegalArgumentException_if_the_path_contains_a_file_in_previous_layer() {
        addToDb(1L, "", Item.Type.Directory);
        addToDb(1L, "root", Item.Type.Directory);
        addToDb(1L, "root/child", Item.Type.Directory);
        addToDb(1L, "root/child/grandchild", Item.Type.File);
        var e = assertThrows(IllegalArgumentException.class, () ->
            daoTestExtension.inTransaction(() -> db.addDirectory(2L, "root/child/grandchild"))
        );
        assertThat(e.getMessage()).isEqualTo("Cannot add directory root/child/grandchild because it is already occupied by a file.");
    }

    @Test
    public void should_throw_an_IllegalArgumentException_if_the_path_contains_a_file_in_the_same_layer() {
        addToDb(1L, "root/child/grandchild", Item.Type.File);
        var e = assertThrows(IllegalArgumentException.class, () ->
            daoTestExtension.inTransaction(() -> db.addDirectory(1L, "root/child/grandchild"))
        );
        assertThat(e.getMessage()).isEqualTo("Cannot add directory root/child/grandchild because it is already occupied by a file.");
    }

}
