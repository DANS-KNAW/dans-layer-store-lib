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

public class LayerDatabaseExistsPathLikeTest extends AbstractLayerDatabaseTest {

    @Test
    public void should_return_false_when_no_records_exist() {
        var result = daoTestExtension.inTransaction(() -> dao.existsPathLike("file1.txt"));
        assertThat(result).isFalse();
    }

    @Test
    public void should_return_false_when_no_records_exist_for_path() {
        addToDb(1L, "file1.txt", Item.Type.File);
        addToDb(2L, "file2.txt", Item.Type.File);
        addToDb(3L, "file3.txt", Item.Type.File);
        var result = daoTestExtension.inTransaction(() -> dao.existsPathLike("file4.txt"));
        assertThat(result).isFalse();
    }

    @Test
    public void should_return_true_when_one_record_exists_for_path() {
        addToDb(1L, "file1.txt", Item.Type.File);
        addToDb(2L, "file2.txt", Item.Type.File);
        addToDb(3L, "file3.txt", Item.Type.File);
        var result = daoTestExtension.inTransaction(() -> dao.existsPathLike("file1.txt"));
        assertThat(result).isTrue();
    }

    @Test
    public void should_return_true_when_multiple_records_exist_for_path() {
        addToDb(1L, "file123.txt", Item.Type.File);
        addToDb(2L, "file456.txt", Item.Type.File);
        addToDb(3L, "file789.txt", Item.Type.File);
        addToDb(4L, "nomatch123.txt", Item.Type.File);
        var result = daoTestExtension.inTransaction(() -> dao.existsPathLike("file%.txt"));
        assertThat(result).isTrue();
    }

}
