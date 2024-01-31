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

import io.dropwizard.testing.junit5.DAOTestExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

import static nl.knaw.dans.layerstore.Item.Type;

@ExtendWith(DropwizardExtensionsSupport.class)
public abstract class AbstractLayerDatabaseTest extends AbstractTestWithTestDir {
    protected final DAOTestExtension daoTestExtension = DAOTestExtension.newBuilder()
        .addEntityClass(ItemRecord.class)
        .build();
    protected LayerDatabase dao;

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        dao = new LayerDatabaseImpl(daoTestExtension.getSessionFactory());
    }

    protected ItemRecord addToDb(Long layerId, String path, Type type) {
        return daoTestExtension.inTransaction(() -> {
            var record = ItemRecord.builder()
                .layerId(layerId)
                .path(path)
                .type(type)
                .build();
            dao.saveRecords(record);
            return record;
        });
    }
}
