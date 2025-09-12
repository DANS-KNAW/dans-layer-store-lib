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

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.IteratorUtils;

import java.io.IOException;

@Slf4j
@AllArgsConstructor
public class ItemsMatchDbConsistencyChecker implements LayerConsistencyChecker {
    private final LayerDatabase database;

    @Override
    public void check(Layer layer) throws IOException, ItemsMismatchException {
        checkSameItemsFoundOnStorageAsInDatabase(layer);
    }

    private void checkSameItemsFoundOnStorageAsInDatabase(Layer layer) throws IOException, ItemsMismatchException {
        log.debug("Checking consistency of items found on storage for layer {}", layer.getId());
        var itemsInDb = database.getRecordsByLayerId(layer.getId()).stream().map(ItemRecord::toItem).toList();
        var itemsOnStorage = IteratorUtils.toList(layer.listAllItems());
        var missingOnStorage = itemsInDb.stream().filter(item -> !itemsOnStorage.contains(item)).toList();
        var missingInDb = itemsOnStorage.stream().filter(item -> !itemsInDb.contains(item)).toList();
        if (!missingInDb.isEmpty() || !missingOnStorage.isEmpty()) {
            throw new ItemsMismatchException(missingInDb, missingOnStorage);
        }
        log.info("Consistency check of items found on storage for layer {} OK.", layer.getId());
    }
}
