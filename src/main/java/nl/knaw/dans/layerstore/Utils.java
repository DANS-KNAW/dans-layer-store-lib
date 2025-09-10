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

import java.util.ArrayList;
import java.util.List;

public class Utils {

    public static <T> void throwOnListDifference(List<T> databaseList, List<T> storageList, String baseMessage) {
        var missingInDb = new ArrayList<T>();
        for (var id : storageList) {
            if (!databaseList.contains(id)) {
                missingInDb.add(id);
            }
        }
        var missingOnStorage = new ArrayList<T>();
        for (var id : databaseList) {
            if (!storageList.contains(id)) {
                missingOnStorage.add(id);
            }
        }
        if (!missingInDb.isEmpty() || !missingOnStorage.isEmpty()) {
            var message = baseMessage;
            if (!missingInDb.isEmpty()) {
                message += " Missing in database: " + missingInDb;
            }
            if (!missingOnStorage.isEmpty()) {
                message += " Missing on storage: " + missingOnStorage;
            }
            throw new IllegalStateException(message);
        }
    }
}
