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

import lombok.Getter;

import java.util.List;

/**
 * Exception thrown when the layer ids in the database and the layer ids in the storage do not match.
 */
@Getter
public class LayerIdsMismatchException extends Exception {
    private final List<Long> missingInDb;
    private final List<Long> missingInStorage;

    public LayerIdsMismatchException(List<Long> missingInDb, List<Long> missingInStorage) {
        super("Layer ids mismatch. Missing in database: " + missingInDb + ", missing in storage: " + missingInStorage + ".");
        this.missingInDb = missingInDb;
        this.missingInStorage = missingInStorage;
    }
}
