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

import lombok.Builder;
import lombok.Data;

/**
 * A file or directory in the layer store. It may be represented by multiple {@link ItemRecord}s in the database, one for each layer that contains the item. Note that to obtain the current (i.e.,
 * latest) content of a File item, you need to retrieve the ItemRecord with the highest layerId.
 */
@Data
@Builder
public class Item {
    /**
     * The type of the item.
     */
    public enum Type {
        /**
         * A file.
         */
        File,
        /**
         * A directory.
         */
        Directory
    }

    /**
     * The path of the item. The path is relative to the root of the {@link ItemStore}. Note that directory paths should <b>not</b> end with a slash.
     */
    private final String path;

    /**
     * The type of the item.
     */
    private final Type type;
}
