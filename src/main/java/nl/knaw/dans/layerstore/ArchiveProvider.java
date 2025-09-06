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

import java.io.IOException;
import java.util.List;

/**
 * Provides a way to create an Archive. The implementation should provide a way to configure where the archive is stored.
 */
public interface ArchiveProvider {

    /**
     * Create a new {@link Archive} instance for the given layer ID.
     *
     * @param layerId the layer ID
     * @return the new archive
     */
    Archive createArchive(long layerId);


    /**
     * Check if an archive exists at the given path.
     *
     * @param layerId the layer ID
     * @return true if the archive exists, false otherwise
     */
    boolean exists(long layerId);


    /**
     * List all archived layer IDs.
     *
     * @return a list of layer IDs for which archives exist
     */
    List<Long> listArchivedLayers() throws IOException;
}
