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
 * Manages {@link Layer}s. Implementations of this interface should create new layers only through the `newTopLayer` method.
 */
public interface LayerManager {
    /**
     * Closes the current top layer (if present) and creates a new top layer. The old top layer will be scheduled for archiving.
     */
    void newTopLayer() throws IOException;

    /**
     * Returns the current top layer.
     *
     * @return the current top layer
     */
    Layer getTopLayer();

    /**
     * Returns the layer with the given id.
     *
     * @param id the id of the layer
     * @return the layer
     */
    Layer getLayer(long id);

    /**
     * Lists all layer IDs that are currently managed.
     *
     * @return a list of layer IDs
     * @throws IOException if an I/O error occurs
     */
    List<Long> listLayerIds() throws IOException;
}
