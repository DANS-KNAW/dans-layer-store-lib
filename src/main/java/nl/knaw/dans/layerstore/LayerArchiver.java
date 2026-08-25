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

/**
 * Interface for archiving layers. This interface is used by the {@link LayerManager} to schedule and execute layer archiving.
 */
public interface LayerArchiver {

    /**
     * Archives the specified layer.
     *
     * @param layerId       the id of the layer to archive
     * @param archiveAction the callback action that executes the archiving of the layer
     */
    void archive(long layerId, Runnable archiveAction);
}
