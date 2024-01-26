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

import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;

/**
 * An implementation of FileStore that stores files and directories as a stack of layers. A layer can be staged or archived. Staged layers can be modified, archived layers are read-only. To transform
 * the layered file store into a regular file system directory, each layer must be unarchived (if it was archived) to a staging directory and the staging directories must be copied into a single
 * directory, starting with the oldest layer and ending with the newest layer. Files in newer layers overwrite files in older layers.
 *
 * The LayeredFileStore is backed by a LayerDatabase to support storage of layers in a way that may not be fast enough for direct access, for example on tape. See the LayerDatabase interface for more
 * information.
 *
 * @see LayerDatabase
 */
@AllArgsConstructor
public class LayeredItemStore implements ItemStore {
    private final LayerDatabase database;
    private final LayerManager layerManager;

    @Override
    public List<Item> listDirectory(String directoryPath) {
        return null;
    }

    @Override
    public List<Item> listRecursive(String directoryPath) {
        return null;
    }

    @Override
    public boolean existsPathLike(String path) {
        return false;
    }

    @Override
    public InputStream readFile(String path) {
        return null;
    }

    @Override
    public void writeFile(String path, InputStream content) {

    }

    @Override
    public void moveDirectoryInto(Path source, String destination) {

    }

    @Override
    public void moveDirectoryInternal(String source, String destination) {

    }

    @Override
    public void deleteDirectory(String path) {

    }

    @Override
    public void deleteFiles(List<String> paths) {

    }
}
