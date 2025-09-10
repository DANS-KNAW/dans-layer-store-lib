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

import nl.knaw.dans.layerstore.Item.Type;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;

/**
 * An iterator that traverses a directory tree and yields {@link Item}s representing files and directories. The paths of the items are relative to the provided root directory. The iterator includes
 * the root directory itself as an item with an empty path.
 */
public class DirectoryTreeItemIterator implements Iterator<Item> {
    private final Path directoryPath;
    private final Iterator<File> pathIterator;

    public DirectoryTreeItemIterator(Path directoryPath) throws IOException {
        this.directoryPath = directoryPath;
        this.pathIterator = FileUtils.iterateFilesAndDirs(directoryPath.toFile(), TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);
    }

    public boolean hasNext() {
        return pathIterator.hasNext();
    }

    @Override
    public Item next() {
        var next = pathIterator.next();
        return new Item(directoryPath.relativize(next.toPath()).toString(),
            next.isDirectory() ? Type.Directory : Type.File);
    }
}