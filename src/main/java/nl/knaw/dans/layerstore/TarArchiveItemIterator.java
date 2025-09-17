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

import lombok.NonNull;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarFile;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;

/**
 * An iterator that iterates over the {@link Item}s in a tar archive.
 */
public class TarArchiveItemIterator implements Iterator<Item> {
    private final TarFile tarFile;
    private final Iterator<TarArchiveEntry> entries;

    /**
     * The root item represents the root of the zip archive, and is implicitly present in every zip archive.
     */
    private final Item rootItem = new Item("", Item.Type.Directory);

    private boolean rootReturned = false;

    public TarArchiveItemIterator(@NonNull Path tarFile) throws IOException {
        this.tarFile = new TarFile(tarFile.toFile());
        this.entries = this.tarFile.getEntries().iterator();

    }

    @Override
    public boolean hasNext() {
        if (!rootReturned) {
            return true;
        }
        return entries.hasNext();
    }

    @Override
    @SuppressWarnings("ThrowFromFinallyBlock")
    public Item next() {
        try {
            if (!rootReturned) {
                rootReturned = true;
                return rootItem;
            }
            var next = entries.next();
            // Remove trailing slash from directory names
            var name = next.getName();
            if (next.isDirectory() && name.endsWith("/")) {
                name = name.substring(0, name.length() - 1);
            }
            return new Item(name, next.isDirectory() ? Item.Type.Directory : Item.Type.File);
        }
        finally {
            if (!hasNext()) {
                try {
                    tarFile.close();
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
