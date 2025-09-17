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
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;

/**
 * An iterator that iterates over the {@link Item}s in a zip archive.
 */
public class ZipArchiveItemIterator implements Iterator<Item> {
    private final Iterator<ZipArchiveEntry> entries;
    /**
     * The root item represents the root of the zip archive, and is implicitly present in every zip archive.
     */
    private final Item rootItem = new Item("", Item.Type.Directory);

    private boolean rootReturned = false;

    public ZipArchiveItemIterator(@NonNull Path zipFile) throws IOException {
        var zip = ZipFile.builder()
            .setFile(zipFile.toFile())
            .get();
        entries = zip.getEntries().asIterator();
    }

    @Override
    public boolean hasNext() {
        if (!rootReturned) {
            return true;
        }
        return entries.hasNext();
    }

    @Override
    public Item next() {
        if (!rootReturned) {
            rootReturned = true;
            return rootItem;
        }
        var next = entries.next();
        return new Item(next.getName(),
            next.isDirectory() ? Item.Type.Directory : Item.Type.File);
    }
}
