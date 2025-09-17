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

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An iterator that traverses the entries of a DMF TAR archive and yields {@link Item}s representing files and directories.
 */
public class DmfTarArchiveItemIterator implements Iterator<Item> {
    private final Iterator<String> entries;

    private Item nextItem;

    /**
     * Creates a new iterator over the items in the given archive.
     *
     * @param archiveName  the name of the archive to iterate over (only the filename, without the path)
     * @param dmfTarRunner the DmfTarRunner to use for accessing the archive
     */
    public DmfTarArchiveItemIterator(String archiveName, DmfTarRunner dmfTarRunner) {
        this.entries = dmfTarRunner.listFiles(archiveName);
    }

    @Override
    public boolean hasNext() {
        while (nextItem == null && entries.hasNext()) {
            var path = getPathFromEntry(entries.next());
            if (!isDmftarCacheEntry(path)) {
                nextItem = new Item(removeTrailingSlash(path), path.endsWith("/") || path.isEmpty() ? Item.Type.Directory : Item.Type.File);
            }
        }
        return nextItem != null;
    }

    @Override
    public Item next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        var item = nextItem;
        nextItem = null;
        return item;
    }

    private boolean isDmftarCacheEntry(String entry) {
        return entry.startsWith("dmftar-cache.");
    }

    private String removeTrailingSlash(String path) {
        if (path.endsWith("/")) {
            return path.substring(0, path.length() - 1);
        }
        return path;
    }

    /**
     * Entries are formatted like this example:
     * <pre>
     * -rw-rw-r-- janm/janm      4777 2025-07-14 11:25 ./text/loro.txt
     * </pre>
     * We want to extract the path after the "./".
     */
    private String getPathFromEntry(String entry) {
        int index = entry.indexOf("./");
        if (index == -1) {
            throw new IllegalStateException("Got malformed entry from tar (no './' found): " + entry);
        }

        return entry.substring(index + 2);
    }
}
