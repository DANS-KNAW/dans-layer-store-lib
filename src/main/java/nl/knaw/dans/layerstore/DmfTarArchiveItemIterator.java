package nl.knaw.dans.layerstore;

import java.util.Iterator;

public class DmfTarArchiveItemIterator implements Iterator<Item> {
    /**
     * The root item represents the root of the zip archive, and is implicitly present in every zip archive.
     */
    private final Item rootItem = new Item("", Item.Type.Directory);

    private boolean rootReturned = false;

    private final Iterator<String> entries;

    /**
     * Creates a new iterator over the items in the given archive.
     *
     * @param archiveName  the name of the archive to iterate over (without path)
     * @param dmfTarRunner the DmfTarRunner to use for accessing the archive
     */
    public DmfTarArchiveItemIterator(String archiveName, DmfTarRunner dmfTarRunner) {
        this.entries = dmfTarRunner.listFiles(archiveName);
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
        // Remove trailing slash from directory names
        var name = next;
        if (name.endsWith("/")) {
            name = name.substring(0, name.length() - 1);
        }
        return new Item(name, next.endsWith("/") ? Item.Type.Directory : Item.Type.File);
    }
}
