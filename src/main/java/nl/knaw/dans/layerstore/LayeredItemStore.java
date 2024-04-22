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

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

/**
 * An implementation of FileStore that stores files and directories as a stack of layers. A layer can be staged or archived. Staged layers can be modified, archived layers are read-only. To transform
 * the layered file store into a regular file system directory, each layer must be unarchived (if it was archived) to a staging directory and the staging directories must be copied into a single
 * directory, starting with the oldest layer and ending with the newest layer. Files in newer layers overwrite files in older layers.
 * <p>
 * The LayeredFileStore is backed by a LayerDatabase to support storage of layers in a way that may not be fast enough for direct access, for example on tape. See the LayerDatabase interface for more
 * information.
 *
 * @see LayerDatabase
 */
@Slf4j
public class LayeredItemStore implements ItemStore {
    private final LayerDatabase database;
    private final LayerManager layerManager;
    private final DatabaseBackedContentManager databaseBackedContentManager;

    public LayeredItemStore(
        LayerDatabase database,
        LayerManager layerManager,
        DatabaseBackedContentManager databaseBackedContentManager) {
        this.database = database;
        this.layerManager = layerManager;
        this.databaseBackedContentManager = Optional.ofNullable(databaseBackedContentManager).orElse(new NoopDatabaseBackedContentManager());
    }

    public LayeredItemStore(LayerDatabase database, LayerManager layerManager) {
        this(database, layerManager, null);
    }

    @Override
    public List<Item> listDirectory(String directoryPath) throws IOException {
        return database.listDirectory(directoryPath);
    }

    @Override
    public List<Item> listRecursive(String directoryPath) throws IOException {
        return database.listRecursive(directoryPath);
    }

    @Override
    public boolean existsPathLike(String path) {
        return database.existsPathLike(path);
    }

    @Override
    public InputStream readFile(String path) throws IOException {
        // Check that the file exists and is a file
        if (!database.existsPathLike(path)) {
            // Emulate the behavior of Files.readAllBytes(Path) by throwing a NoSuchFileException
            throw new NoSuchFileException(path);
        }
        var latestRecord = database.getRecordsByPath(path).get(0);
        if (latestRecord.getType() == Item.Type.Directory) {
            // Emulate the behavior of Files.readAllBytes(Path) by throwing a generic IOException
            throw new IOException("Path is a directory: " + path);
        }
        if (latestRecord.getContent() == null) {
            log.debug("Reading file {} from layer {}", path, latestRecord.getLayerId());
            return layerManager.getLayer(latestRecord.getLayerId()).readFile(path);
        }
        else {
            log.debug("Reading file {} from database", path);
            return new ByteArrayInputStream(databaseBackedContentManager.postRetrieve(path, latestRecord.getContent()));
        }
    }

    @Override
    public void writeFile(String path, InputStream content) throws IOException {
        log.debug("Writing file {} to top layer", path);
        layerManager.getTopLayer().writeFile(path, content);
        // Get existing record for path in the top layer
        var existingRecords = database.getRecordsByPath(path).stream().filter(r -> r.getLayerId() == layerManager.getTopLayer().getId()).toList();
        ItemRecord record;
        if (existingRecords.size() > 1) {
            throw new IllegalStateException("Found multiple records for path " + path + " in layer " + layerManager.getTopLayer().getId());
        }
        else if (existingRecords.size() == 1) {
            log.debug("Updating existing record for path {} in layer {}", path, layerManager.getTopLayer().getId());
            record = existingRecords.get(0);
        }
        else {
            log.debug("Creating new record for path {} in layer {}", path, layerManager.getTopLayer().getId());
            record = ItemRecord.builder()
                .path(path)
                .type(Item.Type.File)
                .layerId(layerManager.getTopLayer().getId())
                .build();
        }
        if (databaseBackedContentManager.test(path)) {
            // N.B. We read the content from the top layer, not from the InputStream, because it has already read when writing to the top layer.
            log.debug("Storing a copy of the content in the database for path {}", path);
            try (var is = layerManager.getTopLayer().readFile(path)) {
                byte[] bytes = databaseBackedContentManager.preStore(path, IOUtils.toByteArray(is));
                log.debug("Content size: {}", bytes.length);
                record.setContent(bytes);
            }
        }
        database.saveRecords(record); // TODO: roll back writeFile if saveRecords fails? How to do that?
    }

    @Override
    public void moveDirectoryInto(Path source, String destination) throws IOException {
        var parent = Path.of(destination).getParent();
        if (database.existsPathLike(destination)) {
            throw new IllegalArgumentException("Destination already exists: " + destination);
        }
        if (!database.existsPathLike(parent.toString())) {
            throw new IllegalArgumentException("Parent of destination does not exist: " + parent);
        }

        /*
         * Although we have established that the parent of the destination exists in the Store, it is not
         * guaranteed that the parent path exists in the top layer. If it does not, we need to create it.
         * We first create any missing ItemRecords in the database, and then create the directory in the
         * top layer.
         */
        var newItemRecordsUpToDestination = database.addDirectory(
            layerManager.getTopLayer().getId(),
            parent.toString());
        if (!newItemRecordsUpToDestination.isEmpty()) {
            layerManager.getTopLayer().createDirectory(parent.toString());
        }

        // Create listing records for all files in the moved directory
        var records = new ArrayList<ItemRecord>();
        try (var s = Files.walk(source)) {
            for (Path path : s.toList()) {
                var destPath = destination + "/" + source.relativize(path);
                var r = ItemRecord.builder()
                    .layerId(layerManager.getTopLayer().getId())
                    .path(destPath)
                    .type(getItemType(path)).build();
                if (databaseBackedContentManager.test(destPath)) {
                    byte[] content = FileUtils.readFileToByteArray(path.toFile());
                    r.setContent(databaseBackedContentManager.preStore(destPath, content));
                }
                records.add(r);
            }
            layerManager.getTopLayer().moveDirectoryInto(source, destination);
            database.saveRecords(records.toArray(ItemRecord[]::new));
        }
    }

    private Item.Type getItemType(Path path) {
        Item.Type type;
        if (Files.isDirectory(path)) {
            type = Item.Type.Directory;
        }
        else if (Files.isRegularFile(path)) {
            type = Item.Type.File;
        }
        else {
            throw new IllegalArgumentException("Path is not a file or directory: " + path);
        }
        return type;
    }

    @Override
    public void moveDirectoryInternal(String source, String destination) throws IOException {
        checkAllSourceFilesOnlyInTopLayer(source, "moveDirectoryInternal");
        layerManager.getTopLayer().moveDirectoryInternal(source, destination);
        // Update listing records for all files in the moved directory
        var items = database.listRecursive(source);
        for (var item : items) {
            var newPath = destination + "/" + item.getPath().substring(source.length() + 1);
            var records = database.getRecordsByPath(item.getPath());
            for (var record : records) {
                record.setPath(newPath);
            }
            database.saveRecords(records.toArray(ItemRecord[]::new));
        }
    }

    private void checkAllSourceFilesOnlyInTopLayer(String source, String methodName) throws IOException {
        List<String> itemsWithRecordsInOtherLayers = new ArrayList<>();
        for (var item : database.listRecursive(source)) {
            // The only layer that may contain records for the source directory is the top layer
            List<Long> layersContainingItem = database.findLayersContaining(item.getPath());
            if (layersContainingItem.size() > 1 || layersContainingItem.get(0) != layerManager.getTopLayer().getId()) {
                itemsWithRecordsInOtherLayers.add(item.getPath());
            }
        }
        if (!itemsWithRecordsInOtherLayers.isEmpty()) {
            throw new IllegalStateException("Cannot " + methodName + " because the following items are in multiple layers: " + itemsWithRecordsInOtherLayers);
        }
    }

    @Override
    public void deleteDirectory(String path) throws IOException {
        checkAllSourceFilesOnlyInTopLayer(path, "deleteDirectory");
        layerManager.getTopLayer().deleteDirectory(path);
        var items = database.listRecursive(path);
        items.add(new Item(path, Item.Type.Directory));
        var idsToDelete = items.stream().map(item -> {
            var records = database.getRecordsByPath(item.getPath());
            // If there are multiple records for the same path, something went wrong
            if (records.size() > 1) {
                throw new IllegalStateException("Found multiple records for path " + item.getPath());
            }
            return records.get(0).getGeneratedId();
        }).mapToLong(Long::longValue).toArray();
        database.deleteRecordsById(idsToDelete);
    }

    @Override
    public void deleteFiles(List<String> paths) throws IOException {
        var layerPaths = new HashMap<Long, List<String>>();
        for (String path : paths) {
            var layers = database.findLayersContaining(path);
            for (Long layerId : layers) {
                layerPaths.computeIfAbsent(layerId, k -> new ArrayList<>()).add(path);
            }
        }
        // Delete the files in each layer
        for (var entry : layerPaths.entrySet()) {
            var layer = layerManager.getLayer(entry.getKey());
            if (layer.isOpen()) {
                layer.deleteFiles(entry.getValue());
            }
            else {
                throw new IllegalStateException("Cannot delete files from closed layer " + layer.getId());
                // TODO: implement deletion from closed layers, by reopening the layer, deleting the files, and closing and archiving the layer again
            }
        }
    }

    @Override
    public void createDirectory(String path) throws IOException {
        layerManager.getTopLayer().createDirectory(path);
        database.addDirectory(layerManager.getTopLayer().getId(), path);
    }

    @Override
    public void copyDirectoryOutOf(String source, Path destination) throws IOException {
        var items = database.listRecursive(source);
        // Sort by ascending path length, so that we start with the deepest directories
        items.sort(Comparator.comparingInt(listingRecord -> Path.of(listingRecord.getPath()).getNameCount()));
        if (!items.isEmpty()) {
            var deepestDirectory = Path.of(items.get(0).getPath()).getParent();
            if (source.equals(deepestDirectory.toString())) {
                // the source is a leaf directory
                Files.createDirectories(destination.resolve(source));
            }
        }
        for (Item item : items) {
            if (item.getType().equals(Item.Type.Directory)) {
                Files.createDirectories(destination.resolve(item.getPath()));
            }
            else {
                try (OutputStream os = Files.newOutputStream(destination.resolve(item.getPath()))) {
                    IOUtils.copy(readFile(item.getPath()), os);
                }
            }
        }
    }
}
