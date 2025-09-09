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

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;

@Slf4j
public class LayerManagerImpl implements LayerManager {

    private final Path stagingRoot;

    private final ArchiveProvider archiveProvider;

    private final LayerArchiver layerArchiver;

    @Getter
    private Layer topLayer;

    /**
     * Creates a new LayerManagerImpl.
     *
     * @param stagingRoot     the root directory for staging layers.
     * @param archiveProvider the archive provider to use.
     * @param layerArchiver   the layer archiver to use.
     * @throws IOException if the staging root directory cannot be created.
     */
    public LayerManagerImpl(@NonNull Path stagingRoot, @NonNull ArchiveProvider archiveProvider, @NonNull LayerArchiver layerArchiver) throws IOException {
        this.stagingRoot = stagingRoot;
        this.layerArchiver = layerArchiver;
        this.archiveProvider = archiveProvider;
        if (Files.notExists(this.stagingRoot)) {
            Files.createDirectories(this.stagingRoot);
        }
        try (var pathStream = Files.list(this.stagingRoot)) {
            var id = pathStream
                .map(this::toValidLayerName)
                .mapToLong(Long::parseLong)
                .max();
            if (id.isPresent()) {
                topLayer = new LayerImpl(id.getAsLong(), this.stagingRoot.resolve(Long.toString(id.getAsLong())), this.archiveProvider.createArchive(id.getAsLong()));
            }
        }
    }

    private String toValidLayerName(Path path) {
        if (!path.toFile().isDirectory()) {
            throw new IllegalStateException("Not a directory: " + path);
        }
        if (!path.getFileName().toString().matches("\\d{13,}")) {
            // more than 13 digits in nov 2286, comma allows a longer future
            throw new IllegalStateException("Not a timestamp: " + path);
        }
        return path.getFileName().toString();
    }

    @Override
    public void newTopLayer() throws IOException {
        var oldTopLayer = topLayer;
        // Wait 2 millis before creating a new top layer to avoid name collision
        try {
            Thread.sleep(2);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        long id = System.currentTimeMillis();
        log.debug("Creating new top layer with id {}", id);
        var stagingDir = stagingRoot.resolve(Long.toString(id));
        var newLayer = new LayerImpl(id, stagingDir, archiveProvider.createArchive(id));
        Files.createDirectories(stagingDir);
        topLayer = newLayer;

        if (oldTopLayer != null) {
            oldTopLayer.close();
            log.debug("Scheduling old top layer with id {} for archiving", oldTopLayer.getId());
            archive(oldTopLayer);
        }
        else {
            log.debug("No old top layer to archive");
        }
    }

    private void archive(Layer layer) {
        layerArchiver.archive(layer);
    }

    public List<Long> listLayerIds() throws IOException {
        try (var pathStream = Files.list(stagingRoot)) {
            var allIds = new HashSet<>(pathStream
                .map(this::toValidLayerName)
                .map(Long::valueOf)
                .toList());
            allIds.addAll(archiveProvider.listArchivedLayers());
            return allIds.stream().sorted().toList();
        }
    }

    @Override
    public Layer getLayer(long id) {
        if (id == topLayer.getId()) {
            // safeguard/shortcut: a fresh top layer that never received content has no directory in the staging root
            return topLayer;
        }
        else if (stagingRoot.resolve(Long.toString(id)).toFile().exists() || archiveProvider.exists(id)) {
            return new LayerImpl(id, stagingRoot.resolve(Long.toString(id)), archiveProvider.createArchive(id));
        }
        else {
            throw new IllegalArgumentException("No layer found with id " + id);
        }
    }
}
