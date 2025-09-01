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
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@Slf4j
public class LayerManagerImpl implements LayerManager {

    private final Path stagingRoot;

    private final ArchiveProvider archiveProvider;

    private final Executor archivingExecutor;

    @Getter
    private Layer topLayer;

    public LayerManagerImpl(@NonNull Path stagingRoot, @NonNull ArchiveProvider archiveProvider, Executor archivingExecutor) throws IOException {
        this.stagingRoot = stagingRoot;
        this.archivingExecutor = Objects.requireNonNullElseGet(archivingExecutor, Executors::newSingleThreadExecutor);
        this.archiveProvider = archiveProvider;
        initTopLayer();
    }

    public LayerManagerImpl(@NonNull Path stagingRoot, @NonNull ArchiveProvider archiveProvider) throws IOException {
        this(stagingRoot, archiveProvider, null);
    }

    private void initTopLayer() throws IOException {
        if (Files.notExists(stagingRoot)) {
            Files.createDirectories(stagingRoot);
        }
        try (var pathStream = Files.list(stagingRoot)) {
            long id = pathStream
                .map(this::toValidLayerName)
                .mapToLong(Long::parseLong)
                .max()
                .orElse(createNewTopLayer().getId());
            topLayer = new LayerImpl(id, stagingRoot.resolve(Long.toString(id)), archiveProvider.createArchive(id));
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

    private Layer createNewTopLayer() {
        long id = System.currentTimeMillis();
        log.debug("Creating new top layer with id {}", id);
        return new LayerImpl(id, stagingRoot.resolve(Long.toString(id)), archiveProvider.createArchive(id));
    }

    @Override
    public void newTopLayer() {
        var oldTopLayer = topLayer;
        topLayer = createNewTopLayer();
        oldTopLayer.close();
        log.debug("Scheduling old top layer with id {} for archiving", oldTopLayer.getId());
        archive(oldTopLayer);
    }

    private void archive(Layer layer) {
        archivingExecutor.execute(() -> {
            try {
                layer.archive();
            }
            catch (Exception e) {
                log.error("Error archiving layer with id {}", layer.getId(), e);
                throw new RuntimeException(e);
            }
        });
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
