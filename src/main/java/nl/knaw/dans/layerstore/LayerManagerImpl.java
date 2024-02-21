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
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

@Slf4j
public class LayerManagerImpl implements LayerManager {

    private final Path stagingRoot;

    private final ArchiveProvider archiveProvider;

    private final Executor archivingExecutor;

    @Getter
    private Layer topLayer;

    public LayerManagerImpl(@NonNull Path stagingRoot, @NonNull ArchiveProvider archiveProvider, Executor archivingExecutor) {
        this.stagingRoot = stagingRoot;
        this.archivingExecutor = Objects.requireNonNullElseGet(archivingExecutor, Executors::newSingleThreadExecutor);
        this.archiveProvider = archiveProvider;
        initTopLayer();
    }

    public LayerManagerImpl(@NonNull Path stagingRoot, @NonNull ArchiveProvider archiveProvider) {
        this(stagingRoot, archiveProvider, null);
    }

    @SneakyThrows
    private void initTopLayer() {
        List<Path> paths;
        try (Stream<Path> pathStream = Files.list(stagingRoot)) {
            paths = pathStream.toList();
        }
        if (paths.isEmpty()) {
            topLayer = createNewTopLayer();
        }
        else {
            long id = paths.stream()
                .map(Path::getFileName)
                .map(Path::toString)
                .mapToLong(Long::parseLong)
                .max()
                .orElseThrow();
            topLayer = new LayerImpl(id, stagingRoot.resolve(Long.toString(id)), archiveProvider.createArchive(Long.toString(id)));
        }
    }

    private Layer createNewTopLayer() {
        long id = System.currentTimeMillis();
        log.debug("Creating new top layer with id {}", id);
        return new LayerImpl(id, stagingRoot.resolve(Long.toString(id)), archiveProvider.createArchive(Long.toString(id)));
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

    @Override
    public Layer getLayer(long id) {
        if (stagingRoot.resolve(Long.toString(id)).toFile().exists() || archiveProvider.exists(Long.toString(id))) {
            return new LayerImpl(id, stagingRoot.resolve(Long.toString(id)), archiveProvider.createArchive(Long.toString(id)));
        }
        else {
            throw new IllegalArgumentException("No layer found with id " + id);
        }
    }
}
