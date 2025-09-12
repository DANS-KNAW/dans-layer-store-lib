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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;

@Slf4j
public class LayerManagerImpl implements LayerManager {
    /**
     * Pattern for valid layer names. Layer names are Unix timestamps with the optional suffix '.closed', for closed layers. Current timestamps have 13 digits. After November 2286, timestamps will
     * have 14 digits.
     */
    private static final Pattern validLayerNamePattern = Pattern.compile("^\\d{13,}(.closed)?$");

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
            pathStream
                .map(StagingDir::new)
                .max(Comparator.comparingLong(StagingDir::getId))
                .ifPresent(maxDir -> {
                        topLayer = new LayerImpl(maxDir.getId(), maxDir, this.archiveProvider.createArchive(maxDir.getId()));
                    }
                );
        }
    }

    @Override
    public Layer newTopLayer() throws IOException {
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
        Files.createDirectories(stagingDir);
        var newLayer = new LayerImpl(id, new StagingDir(stagingDir), archiveProvider.createArchive(id));
        topLayer = newLayer;

        if (oldTopLayer != null) {
            oldTopLayer.close();
            log.debug("Scheduling old top layer with id {} for archiving", oldTopLayer.getId());
            archive(oldTopLayer);
        }
        else {
            log.debug("No old top layer to archive");
        }
        return newLayer;
    }

    private void archive(Layer layer) {
        layerArchiver.archive(layer);
    }

    public List<Long> listLayerIds() throws IOException {
        try (var pathStream = Files.list(stagingRoot)) {
            var allIds = new HashSet<>(pathStream
                .map(StagingDir::new)
                .map(StagingDir::getId)
                .toList());
            allIds.addAll(archiveProvider.listArchivedLayers());
            return allIds.stream().sorted().toList();
        }
    }

    @Override
    public Layer getLayer(long id) {
        if (topLayer != null && id == topLayer.getId()) {
            return topLayer;
        }
        else {
            var stagingDir = new StagingDir(stagingRoot, id);
            if (stagingDir.isStaged() || archiveProvider.exists(id)) {
                return new LayerImpl(id, stagingDir, archiveProvider.createArchive(id));
            }
            else {
                throw new IllegalArgumentException("No layer found with id " + id);
            }
        }
    }
}
