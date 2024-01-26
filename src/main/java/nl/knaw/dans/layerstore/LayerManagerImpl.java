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

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class LayerManagerImpl implements LayerManager {

    private final Map<Long, Layer> layers = new HashMap<>();

    @NonNull
    private final Path stagingRoot;

    @NonNull
    private final Path archiveRoot;

    private final Executor archivingExecutor;

    private Layer topLayer;

    public LayerManagerImpl(@NonNull Path stagingRoot, @NonNull Path archiveRoot, Executor archivingExecutor) {
        this.stagingRoot = stagingRoot;
        this.archiveRoot = archiveRoot;
        this.archivingExecutor = Objects.requireNonNullElseGet(archivingExecutor, Executors::newSingleThreadExecutor);
        newTopLayer();
    }

    @Override
    public void newTopLayer() {
        var oldTopLayer = topLayer;
        long id = System.currentTimeMillis();
        Layer layer = new LayerImpl(id, stagingRoot.resolve(Long.toString(id)), new ZipArchive(archiveRoot.resolve(Long.toString(id) + ".zip")));
        layers.put(id, layer);
        topLayer = layer;
        if (oldTopLayer != null) {
            oldTopLayer.close();
            archivingExecutor.execute(() -> {
                try {
                    oldTopLayer.archive();
                    // TODO: store the result of the archiving process in the database (use ExecutorService?)
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    @Override
    public Layer getTopLayer() {
        if (topLayer == null) {
            topLayer = layers.keySet().stream().max(Long::compareTo).map(layers::get).orElse(null);
            if (topLayer == null) {
                throw new IllegalStateException("No top layer found");
            }
        }
        return topLayer;
    }

    @Override
    public Layer getLayer(long id) {
        if (layers.containsKey(id)) {
            return layers.get(id);
        }
        else if (stagingRoot.resolve(Long.toString(id)).toFile().exists() || archiveRoot.resolve(Long.toString(id)).toFile().exists()) {
            Layer layer = new LayerImpl(id, stagingRoot.resolve(Long.toString(id)), new ZipArchive(archiveRoot.resolve(Long.toString(id))));
            layers.put(id, layer);
            return layer;
        }
        else {
            throw new IllegalArgumentException("No layer found with id " + id);
        }
    }
}
