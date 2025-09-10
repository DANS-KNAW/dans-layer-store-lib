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

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * An {@link LayerArchiver} that archives layers in a separate thread. It also checks that the items found on storage are the same as the items found in the database for the layer.
 */
@Slf4j
public class ConsistencyCheckingAsyncLayerArchiver implements LayerArchiver {
    private final LayerConsistencyChecker consistencyChecker;
    private final Executor executor;

    public ConsistencyCheckingAsyncLayerArchiver(LayerConsistencyChecker consistencyChecker, Executor executor) {
        this.consistencyChecker = consistencyChecker;
        this.executor = executor == null ? Executors.newSingleThreadExecutor() : executor;
    }

    @Override
    public void archive(Layer layer) {
        executor.execute(() -> {
            try {
                log.info("Checking consistency of layer {}", layer.getId());
                consistencyChecker.check(layer);
                log.info("Archiving layer {}", layer.getId());
                layer.archive();
                log.info("Layer {} archived", layer.getId());
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
