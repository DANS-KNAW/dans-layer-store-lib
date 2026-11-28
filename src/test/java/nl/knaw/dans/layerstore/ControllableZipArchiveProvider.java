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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link ZipArchiveProvider} for tests, of which the recall behavior can be controlled and the unarchiving can be held up. The state is shared by all archives created by the provider, as the
 * layer manager creates a new archive object every time a layer is looked up.
 */
class ControllableZipArchiveProvider implements ArchiveProvider {
    private final ZipArchiveProvider delegate;

    final AtomicInteger recallRequests = new AtomicInteger();
    final CountDownLatch unarchiveStarted = new CountDownLatch(1);
    final CountDownLatch unarchiveMayProceed = new CountDownLatch(1);
    volatile boolean recalled = true;
    volatile boolean holdUpUnarchive = false;

    ControllableZipArchiveProvider(Path archiveRoot) {
        this.delegate = new ZipArchiveProvider(archiveRoot);
    }

    @Override
    public Archive createArchive(long layerId) {
        var archive = delegate.createArchive(layerId);
        return new Archive() {

            @Override
            public InputStream readFile(String filePath) throws IOException {
                return archive.readFile(filePath);
            }

            @Override
            public void unarchiveTo(Path stagingDir) {
                unarchiveStarted.countDown();
                if (holdUpUnarchive) {
                    try {
                        if (!unarchiveMayProceed.await(10, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("Unarchiving was not allowed to proceed in time");
                        }
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    }
                }
                archive.unarchiveTo(stagingDir);
            }

            @Override
            public void archiveFrom(Path stagingDir) {
                archive.archiveFrom(stagingDir);
            }

            @Override
            public boolean fileExists(String filePath) {
                return archive.fileExists(filePath);
            }

            @Override
            public Iterator<Item> listAllItems() throws IOException {
                return archive.listAllItems();
            }

            @Override
            public void requestRecall() {
                recallRequests.incrementAndGet();
            }

            @Override
            public boolean isRecalled() {
                return recalled;
            }
        };
    }

    @Override
    public boolean exists(long layerId) {
        return delegate.exists(layerId);
    }

    @Override
    public List<Long> listLayerIds() throws IOException {
        return delegate.listLayerIds();
    }

    @Override
    public void validateRoot() throws IOException {
        delegate.validateRoot();
    }
}
