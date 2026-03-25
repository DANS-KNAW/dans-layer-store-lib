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
import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;
import java.util.List;

@Slf4j
class LayerImpl implements Layer {

    @Getter
    private final long id;

    @NonNull
    private final StagingDir stagingDir;

    @NonNull
    private final Archive archive;

    LayerImpl(long id, @NonNull StagingDir stagingDir, @NonNull Archive archive) {
        this.id = id;
        this.stagingDir = stagingDir;
        this.archive = archive;
    }

    @Override
    public synchronized State getState() {
        if (stagingDir.isStaged()) {
            if (stagingDir.isOpen()) {
                return State.OPEN;
            }
            if (stagingDir.isClosed()) {
                return State.CLOSED;
            }
            if (stagingDir.isPartial()) {
                return State.ARCHIVED;
            }
        }
        if (archive.isArchived()) {
            return State.ARCHIVED;
        }
        throw new IllegalStateException("Layer " + id + " is in an inconsistent state: not staged and not archived");
    }

    @Override
    public void createDirectory(String path) throws IOException {
        checkState(State.OPEN);
        validatePath(path);
        Files.createDirectories(stagingDir.getPath().resolve(path));
    }

    private void checkState(State expectedState) {
        State currentState = getState();
        if (currentState != expectedState) {
            throw new IllegalStateException(String.format("Layer %d is in state %s, but must be in state %s for this operation", id, currentState, expectedState));
        }
    }

    @Override
    public void deleteFiles(List<String> paths) throws IOException {
        checkState(State.OPEN);
        if (paths == null)
            throw new IllegalArgumentException("Paths cannot be null");
        for (String path : paths) {
            validatePath(path);
            Files.delete(stagingDir.getPath().resolve(path));
        }
    }

    /*
     * This method is synchronized because the layer might otherwise be closed just after the check. Note, that after the file handle is returned, the layer may be closed, but
     * that is not a problem because the file handle is still valid until it is closed, even if the directory containing the file is deleted.
     */
    @Override
    public synchronized InputStream readFile(String path) throws IOException {
        if (getState() == State.ARCHIVED) {
            return archive.readFile(path);
        }
        else {
            return readFromStaging(path);
        }
    }

    private InputStream readFromStaging(String path) throws IOException {
        return Files.newInputStream(stagingDir.getPath().resolve(path));
    }

    @Override
    public synchronized void close() {
        checkState(State.OPEN);
        try {
            stagingDir.close();
        }
        catch (IOException e) {
            log.error("Error closing layer", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void reopen() throws IOException {
        checkState(State.ARCHIVED);
        stagingDir.open();
        archive.unarchiveTo(stagingDir.getPath());
    }

    @Override
    public synchronized void archive(boolean overwrite) {
        checkState(State.CLOSED);
        if (!overwrite && archive.isArchived()) {
            throw new IllegalArgumentException("Layer " + id + " is already archived");
        }
        try {
            log.debug("Start archiving layer {}", id);
            archive.archiveFrom(stagingDir.getPath());
            log.debug("Deleting staging directory {}", stagingDir.getPath());
            stagingDir.delete();
            log.debug("Staging directory {} deleted", stagingDir.getPath());
        }
        catch (IOException e) {
            log.error("Error archiving layer", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeFile(String filePath, InputStream content) throws IOException {
        checkState(State.OPEN);
        validatePath(filePath);
        Files.copy(content, stagingDir.getPath().resolve(filePath), StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public void moveDirectoryInto(Path source, String destination) throws IOException {
        checkState(State.OPEN);
        validatePath(destination);
        var destinationPath = stagingDir.getPath().resolve(destination);
        Files.move(source, destinationPath);
    }

    @Override
    public boolean fileExists(String path) throws IOException {
        validatePath(path);
        if (getState() == State.ARCHIVED) {
            return archive.fileExists(path);
        }
        else {
            return fileExistsInStaging(path);
        }
    }

    private boolean fileExistsInStaging(String path) {
        return Files.exists(stagingDir.getPath().resolve(path));
    }

    @Override
    public void moveDirectoryInternal(String source, String destination) throws IOException {
        checkState(State.OPEN);
        validatePath(source);
        validatePath(destination);
        Files.move(stagingDir.getPath().resolve(source), stagingDir.getPath().resolve(destination));
    }

    @Override
    public void deleteDirectory(String path) throws IOException {
        checkState(State.OPEN);
        validatePath(path);
        FileUtils.deleteDirectory(stagingDir.getPath().resolve(path).toFile());
    }

    @Override
    public long getSizeInBytes() throws IOException {
        if (getState() == State.OPEN || getState() == State.CLOSED) {
            return FileUtils.sizeOfDirectory(stagingDir.getPath().toFile());
        }
        else {
            // TODO: replace with implementation that reads total size from database?
            throw new UnsupportedOperationException("Layer is ARCHIVED");
        }
    }

    private void validatePath(String path) {
        if (path == null)
            throw new IllegalArgumentException("Path cannot be null");
        if (path.isBlank() && !path.isEmpty())
            throw new IllegalArgumentException("Path cannot be blank");
        var pathInStagingDir = stagingDir.getPath().resolve(path).normalize();
        // Check if the path is outside the staging dir
        if (!pathInStagingDir.startsWith(stagingDir.getPath()))
            throw new IllegalArgumentException("Path is outside staging directory");
    }

    // Cannot be reliably called during archiving, because the layer might be closed just after the check
    @Override
    public Iterator<Item> listAllItems() throws IOException {
        if (getState() == State.ARCHIVED) {
            return archive.listAllItems();
        }
        else {
            return new DirectoryTreeItemIterator(stagingDir.getPath());
        }
    }
}
