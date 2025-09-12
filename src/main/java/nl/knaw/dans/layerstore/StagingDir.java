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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;

/**
 * A very light-weight wrapper around a Path that represents a layer staging directory. It takes care of opening and closing the directory and getting the layer id from the directory name.
 *
 */
public class StagingDir {
    /**
     * Pattern for valid layer names. Layer names are Unix timestamps with the optional suffix '.closed', for closed layers. Current timestamps have 13 digits. After November 2286, timestamps will
     * have 14 digits.
     */
    private static final Pattern validLayerNamePattern = Pattern.compile("^\\d{13,}(.closed)?$");

    @Getter
    private Path path;

    /**
     * Creates a new StagingDir for the given layer id.
     *
     * @param stagingRoot the root directory for staging layers.
     * @param id          the layer id.
     */
    public StagingDir(@NonNull Path stagingRoot, @NonNull Long id) {
        validateName(id.toString());
        var closedPath = stagingRoot.resolve(id + ".closed");
        if (Files.exists(closedPath)) {
            path = closedPath;
        }
        else {
            path = stagingRoot.resolve(Long.toString(id));
        }
        validateOpenOrClosedState(path);
    }

    public StagingDir(@NonNull Path path) {
        this.path = path;
        validateOpenOrClosedState(path);
        validateName(path.getFileName().toString());
        if (Files.isRegularFile(path)) {
            throw new IllegalArgumentException("Not a directory: " + path);
        }

    }

    private static void validateOpenOrClosedState(Path path) {
        if (path.getFileName().toString().endsWith(".closed")) {
            var openPath = path.resolveSibling(path.getFileName().toString().substring(0, path.getFileName().toString().length() - ".closed".length()));
            if (Files.exists(openPath)) {
                throw new IllegalArgumentException("Layer " + path.getFileName().toString() + " is both open and closed");
            }
        }
        else if (Files.exists(path.resolveSibling(path.getFileName() + ".closed"))) {
            throw new IllegalArgumentException("Layer " + path.getFileName() + " is closed");
        }
        // If neither exists, that is OK. It means the layer is not staged.
    }

    private static void validateName(String name) {
        if (!validLayerNamePattern.matcher(name).matches()) {
            throw new IllegalArgumentException("Invalid layer name: " + name);
        }
    }

    public Long getId() {
        int lastDot = path.getFileName().toString().lastIndexOf('.');
        if (lastDot == -1) {
            return Long.parseLong(path.getFileName().toString());
        }
        else {
            return Long.parseLong(path.getFileName().toString().substring(0, lastDot));
        }
    }

    public boolean isClosed() {
        return !isStaged() || path.getFileName().toString().endsWith(".closed");
    }

    public boolean isOpen() {
        return !isClosed();
    }

    public void checkOpen() {
        if (!isOpen())
            throw new IllegalStateException("Layer is closed, but must be open for this operation");
    }

    public void checkClosed() {
        if (isOpen())
            throw new IllegalStateException("Layer is open, but must be closed for this operation");
    }

    public void close() throws IOException {
        checkOpen();
        path = Files.move(path, path.resolveSibling(path.getFileName() + ".closed"));
    }

    public void open() throws IOException {
        checkClosed();
        var openPath = path.resolveSibling(Long.toString(getId()));
        if (Files.exists(path)) {
            Files.move(path, openPath);
        }
        path = openPath;
    }

    public boolean isStaged() {
        return Files.exists(path);
    }

}
