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
import java.util.List;

public interface Layer {
    /**
     * Returns the id of the layer.
     *
     * @return the id of the layer
     */
    long getId();

    /**
     * Changes the state of the layer to closed.
     */
    void close();

    /**
     * Changes the state of the layer to open. It also stages the archive file in the staging directory. This operation is not allowed when the layer is not closed or not yet archived.
     */
    void reopen() throws IOException;

    /**
     * Returns whether the layer is open.
     *
     * @return whether the layer is open
     */
    boolean isOpen();

    /**
     * Turns the layer into an archive file.
     */
    void archive();

    /**
     * Returns whether the layer is archived.
     *
     * @return whether the layer is archived
     */
    boolean isArchived();

    void createDirectory(String path) throws IOException;

    void deleteDirectory(String path) throws IOException;

    boolean fileExists(String path) throws IOException;

    InputStream readFile(String path) throws IOException;

    /**
     * Writes the content of the given input stream to the file at the given path. Not allowed when the layer is closed. If the file already exists, it is overwritten.
     *
     * @param filePath the path of the file relative to the storage root
     * @param content  the content of the file
     * @throws IOException if the file cannot be written
     */
    void writeFile(String filePath, InputStream content) throws IOException;

    /**
     * Deletes the files pointed to by <code>paths</code>. Not allowed when the layer is closed.
     *
     * @param paths the paths of the files to be deleted
     * @throws IOException if the files cannot be deleted
     */
    void deleteFiles(List<String> paths) throws IOException;

    void moveDirectoryInto(Path source, String destination) throws IOException;

    void moveDirectoryInternal(String source, String destination) throws IOException;
}
