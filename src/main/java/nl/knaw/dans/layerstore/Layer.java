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

/**
 * This interface represents a layer in the {@link LayeredItemStore}. See <a href="/#layer-status">this page</a> for more information about layer status.
 */
public interface Layer {
    /**
     * Returns the id of the layer.
     *
     * @return the id of the layer
     */
    long getId();

    /**
     * Changes the state of the layer to 'closed'.
     *
     * @throws IllegalStateException if the layer is already closed
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

    /**
     * Creates a directory at the given path. Not allowed when the layer is closed.
     *
     * @param path the path of the directory relative to the storage root
     * @throws IOException if the directory cannot be created
     */
    void createDirectory(String path) throws IOException;

    /**
     * Deletes the directory at the given path, including all its contents. Not allowed when the layer is closed.
     *
     * @param path the path of the directory relative to the storage root
     * @throws IOException if the directory cannot be deleted
     */
    void deleteDirectory(String path) throws IOException;

    /**
     * Returns whether a file exists at the given path in this layer.
     *
     * @param path the path of the file relative to the storage root
     * @return whether the file exists
     * @throws IOException if the existence of the file cannot be determined
     */
    boolean fileExists(String path) throws IOException;

    /**
     * Reads the file at the given path. Note, that this may take a long time if the layer is archived, especially when the archive is stored on tape.
     *
     * @param path the path of the file relative to the storage root
     * @return an input stream for reading the file; the caller is responsible for closing the stream
     * @throws IOException if the file cannot be read
     */
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

    /**
     * Moves the directory at <code>source</code> into the directory at <code>destination</code>. Not allowed when the layer is closed.
     *
     * @param source      the path of the directory to be moved; must be an existing directory
     * @param destination the path of the directory to move into; the directory is created if it does not exist yet
     * @throws IOException if the directory cannot be moved
     */
    void moveDirectoryInto(Path source, String destination) throws IOException;

    /**
     * Moves the directory at <code>source</code> to <code>destination</code>, where both paths are relative to the root of the layer.
     *
     * @param source      the path of the directory to be moved; must be an existing directory
     * @param destination the path of the destination directory; must not exist yet
     * @throws IOException if the directory cannot be moved
     */
    void moveDirectoryInternal(String source, String destination) throws IOException;

    /**
     * Returns the size of the layer in bytes. If the layer is archived, this may take a long time, especially when the archive is stored on tape.
     *
     * @return the size of the layer in bytes
     * @throws IOException if the size cannot be determined
     */
    long getSizeInBytes() throws IOException;

    /**
     * Lists the items in this layer.
     *
     * @return the items in this layer
     * @throws IOException if the items cannot be listed
     */
    Iterator<Item> listAllItems() throws IOException;
}
