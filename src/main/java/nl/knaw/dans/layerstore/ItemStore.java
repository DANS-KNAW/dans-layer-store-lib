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

import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;

/**
 * <p>
 * An ItemStore stores "items", which are files and directories. The name "item" was chosen to avoid name clashes with the java.io.File class. This library implements a layered way of storing items,
 * but other implementations are conceivable. The main reason for defining this interface in more general terms is to hide the details of the underlying storage mechanism from the client code.
 * </p>
 * <p>
 * An ItemStore has a storage root, which is implementation dependent.
 * </p>
 */
public interface ItemStore {
    /**
     * Returns the items in the given directory, taking into account the complete stack of layers.
     *
     * @param directoryPath the directory path relative to the storage root
     * @return the items in the directory
     * @throws IllegalArgumentException if the directory does not exist in any of the layers
     */
    List<Item> listDirectory(String directoryPath);

    /**
     * Returns the files and directories in the given directory and its subdirectories.
     *
     * @param directoryPath the directory path relative to the storage root
     * @return the items in the directory and its subdirectories
     * @throws IllegalArgumentException if the directory does not exist in any of the layers
     */
    List<Item> listRecursive(String directoryPath);

    /**
     * Returns whether a path with the given pattern exists in the store.
     *
     * @param path a path, which may contain the SQL wildcard character '%'
     * @return true if a path exists that matches the pattern, false otherwise
     */
    boolean existsPathLike(String path);

    /**
     * Opens an input stream to the file at the given path.
     *
     * @param path the path of the file relative to the storage root
     * @return an input stream to the file
     * @throws IllegalArgumentException if the path does not exist or is a directory
     */
    InputStream getInputStream(String path);

    /**
     * Writes the given content to the file at the given path. If the file does not exist yet, it is created.
     *
     * @param path    the path of the file relative to the storage root
     * @param content the content to write
     */
    void write(String path, InputStream content);

    /**
     * Moves the directory outside the store into the given destination. The parent of the destination must exist, but the destination itself must not exist yet.
     *
     * @param source      the path of the directory to move
     * @param destination the path of the destination directory relative to the storage root
     * @throws IllegalArgumentException if the source does not exist or is not a directory, or if the destination already exists, or its parent does not exist
     */
    void moveDirectoryInto(Path source, String destination);

    /**
     * Moves the directory inside the store to the given destination. The destination must not exist yet, but its parent must exist.
     *
     * @param source      the path of the directory to move relative to the storage root
     * @param destination the path of the destination directory relative to the storage root
     * @throws IllegalArgumentException if the source does not exist or is not a directory, or if the destination already exists, or its parent does not exist
     * @throws IllegalStateException    if the implementation has persisted the source directory, or part of it in a way that does not allow it to be moved
     */
    void moveDirectoryInternal(String source, String destination);

    /**
     * Deletes the directory at the given path, including all its contents.
     *
     * @param path the path of the directory relative to the storage root
     * @throws IllegalArgumentException if the path does not exist or is not a directory
     * @throws IllegalStateException    if the implementation has persisted the directory, or part of it in a way that does not allow it to be deleted
     */
    void deleteDirectory(String path);

    /**
     * Deletes the files at the given paths.
     *
     * @param paths the paths of the files relative to the storage root
     * @throws IllegalArgumentException if any of the paths does not exist or is a directory
     */
    void deleteFiles(List<String> paths);
}
