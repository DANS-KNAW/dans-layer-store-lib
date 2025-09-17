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

/**
 * Controls selecting and pre- and post-processing content for storage in the database.
 */
public interface DatabaseBackedContentManager {

    /**
     * Test if the content at the given path should be stored in the database.
     *
     * @param path the path of the content relative to the root of the store
     * @return true if the content should be stored in the database, false otherwise
     */
    boolean test(String path);

    /**
     * Process the content before it is stored in the database. The path of the file is also provided, so that the processor can decide to process the content based on the path. Note that the
     * implementation should take care to select the same paths as in {@link #postRetrieve(String, byte[])}
     * <p>
     * The most important use case for this method is to compress the content before it is stored in the database.
     *
     * @param path  the path of the content relative to the root of the store
     * @param bytes the content to be stored
     * @return the processed content
     */
    byte[] preStore(String path, byte[] bytes);

    /**
     * Process the content after it is retrieved from the database. The path of the file is also provided, so that the processor can decide to process the content based on the path. Note that the
     * implementation should take care to select the same paths as in {@link #preStore(String, byte[])}
     * <p>
     * The most important use case for this method is to decompress the content after it is retrieved from the database (assuming it was compressed before it was stored).
     *
     * @param path  the path of the content relative to the root of the store
     * @param bytes the content to be processed
     * @return the processed content
     */
    byte[] postRetrieve(String path, byte[] bytes);
}
