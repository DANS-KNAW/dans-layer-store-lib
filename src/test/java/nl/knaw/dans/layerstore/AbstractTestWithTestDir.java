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

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Base class for tests that require an isolated filesystem workspace.
 *
 * Provides a per-test-class directory under the build target directory and recreates it before each test run. The workspace includes locations
 * intended for staging files and archive output, allowing tests to create and inspect temporary file structures without sharing state with other test
 * classes.
 */
public abstract class AbstractTestWithTestDir {
    protected final Path testDir = Path.of("target/test")
        .resolve(getClass().getSimpleName());

    protected final Path stagingRoot = testDir.resolve("staging_root");

    protected final Path stagingDir = stagingRoot.resolve("1234567890123");

    protected final Path archiveRoot = testDir.resolve("archive_root");

    @BeforeEach
    public void setUp() throws Exception {
        if (testDir.toFile().exists()) {
            FileUtils.deleteDirectory(testDir.toFile());
        }
        Files.createDirectories(testDir);
    }

    /**
     * Writes content to a file in the staging directory, creating parent directories as needed.
     *
     * @param path    the path of the file to write, relative to the staging directory
     * @param content the text content to write using UTF-8 encoding
     * @throws IOException if the parent directories or staging file cannot be created or written
     */
    protected void writeStagingFile(String path, String content) throws IOException {
        var file = stagingDir.resolve(path);
        Files.createDirectories(file.getParent());
        Files.writeString(file, content, StandardCharsets.UTF_8);
    }

    /**
     * Creates empty files in the staging directory for the specified relative paths. Parent directories are created as needed.
     *
     * @param paths the paths of the empty staging files to create, relative to the staging directory
     * @throws RuntimeException if a staging file or one of its parent directories cannot be created
     */
    protected void createEmptyStagingFiles(String... paths) {
        for (String path : paths) {
            var file = stagingDir.resolve(path);
            try {
                Files.createDirectories(file.getParent());
                Files.createFile(file);
            }
            catch (IOException e) {
                throw new RuntimeException("Could not create staging file: " + path, e);
            }
        }
    }
}
