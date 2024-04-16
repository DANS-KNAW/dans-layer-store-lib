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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * A test class that creates a test directory for each test method.
 */
public abstract class AbstractTestWithTestDir {
    protected final Path testDir = Path.of("target/test")
        .resolve(getClass().getSimpleName());

    protected final Path stagingDir = testDir.resolve("layer_staging");

    protected final Path archiveDir = testDir.resolve("layer_archive");

    @BeforeEach
    public void setUp() throws Exception {
        if (testDir.toFile().exists()) {
            // github stumbled: https://github.com/DANS-KNAW/dans-layer-store-lib/actions/runs/8705753485/job/23876831089?pr=7#step:4:106
            FileUtils.deleteDirectory(testDir.toFile());
        }
    }

    public static ByteArrayInputStream toInputStream(String testContent) {
        return new ByteArrayInputStream(toBytes(testContent));
    }

    public static byte[] toBytes(String testContent) {
        return testContent.getBytes(StandardCharsets.UTF_8);
    }

    public void createEmptyStagingDirFiles(String... paths) {
        for (String path : paths) {
            var message = "Could not create staging file: " + path;
            var parent = stagingDir.resolve(path).getParent().toFile();
            try {
                if ((!parent.exists() && !parent.mkdirs()) ||
                        !stagingDir.resolve(path).toFile().createNewFile()) {
                    throw new RuntimeException(message);
                }
            } catch (IOException e) {
                throw new RuntimeException(message, e);
            }
        }
    }

    /**
     * Assume that a bug is not yet fixed. This allows to skip assertions while still showing the code covered by the test.
        *
        * @param message the message to display
        */
    public void assumeNotYetFixed (String message) {
        assumeTrue(false, message);
    }
}
