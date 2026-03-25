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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class LayerReopenReproductionTest extends AbstractTestWithTestDir {

    @Test
    public void should_allow_reopening_from_CLOSED_state() throws Exception {
        Files.createDirectories(stagingRoot);
        var layer = new LayerImpl(1234567890123L, new StagingDir(stagingRoot, 1234567890123L), new ZipArchive(archiveRoot.resolve("test.zip")));
        createEmptyStagingDirFiles("file1");
        layer.close();
        assertThat(layer.getState()).isEqualTo(Layer.State.CLOSED);
        assertThat(stagingRoot.resolve("1234567890123.closed")).exists();

        layer.reopen();

        assertThat(layer.getState()).isEqualTo(Layer.State.OPEN);
        assertThat(stagingRoot.resolve("1234567890123")).exists();
        assertThat(stagingRoot.resolve("1234567890123.closed")).doesNotExist();
    }

    @Test
    public void should_throw_exception_and_cleanup_when_unarchiving_fails() throws Exception {
        Files.createDirectories(archiveRoot);
        // Create a faulty archive that throws an exception during unarchiving
        Archive faultyArchive = new Archive() {
            @Override
            public java.io.InputStream readFile(String filePath) throws IOException {
                return null;
            }

            @Override
            public void unarchiveTo(Path stagingDir) {
                try {
                    Files.createDirectories(stagingDir);
                    Files.createFile(stagingDir.resolve("partial_file"));
                } catch (IOException e) {
                    // ignore
                }
                throw new RuntimeException("Unarchiving failed");
            }

            @Override
            public void archiveFrom(Path stagingDir) {
            }

            @Override
            public boolean isArchived() {
                return true;
            }

            @Override
            public boolean fileExists(String filePath) {
                return false;
            }

            @Override
            public java.util.Iterator<Item> listAllItems() throws IOException {
                return null;
            }
        };

        var layer = new LayerImpl(1234567890123L, new StagingDir(stagingRoot, 1234567890123L), faultyArchive);
        assertThat(layer.getState()).isEqualTo(Layer.State.ARCHIVED);

        assertThatThrownBy(layer::reopen)
            .isInstanceOf(RuntimeException.class)
            .hasMessage("Unarchiving failed");

        assertThat(layer.getState()).isEqualTo(Layer.State.ARCHIVED);
        assertThat(stagingRoot.resolve("1234567890123.partial")).doesNotExist();
        assertThat(stagingRoot.resolve("1234567890123")).doesNotExist();
    }
}
