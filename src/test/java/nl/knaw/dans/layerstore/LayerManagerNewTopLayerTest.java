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

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class LayerManagerNewTopLayerTest extends AbstractCapturingTest {

    // the method under test is also involved in tests for other LayerManagerImpl methods and LayeredItemStore methods

    @Test
    public void should_log_already_archived() throws IOException {

        var layerManager = new LayerManagerImpl(stagingDir,
            new DmfTarArchiveProvider(
                getNoopDmfTarRunner(),
                sshRunnerExpectsFileToExist(true)
            ),
            new DirectLayerArchiver()
        );

        // Run the method under test
        assertThatThrownBy(layerManager::newTopLayer)
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Layer is already archived");
    }

    private static @NotNull SshRunner sshRunnerExpectsFileToExist(boolean exists) {
        return new SshRunner(Path.of("ssh"), "testuser", "dummyhost", Path.of("testarchive")) {

            @Override
            public boolean fileExists(String archiveName) {
                return exists;
            }
        };
    }

    private static @NotNull DmfTarRunner getNoopDmfTarRunner() {
        return new DmfTarRunner(Path.of("dmftar"), "testuser", "dummyhost", Path.of("testarchive")) {

            @Override
            public void tarDirectory(Path directory, String archiveName) {
                // Do nothing
            }
        };
    }
}
