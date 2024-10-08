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

import ch.qos.logback.classic.Level;
import io.dropwizard.util.DirectExecutorService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class LayerManagerNewTopLayerTest extends AbstractCapturingTest {

    // the method under test is also involved in tests for other LayerManagerImpl methods and LayeredItemStore methods

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        logged = captureLog(Level.ERROR, "nl.knaw.dans"); // override debug level
    }

    @Test
    public void should_log_already_archived() throws IOException {

        var layerManager = new LayerManagerImpl(stagingDir,
            new DmfTarArchiveProvider(
                getDmfTarRunner(),
                sshRunnerExpectsFileToExist(true)
            ),
            new DirectExecutorService()
        );
        var initialTopLayerId = layerManager.getTopLayer().getId();

        // Run the method under test
        assertThatThrownBy(layerManager::newTopLayer)
            .isInstanceOf(RuntimeException.class)
            .hasMessage("java.lang.IllegalStateException: Layer is already archived");

        // Check the logs
        var loggingEvent = logged.list.get(0);
        assertThat(loggingEvent.getLevel()).isEqualTo(Level.ERROR);
        assertThat(loggingEvent.getFormattedMessage())
            .isEqualTo("Error archiving layer with id " + initialTopLayerId);

        // Check stdout, different formats when running standalone or in a suite
        var contentString = stdout.toString();
        assertThat(contentString).contains("Error archiving layer with id " + initialTopLayerId);
        assertThat(contentString).contains("java.lang.IllegalStateException: Layer is already archived");
        assertThat(contentString).contains("at nl.knaw.dans.layerstore.LayerManagerNewTopLayerTest");
    }

    private static @NotNull SshRunner sshRunnerExpectsFileToExist(boolean exists) {
        return new SshRunner(Path.of("ssh"), "testuser", "dummyhost", Path.of("testarchive")) {

            @Override
            public boolean fileExists(String archiveName) {
                return exists;
            }
        };
    }

    private static @NotNull DmfTarRunner getDmfTarRunner() {
        return new DmfTarRunner(Path.of("dmftar"), "testuser", "dummyhost", Path.of("testarchive")) {

            @Override
            public void tarDirectory(Path directory, String archiveName) {
                // Do nothing
            }
        };
    }
}
