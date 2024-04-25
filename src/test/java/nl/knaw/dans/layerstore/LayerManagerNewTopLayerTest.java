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
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class LayerManagerNewTopLayerTest extends AbstractTestWithTestDir {
    private ListAppender<ILoggingEvent> listAppender;

    private static class ThrowingArchiveProvider implements ArchiveProvider {

        @Override
        public Archive createArchive(String path) {
            return new Archive() {

                @Override
                public InputStream readFile(String filePath) {
                    return null;
                }

                @Override
                public void unarchiveTo(Path stagingDir) {

                }

                @Override
                public void archiveFrom(Path stagingDir) {
                    throw new RuntimeException("archiveFrom failed");
                }

                @Override
                public boolean isArchived() {
                    return false;
                }

                @Override
                public boolean fileExists(String filePath) {
                    return false;
                }
            };
        }

        @Override
        public boolean exists(String path) {
            return false;
        }
    }

    @BeforeEach
    public void captureLog() {
        var logger = (Logger) LoggerFactory.getLogger(LayerManagerImpl.class);
        listAppender = new ListAppender<>();
        listAppender.start();
        logger.setLevel(Level.ERROR);
        logger.addAppender(listAppender);
    }

    private static ByteArrayOutputStream captureStdout() {
        var outContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent));
        return outContent;
    }

    @Test
    public void should_log_an_archive_failure() throws IOException {

        var layerManager = new LayerManagerImpl(stagingDir, new ThrowingArchiveProvider(), new DirectExecutor());
        var initialTopLayerId = layerManager.getTopLayer().getId();

        var outContent = captureStdout();

        // Run the method under test
        assertThatThrownBy(layerManager::newTopLayer)
            .isInstanceOf(RuntimeException.class)
            .hasMessage("java.lang.RuntimeException: archiveFrom failed");

        // Check the logs
        var loggingEvent = listAppender.list.get(0);
        assertThat(loggingEvent.getLevel()).isEqualTo(Level.ERROR);
        assertThat(loggingEvent.getFormattedMessage())
            .isEqualTo("Error archiving layer with id " + initialTopLayerId);

        // Check stdout, different formats when running standalone or in a suite
        var contentString = outContent.toString();
        assertThat(contentString).contains("Error archiving layer with id " + initialTopLayerId);
        assertThat(contentString).contains("java.lang.RuntimeException: archiveFrom failed");
        assertThat(contentString).contains("at nl.knaw.dans.layerstore.LayerManagerNewTopLayerTest");
    }

    // the method is also involved in tests for other LayerManagerImpl methods and LayeredItemStore methods
}
