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
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestUtils {
    public static ListAppender<ILoggingEvent> captureLog() {
        var logger = (Logger) LoggerFactory.getLogger(LayerManagerImpl.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        logger.setLevel(Level.ERROR);
        logger.addAppender(listAppender);
        return listAppender;
    }

    public static ByteArrayOutputStream captureStdout() {
        var outContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent));
        return outContent;
    }

    public static ByteArrayInputStream toInputStream(String testContent) {
        return new ByteArrayInputStream(toBytes(testContent));
    }

    public static byte[] toBytes(String testContent) {
        return testContent.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Assume that a bug is not yet fixed. This allows to skip assertions while still showing the code is covered by the test.
     *
     * @param message the message to display
     */
    public static void assumeNotYetFixed(String message) {
        assumeTrue(false, message);
    }
}