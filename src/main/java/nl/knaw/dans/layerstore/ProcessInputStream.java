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

import lombok.AllArgsConstructor;

import java.io.IOException;
import java.io.InputStream;

/**
 * An InputStream that checks the status of a process after each read operation and throws an IOException if the process has terminated with a non-zero exit code.
 */
public class ProcessInputStream extends InputStream {
    private final InputStream delegate;
    private final Process process;

    /**
     * Creates a new ProcessInputStream that reads from the given process's input stream.
     *
     * @param process the process to read from
     */
    public ProcessInputStream(Process process) {
        this.delegate = process.getInputStream();
        this.process = process;
    }

    @Override
    public int read() throws IOException {
        int result = delegate.read();
        checkProcessStatus();
        return result;
    }

    @Override
    public int read(byte[] b) throws IOException {
        int result = delegate.read(b);
        checkProcessStatus();
        return result;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int result = delegate.read(b, off, len);
        checkProcessStatus();
        return result;
    }

    private void checkProcessStatus() throws IOException {
        try {
            if (process.isAlive()) {
                return;
            }
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new IOException("Process terminated with error code: " + exitCode);
            }
        }
        catch (InterruptedException e) {
            throw new IOException("Interrupted while waiting for process", e);
        }
    }
}