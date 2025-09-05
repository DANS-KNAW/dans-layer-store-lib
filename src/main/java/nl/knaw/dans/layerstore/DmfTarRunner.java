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
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lib.util.ProcessInputStream;
import nl.knaw.dans.lib.util.ProcessRunner;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.io.LineIterator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.stream.Stream;

@AllArgsConstructor
@Slf4j
public class DmfTarRunner {
    private final Path dmfTarExecutable;
    private final String user;
    private final String host;
    private final Path remoteBaseDir;

    public void tarDirectory(Path directory, String archiveName) {
        var commandLine = CommandLine.parse(dmfTarExecutable.toAbsolutePath() + " -cf " + getRemotePath(archiveName) + " .");
        var executor = DefaultExecutor.builder()
            .setWorkingDirectory(directory.toAbsolutePath().toFile())
            .get();
        try {
            executor.execute(commandLine);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create tar archive: " + e.getMessage(), e);
        }
    }

    public InputStream readFile(String archiveName, String fileName) {
        var runner = new ProcessRunner(dmfTarExecutable.toAbsolutePath().toString(),
            "-o=-O", // Send extra option to underlying tar command to write to stdout
            "-q", // Suppress dmftar messages to stdout
            "-xf", // Extract files from archive
            getRemotePath(archiveName), fileName);
        return new ProcessInputStream(runner.start());
    }

    public boolean fileExists(String archiveName, String fileName) throws IOException {
        var runner = new ProcessRunner(dmfTarExecutable.toAbsolutePath().toString(),
            "-tf", // List files in archive
            "-q", // Suppress dmftar messages to stdout
            getRemotePath(archiveName));
        var reader = new BufferedReader(new InputStreamReader(new ProcessInputStream(runner.start()), StandardCharsets.UTF_8));
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.equals(fileName)) {
                return true;
            }
        }
        return false;
    }

    public Iterator<String> listFiles(String archiveName) {
        var runner = new ProcessRunner(dmfTarExecutable.toAbsolutePath().toString(),
            "-tf", // List files in archive
            "-q", // Suppress dmftar messages to stdout
            getRemotePath(archiveName));
        var process = runner.start();
        var reader = new BufferedReader(new InputStreamReader(new ProcessInputStream(process), StandardCharsets.UTF_8));
        return new LineIterator(reader);
    }

    private String getRemotePath(String archiveName) {
        return user + "@" + host + ":" + remoteBaseDir.resolve(archiveName);
    }
}
