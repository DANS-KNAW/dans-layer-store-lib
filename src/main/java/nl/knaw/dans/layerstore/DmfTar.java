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

import java.io.InputStream;
import java.nio.file.Path;

@AllArgsConstructor
public class DmfTar {
    private final Path dmfTarExecutable;
    private final String user;
    private final String host;
    private final Path remoteBaseDir;

    public void tarDirectory(Path directory, String archiveName) {
        var runner = new ProcessRunner(dmfTarExecutable.toAbsolutePath().toString(),
            "-cf",
            getRemotePath(archiveName),
            ".");
        // Always tar relative to the current directory (.) and then set that to the storage root, so that the entries in the tar file are
        // relative to the storage root
        runner.setWorkingDirectory(directory.toAbsolutePath().toString());
        var result = runner.runToEnd();
        if (result.getExitCode() != 0) {
            throw new RuntimeException("Failed to create tar archive: " + result.getErrorOutput());
        }
    }

    public InputStream readFile(String archiveName, String fileName) {
        var runner = new ProcessRunner(dmfTarExecutable.toAbsolutePath().toString(),
            "-o=-O", // Send extra option to underlying tar command to write to stdout
            "-q", // Suppress dmftar messages to stdout (sic!)
            "-xf", // Extract files from archive
            getRemotePath(archiveName), fileName);
        return new ProcessInputStream(runner.start());
    }


    private String getRemotePath(String archiveName) {
        return user + "@" + host + ":" + remoteBaseDir.resolve(archiveName);
    }
}
