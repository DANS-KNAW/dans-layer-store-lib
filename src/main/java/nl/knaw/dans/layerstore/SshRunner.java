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
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

@AllArgsConstructor
public class SshRunner {
    private final Path sshExecutable;
    private final String user;
    private final String host;
    private final Path remoteBaseDir;

    public boolean fileExists(String archiveName) {
        var command = String.format("%s %s@test %s -e %s",
            sshExecutable.toAbsolutePath(),
            user,
            host,
            remoteBaseDir.resolve(archiveName));
        var cmdLine = CommandLine.parse(command);
        var executor = DefaultExecutor.builder().get();
        executor.setExitValues(new int[] { 0, 1 });
        try {
            int exitValue = executor.execute(cmdLine);
            return exitValue == 0;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to check existence of " + archiveName + " on " + host, e);
        }
    }

    public List<String> listFiles() throws IOException {
        var command = String.format("%s %s@%s ls -1 %s",
            sshExecutable.toAbsolutePath(),
            user,
            host,
            remoteBaseDir);
        var cmdLine = CommandLine.parse(command);
        var executor = DefaultExecutor.builder().get();
        var outputStream = new ByteArrayOutputStream();
        executor.setStreamHandler(new org.apache.commons.exec.PumpStreamHandler(outputStream));
        int exitValue = executor.execute(cmdLine);
        if (exitValue != 0) {
            throw new RuntimeException("Failed to list files in " + remoteBaseDir + " on " + host + ": exit code " + exitValue);
        }
        return outputStream.toString().lines().toList();
    }
}
