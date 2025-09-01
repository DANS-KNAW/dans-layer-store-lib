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
import nl.knaw.dans.lib.util.ProcessRunner;

import java.nio.file.Path;
import java.util.List;

@AllArgsConstructor
public class SshRunner {
    private final Path sshExecutable;
    private final String user;
    private final String host;
    private final Path remoteBaseDir;

    public boolean fileExists(String archiveName) {
        var runner = new ProcessRunner(sshExecutable.toAbsolutePath().toString(),
            user + "@" + host,
            "test", "-e", remoteBaseDir.resolve(archiveName).toString());
        var result = runner.runToEnd();
        return result.getExitCode() == 0;
    }

    public List<String> listFiles() {
        var runner = new ProcessRunner(sshExecutable.toAbsolutePath().toString(),
            user + "@" + host,
            "ls -1", remoteBaseDir.toString());
        var result = runner.runToEnd();
        if (result.getExitCode() != 0) {
            throw new RuntimeException("Failed to list files in " + remoteBaseDir + " on " + host + ": " + result);
        }
        return result.getStandardOutput().lines().toList();
    }
}
