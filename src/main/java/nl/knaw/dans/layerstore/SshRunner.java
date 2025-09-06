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
import nl.knaw.dans.lib.util.ProcessInputStream;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

@AllArgsConstructor
public class SshRunner extends AbstractRunner {
    private final Path sshExecutable;
    private final String user;
    private final String host;
    private final Path remoteBaseDir;

    public boolean fileExists(String archiveName) {
        var command = String.format("%s %s@test %s -e %s",
            sshExecutable.toAbsolutePath(),
            checkUserOrHostNameForSecurity(user),
            checkUserOrHostNameForSecurity(host),
            checkRemoteBaseDirForSecurity(remoteBaseDir.resolve(archiveName).toString()));
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

    public List<String> listFiles() {
        try {
            var command = String.format("%s %s@%s ls -1 %s",
                sshExecutable.toAbsolutePath(),
                checkUserOrHostNameForSecurity(user),
                checkUserOrHostNameForSecurity(host),
                checkRemoteBaseDirForSecurity(remoteBaseDir.toString()));
            var cmdLine = CommandLine.parse(command);

            try (var in = ProcessInputStream.start(cmdLine);
                var reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                return reader.lines().toList();
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to list files in " + remoteBaseDir + " on " + host + ": " + e.getMessage(), e);
        }
    }
}
