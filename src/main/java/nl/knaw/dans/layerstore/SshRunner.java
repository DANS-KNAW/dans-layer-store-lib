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

import nl.knaw.dans.lib.util.ProcessInputStream;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

/**
 * A runner for the ssh command line tool, which executes commands on a remote host via SSH.
 */
public class SshRunner extends AbstractRunner {
    private final Path sshExecutable;
    private final String user;
    private final String host;
    private final Path remoteBaseDir;
    private final int connectionTimeout; // seconds

    /**
     * Creates a new SshRunner.
     *
     * @param sshExecutable path to the ssh executable
     * @param user username for the remote host
     * @param host host name or IP address of the remote host
     * @param remoteBaseDir base directory on the remote host where archives are stored
     */
    public SshRunner(Path sshExecutable, String user, String host, Path remoteBaseDir) {
        this(sshExecutable, user, host, remoteBaseDir, 10);
    }

    /**
     * Creates a new SshRunner.
     *
     * @param sshExecutable path to the ssh executable
     * @param user username for the remote host
     * @param host host name or IP address of the remote host
     * @param remoteBaseDir base directory on the remote host where archives are stored
     * @param connectionTimeout connection timeout in seconds
     */
    public SshRunner(Path sshExecutable, String user, String host, Path remoteBaseDir, int connectionTimeout) {
        this.sshExecutable = Path.of(checkExecutableForSecurity(sshExecutable));
        this.user = checkUserOrHostNameForSecurity(user);
        this.host = checkUserOrHostNameForSecurity(host);
        this.remoteBaseDir = Path.of(checkRemoteBaseDirForSecurity(remoteBaseDir.toString()));
        this.connectionTimeout = connectionTimeout;
    }

    /**
     * Checks if the given archive name exists on the remote host.
     *
     * @param archiveName the name of the archive to check
     * @return true if the archive exists, false otherwise
     */
    public boolean fileExists(String archiveName) {
        var cmdLine = new CommandLine(sshExecutable.toAbsolutePath().toString())
            .addArgument("-o")
            .addArgument("BatchMode=yes")
            .addArgument("-o")
            .addArgument("ConnectTimeout=" + connectionTimeout)
            .addArgument(user + "@" + host)
            .addArgument("/usr/bin/test -e '" + remoteBaseDir.resolve(archiveName) + "'", false);
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

    /**
     * Lists the files in the remote base directory.
     *
     * @return a list of file names in the remote base directory
     */
    public List<String> listFiles() {
        try {
            var cmdLine = new CommandLine(sshExecutable.toAbsolutePath().toString())
                .addArgument("-o")
                .addArgument("BatchMode=yes")
                .addArgument("-o")
                .addArgument("ConnectTimeout=" + connectionTimeout)
                .addArgument(user + "@" + host)
                .addArgument("ls -1", false)
                .addArgument(remoteBaseDir.toString());
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
