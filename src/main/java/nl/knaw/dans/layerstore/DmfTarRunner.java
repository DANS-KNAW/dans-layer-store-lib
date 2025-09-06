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

import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lib.util.ProcessInputStream;
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

/**
 * A runner for the dmftar command line tool, which creates and reads DMF TAR archives on a remote host via SSH.
 * <p>
 * Security: all parameters provided to the constructor are validated to prevent command injection attacks. The {@link #tarDirectory(Path, String)} method checks that the directory to be archived does
 * not contain any subdirectories prefixed with 'dmftar-cache.', as these may conflict with temporary cache directories created by dmftar itself.
 */
@Slf4j
public class DmfTarRunner extends AbstractRunner {
    private final Path dmfTarExecutable;

    private final String user;
    private final String host;
    private final Path remoteBaseDir;

    /**
     * Creates a new DmfTarRunner.
     *
     * @param dmfTarExecutable path to the dmftar executable
     * @param user             username for the remote host
     * @param host             host name or IP address of the remote host
     * @param remoteBaseDir    base directory on the remote host where archives are stored
     */
    public DmfTarRunner(Path dmfTarExecutable, String user, String host, Path remoteBaseDir) {
        this.dmfTarExecutable = Path.of(checkExecutableForSecurity(dmfTarExecutable));
        this.user = checkUserOrHostNameForSecurity(user);
        this.host = checkUserOrHostNameForSecurity(host);
        this.remoteBaseDir = Path.of(checkRemoteBaseDirForSecurity(remoteBaseDir.toString()));
    }

    /**
     * Create a DMF TAR archive from the contents of the given directory. The directory itself is not included in the archive. The directory must not contain any subdirectories prefixed with
     * 'dmftar-cache.', as these may conflict with temporary cache directories created by dmftar itself. The resulting DMT TAR archive will be created in the remote base directory with the given
     * name.
     *
     * @param directory   the directory to archive
     * @param archiveName the name of the archive to create on the remote host
     */
    public void tarDirectory(Path directory, String archiveName) {
        checkForDmftarCacheDirectories(directory);
        var commandLine = CommandLine.parse(dmfTarExecutable.toAbsolutePath() + " -cf " + getRemotePath(archiveName) + " .");
        var executor = DefaultExecutor.builder()
            .setWorkingDirectory(directory.toAbsolutePath().toFile())
            .get();
        try {
            executor.execute(commandLine);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to create tar archive: " + e.getMessage(), e);
        }
    }

    private void checkForDmftarCacheDirectories(Path directory) {
        var cacheDirs = directory.toFile().listFiles((file) -> file.isDirectory() && file.getName().startsWith("dmftar-cache."));
        if (cacheDirs != null && cacheDirs.length > 0) {
            throw new IllegalArgumentException("Directory to be archived contains directories prefixed with 'dmftar-cache.': " + cacheDirs[0].getAbsolutePath());
        }
    }

    /**
     * Reads a file from a DMF TAR archive on the remote host.
     *
     * @param archiveName the name of the archive to read from
     * @param fileName    the name of the file to read from the archive
     * @return an InputStream for reading the file
     */
    public InputStream readFile(String archiveName, String fileName) throws IOException{
        if (fileName.startsWith("./")) {
            throw new IllegalArgumentException("File name cannot start with './': " + fileName);
        }
        var commandLine = new CommandLine(dmfTarExecutable.toAbsolutePath().toString())
            .addArgument("--options=--to-stdout") // Stream the output to stdout instead of writing it to a file
            .addArgument("--quiet") // Suppress progress messages from dmftar
            .addArgument("--extract")
            .addArgument("--archive=" + getRemotePath(archiveName))
            .addArgument(addPrefix(fileName));
        return ProcessInputStream.start(commandLine);
    }

    /*
     * dmftar wants ./<filename>
     */
    private String addPrefix(String name) {
        return "./" + name;
    }

    /**
     * Lists the files in a DMF TAR archive on the remote host.
     *
     * @param archiveName the name of the archive to list files from
     * @return an Iterator over the file names in the archive
     */
    public Iterator<String> listFiles(String archiveName) {
        var commandLine = new CommandLine(dmfTarExecutable.toAbsolutePath().toString())
            .addArgument("-qtf")
            .addArgument(getRemotePath(archiveName));
        try {
            var inputStream = (InputStream) ProcessInputStream.start(commandLine);
            var reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            return new LineIterator(reader);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to list files in archive: " + e.getMessage(), e);
        }
    }

    /**
     * Checks if a file exists in a DMF TAR archive on the remote host.
     *
     * @param archiveName the name of the archive to check
     * @param fileName    the name of the file to check for
     * @return true if the file exists in the archive, false otherwise
     * @throws IOException if an I/O error occurs
     */
    public boolean fileExists(String archiveName, String fileName) throws IOException {
        var it = listFiles(archiveName);
        while (it.hasNext()) {
            if (it.next().equals(fileName)) {
                return true;
            }
        }
        return false;
    }

    private String getRemotePath(String archiveName) {
        return user + "@" + host + ":" + remoteBaseDir.resolve(archiveName);
    }

}
