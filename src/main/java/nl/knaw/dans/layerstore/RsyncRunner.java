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

import org.apache.commons.exec.CommandLine;

import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;

/**
 * A runner for the rsync command line tool, which uploads files to the Data Archive. The arguments should be such that an interrupted upload can be resumed by running the same command again, for
 * example {@code -av --partial --append}.
 */
public class RsyncRunner extends AbstractRunner {
    private static final Pattern SAFE_SUBDIR = Pattern.compile("[a-zA-Z0-9._-]+$");

    private final Path rsyncExecutable;
    private final List<String> args;
    private final String user;
    private final String host;
    private final Path remoteBaseDir;

    /**
     * Creates a new RsyncRunner.
     *
     * @param rsyncExecutable   path to the rsync executable
     * @param args              arguments to pass to rsync, before the source and destination
     * @param user              username for the remote host
     * @param host              host name or IP address of the remote host
     * @param remoteBaseDir     base directory on the remote host
     * @param moratoriumManager moratorium shared by all connections to the Data Archive; null for none
     */
    public RsyncRunner(Path rsyncExecutable, List<String> args, String user, String host, Path remoteBaseDir, MoratoriumManager moratoriumManager) {
        super(moratoriumManager);
        this.rsyncExecutable = Path.of(checkExecutableForSecurity(rsyncExecutable));
        this.args = List.copyOf(args);
        this.user = checkUserOrHostNameForSecurity(user);
        this.host = checkUserOrHostNameForSecurity(host);
        this.remoteBaseDir = Path.of(checkRemoteBaseDirForSecurity(remoteBaseDir.toString()));
    }

    /**
     * Uploads a file or directory into a subdirectory of the remote base directory, which must exist. Running this again after a failed attempt resumes the upload.
     *
     * @param source       the local file or directory (without trailing slash, so that it is created by name in the destination)
     * @param remoteSubDir the name of the subdirectory of the remote base directory
     * @throws RemoteCommandException if rsync exits with a non-zero exit code
     */
    public void upload(Path source, String remoteSubDir) {
        if (!SAFE_SUBDIR.matcher(remoteSubDir).matches() || remoteSubDir.equals("..")) {
            throw new IllegalArgumentException("Invalid remote subdirectory: " + remoteSubDir);
        }
        var commandLine = new CommandLine(rsyncExecutable.toString());
        args.forEach(commandLine::addArgument);
        commandLine.addArgument(source.toAbsolutePath().toString(), false);
        commandLine.addArgument(user + "@" + host + ":" + remoteBaseDir.resolve(remoteSubDir) + "/", false);
        var result = run(commandLine, null);
        if (result.exitCode() != 0) {
            throw new RemoteCommandException("rsync of " + source + " failed", result.exitCode(), result.output());
        }
    }
}
