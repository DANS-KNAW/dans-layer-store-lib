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
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.io.output.ByteArrayOutputStream;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.regex.Pattern;

/**
 * Base class for command runners.
 */
@Slf4j
abstract class AbstractRunner {
    protected record CommandResult(int exitCode, String output) {
    }

    protected final MoratoriumManager moratoriumManager;

    protected AbstractRunner() {
        this(null);
    }

    protected AbstractRunner(MoratoriumManager moratoriumManager) {
        this.moratoriumManager = moratoriumManager == null ? MoratoriumManager.disabled() : moratoriumManager;
    }

    /**
     * Runs the command, capturing stdout and stderr, and returns the result for any exit code. Throws {@link MoratoriumException} without running the command if a moratorium is in effect. If the
     * output of a failed command contains the moratorium trigger, a moratorium is started.
     */
    protected CommandResult run(CommandLine commandLine, File workingDirectory) {
        moratoriumManager.ensureNotUnderMoratorium();
        log.debug("Executing: {}", commandLine);
        var out = new ByteArrayOutputStream();
        var builder = DefaultExecutor.builder();
        if (workingDirectory != null) {
            builder.setWorkingDirectory(workingDirectory);
        }
        var executor = builder.get();
        executor.setExitValues(null); // accept any exit value, the caller decides
        executor.setStreamHandler(new PumpStreamHandler(out));
        try {
            var exitCode = executor.execute(commandLine);
            var output = out.toString(StandardCharsets.UTF_8);
            if (exitCode != 0) {
                moratoriumManager.checkOutputForTrigger(output);
            }
            return new CommandResult(exitCode, output);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to execute " + commandLine + ": " + e.getMessage(), e);
        }
    }

    protected CommandResult runOrThrow(CommandLine commandLine, File workingDirectory) {
        var result = run(commandLine, workingDirectory);
        if (result.exitCode() != 0) {
            throw new RuntimeException("Command failed with exit code " + result.exitCode() + ": " + commandLine + "\n" + result.output());
        }
        return result;
    }

    /**
     * To be called when a streaming command (see ProcessInputStream) failed, so that the moratorium trigger can be detected in its error message.
     */
    protected void checkFailureForMoratoriumTrigger(Exception e) {
        moratoriumManager.checkOutputForTrigger(e.getMessage());
    }

    private static final Pattern USER_HOST_PATTERN = Pattern.compile("^[a-zA-Z0-9._-]+$");
    private static final Pattern COMMAND_INJECTION_PATTERN = Pattern.compile("[\\s;|&`$<>]");

    protected static String checkExecutableForSecurity(Path path) {
        var absPath = path.toAbsolutePath().toString();
        if (COMMAND_INJECTION_PATTERN.matcher(absPath).find()) {
            throw new IllegalArgumentException("Invalid executable path: " + absPath);
        }
        return absPath;
    }

    protected static String checkUserOrHostNameForSecurity(String param) {
        if (!USER_HOST_PATTERN.matcher(param).matches()) {
            throw new IllegalArgumentException("Invalid username or host name: " + param);
        }
        return param;
    }

    protected static String checkRemoteBaseDirForSecurity(String param) {
        if (param.contains("..")) {
            throw new IllegalArgumentException("Invalid remote base directory: " + param);
        }
        return param;
    }
}
