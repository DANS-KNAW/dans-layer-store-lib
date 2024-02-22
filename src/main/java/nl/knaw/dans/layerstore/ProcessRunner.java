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

import lombok.Value;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

// TODO: MOVE TO dans-java-utils
/**
 * Provides a way to run a process and capture its output.
 */
public class ProcessRunner {
    @Value
    public static class Result {
        int exitCode;
        String standardOutput;
        String errorOutput;
    }

    private static final String[] FORBIDDEN_CHARS = new String[] { ";", "&", "|", "<", ">", "`", "$", "(", ")", "{", "}", "[", "]", "\\", "'", "\"", " ", "\t", "\n", "\r", "\f", "\b",
        "\0" };
    private final ProcessBuilder processBuilder = new ProcessBuilder();

    public ProcessRunner(String... commandAndArgs) {
        List<String> forbiddenFound = Arrays.stream(commandAndArgs)
            .flatMap(arg -> Arrays.stream(FORBIDDEN_CHARS).filter(arg::contains))
            .collect(Collectors.toList());

        if (!forbiddenFound.isEmpty()) {
            throw new IllegalArgumentException("Command or arguments contain forbidden characters: " + String.join(", ", forbiddenFound));
        }
        processBuilder.command(commandAndArgs);
    }

    public void setWorkingDirectory(String directory) {
        processBuilder.directory(new java.io.File(directory));
    }

    public Result run() {
        try {
            Process process = processBuilder.start();
            String standardOutput = new String(process.getInputStream().readAllBytes());
            String errorOutput = new String(process.getErrorStream().readAllBytes());
            int exitCode = process.waitFor();
            return new Result(exitCode, standardOutput, errorOutput);
        }
        catch (Exception e) {
            throw new RuntimeException("Error running process", e);
        }
    }
}
