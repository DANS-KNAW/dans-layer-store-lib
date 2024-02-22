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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.nio.file.Files;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ProcessRunnerTest extends AbstractTestWithTestDir {

    @Test
    @EnabledOnOs({ OS.LINUX, OS.MAC, OS.AIX, OS.SOLARIS, OS.FREEBSD, OS.OPENBSD }) // Systems that should provide the `ls` command
    public void run_should_run_a_process_and_capture_its_output() throws Exception {
        var subdir = Files.createDirectories(testDir.resolve("subdir"));
        Files.createFile(subdir.resolve("file1"));
        Files.createFile(subdir.resolve("file2"));

        var runner = new ProcessRunner("ls", "-1");
        runner.setWorkingDirectory(subdir.toString());
        var result = runner.run();
        assertEquals(0, result.getExitCode());
        var files = result.getStandardOutput().split("\n");
        assertEquals(2, files.length);
        assertThat(files).containsExactlyInAnyOrder("file1", "file2");
    }

    @Test // Whether the `ls` command is available is irrelevant here
    public void run_should_throw_an_exception_when_the_command_or_arguments_contain_forbidden_characters() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> new ProcessRunner("ls", ";", "echo", "evil", ">", "/tmp/somefile.txt").run());
    }
}
