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
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * This class is a live test for the DmfTar class. It requires a file live-test.properties in the root of the project with the following properties:
 * <pre>
 * dmftar.executable = /path/to/dmf-tar
 * dmftar.remote-base-dir = /path/to/remote-base-dir
 * dmftar.user = user
 * dmftar.host = host
 * input-dir = /path/to/input-dir # Optional, will default to src/test/resources/live-test-input-dir
 * </pre>
 * Before running the test, you must:
 * <ul>
 *     <li>the remote base directory exists</li>
 *     <li>your public key is in the authorized_keys file of the user on the host</li>
 * </ul>
 */
@Slf4j
public class DmfTarRunnerLiveTest {
    private DmfTarRunner dmfTarRunner;

    private Path inputDir = Path.of("src/test/resources/live-test-input-dir");

    @BeforeEach
    public void setUp() {
        var p = new LiveTestProperties();
        dmfTarRunner = new DmfTarRunner(p.getDmfTarExecutable(), p.getUser(), p.getHost(), p.getRemoteBaseDir());
        if (p.getInputDir() != null) {
            inputDir = p.getInputDir();
        }
    }

    @Test
    @EnabledIf("nl.knaw.dans.layerstore.TestConditions#dmftarLiveTestConfigured")
    public void roundTrip() throws Exception {
        var tarFile = System.currentTimeMillis() + ".dmftar";
        log.debug("tarFile: {}", tarFile);
        dmfTarRunner.tarDirectory(inputDir, tarFile);
        var actual = IOUtils.toString(dmfTarRunner.readFile(tarFile, "./text/loro.txt"), StandardCharsets.UTF_8);
        var expected = FileUtils.readFileToString(new File("src/test/resources/live-test-input-dir/text/loro.txt"), StandardCharsets.UTF_8);
        assertThat(actual).isEqualTo(expected);
    }
}
