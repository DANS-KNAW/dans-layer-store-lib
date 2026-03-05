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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@EnabledIf("nl.knaw.dans.layerstore.TestConditions#dmftarLiveTestConfigured")
public class DmfTarArchiveProviderLiveTest {
    private DmfTarArchiveProvider provider;
    private DmfTarRunner dmfTarRunner;
    private SshRunner sshRunner;
    private Path inputDir = Path.of("src/test/resources/live-test-input-dir");

    @BeforeEach
    public void setUp() throws IOException {
        var p = new LiveTestProperties();
        dmfTarRunner = new DmfTarRunner(p.getDmfTarExecutable(), p.getUser(), p.getHost(), p.getRemoteBaseDir());
        sshRunner = new SshRunner(Path.of("/usr/bin/ssh"), p.getUser(), p.getHost(), p.getRemoteBaseDir());
        provider = new DmfTarArchiveProvider(dmfTarRunner, sshRunner);
        if (p.getInputDir() != null) {
            inputDir = p.getInputDir();
        }

        // Clean and recreate remote base directory
        sshRunner.runCommand("rm -rf " + sshRunner.getRemoteBaseDir());
        sshRunner.runCommand("mkdir -p " + sshRunner.getRemoteBaseDir());
    }

    @Test
    public void validateRoot_should_pass_on_valid_root() throws IOException {
        provider.validateRoot();
    }

    @Test
    public void validateRoot_should_throw_on_illegal_files() throws IOException {
        sshRunner.runCommand("touch " + sshRunner.getRemoteBaseDir().resolve("illegal.txt"));

        assertThatThrownBy(provider::validateRoot)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Archive root")
            .hasMessageContaining("contains illegal files")
            .hasMessageContaining("illegal.txt");
    }

    @Test
    public void validateRoot_should_throw_on_closed_extension() throws IOException {
        sshRunner.runCommand("mkdir " + sshRunner.getRemoteBaseDir().resolve("1234567890123.dmftar.closed"));

        assertThatThrownBy(provider::validateRoot)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Archive root")
            .hasMessageContaining("contains illegal files")
            .hasMessageContaining("1234567890123.dmftar.closed/");
    }

    @Test
    public void exists_should_return_true_when_archive_exists() throws IOException {
        long layerId = System.currentTimeMillis();
        String archiveName = layerId + ".dmftar";
        dmfTarRunner.tarDirectory(inputDir, archiveName);

        assertThat(provider.exists(layerId)).isTrue();
    }

    @Test
    public void exists_should_return_false_when_archive_does_not_exist() {
        assertThat(provider.exists(1234567890123L)).isFalse();
    }

    @Test
    public void listLayerIds_should_list_existing_archives() throws IOException {
        long layerId = System.currentTimeMillis();
        String archiveName = layerId + ".dmftar";
        dmfTarRunner.tarDirectory(inputDir, archiveName);

        var layerIds = provider.listLayerIds();
        assertThat(layerIds).contains(layerId);
    }
}
