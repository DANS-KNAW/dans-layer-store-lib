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

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DmfTarArchiveProviderValidateRootTest {

    @Test
    public void should_throw_on_illegal_files() throws IOException {
        var sshRunner = new SshRunner(Path.of("ssh"), "user", "host", Path.of("remote")) {
            @Override
            public List<String> listFiles(String flags) {
                return List.of("illegal.txt");
            }
        };
        var provider = new DmfTarArchiveProvider(null, sshRunner);

        assertThatThrownBy(provider::validateRoot)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Archive root")
            .hasMessageContaining("contains illegal files")
            .hasMessageContaining("illegal.txt");
    }

    @Test
    public void should_throw_on_closed_extension() throws IOException {
        var sshRunner = new SshRunner(Path.of("ssh"), "user", "host", Path.of("remote")) {
            @Override
            public List<String> listFiles(String flags) {
                // ls -F appends / to directories
                return List.of("1234567890123.dmftar.closed/");
            }
        };
        var provider = new DmfTarArchiveProvider(null, sshRunner);

        assertThatThrownBy(provider::validateRoot)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Archive root")
            .hasMessageContaining("contains illegal files")
            .hasMessageContaining("1234567890123.dmftar.closed/");
    }

    @Test
    public void should_pass_on_valid_files() throws IOException {
        var sshRunner = new SshRunner(Path.of("ssh"), "user", "host", Path.of("remote")) {
            @Override
            public List<String> listFiles(String flags) {
                return List.of("1234567890123.dmftar/");
            }
        };
        var provider = new DmfTarArchiveProvider(null, sshRunner);

        provider.validateRoot();
    }
}
