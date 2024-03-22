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

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class LayerWriteFileTest extends AbstractTestWithTestDir {
    @Test
    public void should_write_file_to_staging_dir_when_layer_is_open() throws Exception {
        var layer = new LayerImpl(1, stagingDir, new ZipArchive(testDir.resolve("test.zip")));

        // Write a file to the layer
        var testContent = "Hello world!";
        layer.writeFile("test.txt", new ByteArrayInputStream(testContent.getBytes(StandardCharsets.UTF_8)));

        // Verify that the file is written to the staging dir and has
        assertThat(stagingDir.resolve("test.txt")).exists();
        assertThat(stagingDir.resolve("test.txt")).usingCharset(StandardCharsets.UTF_8).hasContent(testContent);
    }

    @Test
    public void should_throw_IllegalStateException_when_layer_is_closed() {
        var layer = new LayerImpl(1, stagingDir, new ZipArchive(testDir.resolve("test.zip")));
        layer.close();

        assertThatThrownBy(() -> layer.writeFile("whatever.txt", new ByteArrayInputStream("whatever".getBytes(StandardCharsets.UTF_8)))).
            isInstanceOf(IllegalStateException.class)
            .hasMessage("Layer is closed, but must be open for this operation");
    }
}
