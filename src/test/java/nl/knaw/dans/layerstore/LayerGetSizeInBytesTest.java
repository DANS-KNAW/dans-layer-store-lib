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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.io.IOUtils.toInputStream;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class LayerGetSizeInBytesTest extends AbstractTestWithTestDir {

    @Test
    public void should_add_up_file_sizes() throws Exception {
        var layer = new LayerImpl(1, stagingDir, new ZipArchive(archiveDir.resolve("test.zip")));
        layer.writeFile("test.txt", toInputStream("Hello world!", UTF_8));
        layer.createDirectory("path/to");
        layer.writeFile("path/to/other.txt", toInputStream("Whatever", UTF_8));

        assertThat(layer.getSizeInBytes()).isEqualTo(20L);
    }

    @Test
    public void should_throw_IllegalStateException_when_layer_is_closed() {
        var layer = new LayerImpl(1, stagingDir, new ZipArchive(archiveDir.resolve("test.zip")));
        layer.close();

        assertThatThrownBy(layer::getSizeInBytes).
            isInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Layer is not open");
    }
}
