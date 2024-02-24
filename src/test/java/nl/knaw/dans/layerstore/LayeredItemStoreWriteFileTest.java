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

import static org.assertj.core.api.Assertions.assertThat;

public class LayeredItemStoreWriteFileTest extends AbstractLayerDatabaseTest {
    private static class StoreTxtContent extends NoopDatabaseBackedContentManager {
        @Override
        public boolean test(String path) {
            return path.endsWith(".txt");
        }
    }


    @Test
    public void should_write_file_to_staging_dir_when_layer_is_open() throws Exception {
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir));
        var layeredStore = new LayeredItemStore(dao, layerManager);

        var testContent = "Hello world!";
        layeredStore.writeFile("test.txt", new ByteArrayInputStream(testContent.getBytes(StandardCharsets.UTF_8)));

        var topLayer = layerManager.getTopLayer();

        assertThat(stagingDir.resolve(Long.toString(topLayer.getId())).resolve("test.txt")).exists();
        assertThat(stagingDir.resolve(Long.toString(topLayer.getId())).resolve("test.txt")).usingCharset(StandardCharsets.UTF_8).hasContent(testContent);
    }

    @Test
    public void should_write_copy_of_content_to_database_if_filter_applies() throws Exception {
        var layerManager = new LayerManagerImpl(stagingDir, new ZipArchiveProvider(archiveDir));
        var layeredStore = new LayeredItemStore(dao, layerManager, new StoreTxtContent());

        var testContent = "Hello world!";
        layeredStore.writeFile("test.txt", new ByteArrayInputStream(testContent.getBytes(StandardCharsets.UTF_8)));

        // Check that the file content is in the database
        dao.getAllRecords().toList().forEach(itemRecord -> {
            if (itemRecord.getPath().equals("test.txt")) {
                assertThat(itemRecord.getContent()).isEqualTo(testContent.getBytes(StandardCharsets.UTF_8));
            }
        });

    }

}
