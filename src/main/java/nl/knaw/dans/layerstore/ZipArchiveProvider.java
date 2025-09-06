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

import lombok.AllArgsConstructor;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

@AllArgsConstructor
public class ZipArchiveProvider implements ArchiveProvider {
    private final Path archiveRoot;

    @Override
    public Archive createArchive(long layerId) {
        return new ZipArchive(archiveRoot.resolve(layerId + ".zip"));
    }

    @Override
    public boolean exists(long layerId) {
        return archiveRoot.resolve(layerId + ".zip").toFile().exists();
    }

    @Override
    public List<Long> listArchivedLayers() throws IOException {
        try (var stream = java.nio.file.Files.list(archiveRoot)) {
            return stream
                .map(Path::getFileName)
                .map(Path::toString)
                .filter(name -> name.endsWith(".zip"))
                .map(name -> name.substring(0, name.length() - ".zip".length()))
                .map(Long::valueOf)
                .toList();
        }
    }
}
