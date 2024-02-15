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
import lombok.NonNull;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

@AllArgsConstructor
public class DmfTarArchive implements Archive {
    @NonNull
    private final String user;
    @NonNull
    private final String host;
    @NonNull
    private final Path path;

    @Override
    public InputStream readFile(String filePath) throws IOException {
        return null;
    }

    @Override
    public void unarchiveTo(Path stagingDir) {

    }

    @Override
    public void archiveFrom(Path stagingDir) {

    }

    @Override
    public boolean isArchived() {
        return false;
    }

    @Override
    public boolean fileExists(String filePath) {
        return false;
    }
}
