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

import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Iterator;

public class DmfTarArchive implements Archive {
    private final DmfTarRunner dmfTarRunner;

    private final String path;

    @Getter
    private boolean archived;

    public DmfTarArchive(@NonNull DmfTarRunner dmfTarRunner, @NonNull String path, boolean archived) {
        this.dmfTarRunner = dmfTarRunner;
        this.path = path;
        this.archived = archived;
    }

    @Override
    public InputStream readFile(String filePath) throws IOException {
        return dmfTarRunner.readFile(path, filePath);
    }

    @Override
    public void unarchiveTo(Path stagingDir) {
        // TODO: implement
    }

    @Override
    public void archiveFrom(Path stagingDir) {
        dmfTarRunner.tarDirectory(stagingDir, path);
    }

    @Override
    @SneakyThrows
    public boolean fileExists(String filePath) {
        return dmfTarRunner.fileExists(path, "./" + filePath);
    }

    @Override
    public Iterator<Item> listAllItems() {
        return new DmfTarArchiveItemIterator(path, dmfTarRunner);
    }
}
