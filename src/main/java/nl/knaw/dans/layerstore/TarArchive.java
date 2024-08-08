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

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarFile;
import org.apache.commons.io.IOUtils;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.text.MessageFormat.format;

@Slf4j
public class TarArchive implements Archive {
    @NonNull
    private final Path tarFile;

    private boolean archived;

    public TarArchive(@NonNull Path tarFile) {
        this.tarFile = tarFile;
        // If the file exists, it is assumed to be a valid tar archive
        this.archived = Files.exists(tarFile);
    }

    @Override
    @SuppressWarnings("resource") // closed by overridden method of returned stream
    public InputStream readFile(String filePath) throws IOException {
        var tar = new TarFile(tarFile.toFile());
        var entry = tar.getEntries().stream()
            .filter(e -> e.getName().equals(filePath))
            .findFirst().orElseThrow(() -> new IOException(format("{0} not found in {1}", filePath, tarFile)));
        return new FilterInputStream(tar.getInputStream(entry)) {

            @Override
            @SneakyThrows
            public void close() {
                // Close the backing stream.
                tar.close();
            }
        };
    }

    @Override
    public void unarchiveTo(Path stagingDir) {
        try (var tar = new TarFile(tarFile.toFile())) {
            tar.getEntries().forEach(entry -> {
                try {
                    if (entry.isDirectory()) {
                        Files.createDirectories(stagingDir.resolve(entry.getName()));
                    }
                    else {
                        Path file = stagingDir.resolve(entry.getName());
                        Files.createDirectories(file.getParent());
                        IOUtils.copy(tar.getInputStream(entry), Files.newOutputStream(file));
                    }
                }
                catch (IOException e) {
                    throw new RuntimeException("Could not unarchive " + tarFile.toFile(), e);
                }
            });
        }
        catch (IOException e) {
            throw new RuntimeException("Could not unarchive " + tarFile.toFile(), e);
        }
    }

    @Override
    @SneakyThrows
    public void archiveFrom(Path stagingDir) {
        try (TarArchiveOutputStream tarOutput = new TarArchiveOutputStream(Files.newOutputStream(tarFile))) {
            tarOutput.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
            try (var files = Files.walk(stagingDir)) {
                files.filter(path -> !Files.isDirectory(path))
                    .forEach(path -> {
                        TarArchiveEntry entry = new TarArchiveEntry(stagingDir.relativize(path).toString());
                        entry.setSize(path.toFile().length());
                        try {
                            tarOutput.putArchiveEntry(entry);
                            log.debug("Adding file {} to tar archive", path);
                            Files.copy(path, tarOutput);
                            log.debug("Closing entry for file");
                            tarOutput.closeArchiveEntry();
                        }
                        catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
            }
            archived = true;
        }
    }

    @Override
    public boolean isArchived() {
        return archived;
    }

    @Override
    @SneakyThrows
    public boolean fileExists(String filePath) {
        try (var tar = new TarFile(tarFile.toFile())) {
            return tar.getEntries().stream().anyMatch(e ->
                e.getName().equals(filePath)
            );
        }
        catch (IOException e) {
            return false;
        }
    }
}
