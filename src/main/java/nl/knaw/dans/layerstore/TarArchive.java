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
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.utils.BoundedInputStream;
import org.apache.commons.compress.utils.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

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
    public InputStream readFile(String filePath) throws IOException {
        // No try-with-resources on tarInput, so that we can use it to back the BoundedInputStream
        TarArchiveInputStream tarInput = new TarArchiveInputStream(Files.newInputStream(tarFile));
        try {
            TarArchiveEntry entry;
            while ((entry = tarInput.getNextTarEntry()) != null) {
                if (entry.getName().equals(filePath)) {
                    return new BoundedInputStream(tarInput, entry.getSize()) {

                        @Override
                        @SneakyThrows
                        public void close() {
                            // Close the backing stream.
                            tarInput.close();
                        }
                    };
                }
            }
        }
        catch (Exception e) {
            // Close the backing stream in case of an exception.
            tarInput.close();
            throw e;
        }
        throw new IOException("File not found in tar archive: " + filePath);
    }

    @Override
    @SneakyThrows
    public void unarchiveTo(Path stagingDir) {
        try (TarArchiveInputStream tarInput = new TarArchiveInputStream(Files.newInputStream(tarFile))) {
            TarArchiveEntry entry;
            while ((entry = tarInput.getNextTarEntry()) != null) {
                Path outputPath = stagingDir.resolve(entry.getName());
                if (entry.isDirectory()) {
                    Files.createDirectories(outputPath);
                }
                else {
                    Files.createDirectories(outputPath.getParent());
                    try (OutputStream outputFileStream = Files.newOutputStream(outputPath)) {
                        IOUtils.copy(tarInput, outputFileStream);
                    }
                }
            }
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
        try (TarArchiveInputStream tarInput = new TarArchiveInputStream(Files.newInputStream(tarFile))) {
            TarArchiveEntry entry;
            while ((entry = tarInput.getNextTarEntry()) != null) {
                if (entry.getName().equals(filePath)) {
                    return true;
                }
            }
        }
        return false;
    }
}
