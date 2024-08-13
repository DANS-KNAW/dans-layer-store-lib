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

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
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
                super.close();
                // Close the backing stream.
                tar.close();
            }
        };
    }

    @Override
    public void unarchiveTo(Path stagingDir) {
        try (var tar = new TarFile(tarFile.toFile())) {
            var entries = tar.getEntries();
            for (var entry : entries) {
                // prevent extracting anything in case of a Zip Slip
                var filePath = stagingDir.resolve(entry.getName());
                if (!filePath.normalize().startsWith(stagingDir)) {
                    throw new IOException(format("Detected Zip Slip: {0} in {1}", entry.getName(), tarFile));
                }
            }
            for (var entry : entries) {
                var filePath = stagingDir.resolve(entry.getName());
                if (filePath.normalize().startsWith(stagingDir)) { // keep CodeQL happy
                    if (entry.isDirectory()) {
                        Files.createDirectories(filePath);
                    }
                    else {
                        Files.createDirectories(filePath.getParent());
                        IOUtils.copy(tar.getInputStream(entry), Files.newOutputStream(filePath));
                    }
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Could not unarchive " + tarFile.toFile(), e);
        }
    }

    @Override
    @SneakyThrows
    public void archiveFrom(Path stagingDir) {
        try (var outputStream = Files.newOutputStream(tarFile);
            var bufferedOutputStream = new BufferedOutputStream(outputStream);
            var tarOutput = new TarArchiveOutputStream(bufferedOutputStream);
            var files = Files.walk(stagingDir)
        ) {
            tarOutput.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
            for (var fileToArchive : files.toList()) {
                if (!fileToArchive.equals(stagingDir)) {
                    var entry = new TarArchiveEntry(fileToArchive, stagingDir.relativize(fileToArchive).toString());
                    var regularFile = Files.isRegularFile(fileToArchive);
                    if (regularFile) {
                        entry.setSize(fileToArchive.toFile().length());
                    }
                    tarOutput.putArchiveEntry(entry);
                    if (regularFile) {
                        try (var fileInputStream = new FileInputStream(fileToArchive.toFile())) {
                            IOUtils.copy(fileInputStream, tarOutput);
                        }
                    }
                    tarOutput.closeArchiveEntry();
                }
            }
            archived = true;
        }
    }

    @Override
    public boolean isArchived() {
        return archived;
    }

    @Override
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
