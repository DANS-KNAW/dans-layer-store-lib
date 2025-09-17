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
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.io.IOUtils;

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.stream.Stream;

import static java.text.MessageFormat.format;

/**
 * Implementation of {@link Archive} for ZIP archives.
 */
public class ZipArchive implements Archive {
    @NonNull
    private final Path zipFile;

    private boolean archived;

    public ZipArchive(Path zipFile) {
        this.zipFile = zipFile;
        this.archived = Files.exists(zipFile);
    }

    @Override
    public InputStream readFile(String filePath) throws IOException {
        var zip = ZipFile.builder()
            .setFile(this.zipFile.toFile())
            .get();
        var entry = Collections.list(zip.getEntries()).stream()
            .filter(e -> e.getName().equals(filePath))
            .findFirst().orElseThrow(() -> new IOException(format("{0} not found in {1}", filePath, zipFile.toFile())));
        return new FilterInputStream(zip.getInputStream(entry)) {

            @Override
            @SneakyThrows
            public void close() {
                super.close();
                // Close the backing stream.
                zip.close();
            }
        };
    }

    @Override
    public void unarchiveTo(Path stagingDir) {
        try (var zip = ZipFile.builder().setFile(this.zipFile.toFile()).get()) {
            var entries = Collections.list(zip.getEntries());
            for (var entry : entries) {
                // prevent extracting anything in case of a Zip Slip
                var filePath = stagingDir.resolve(entry.getName());
                if (!filePath.normalize().startsWith(stagingDir)) {
                    throw new IOException(format("Detected Zip Slip: {0} in {1}", entry.getName(), zipFile));
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
                        IOUtils.copy(zip.getInputStream(entry), Files.newOutputStream(filePath));
                    }
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Could not unarchive " + zipFile.toFile(), e);
        }
    }

    @Override
    @SneakyThrows
    public void archiveFrom(Path stagingDir) {
        Stream<Path> emptyFileStream = Stream.empty();
        try (var outputStream = Files.newOutputStream(zipFile);
            var bufferedOutputStream = new BufferedOutputStream(outputStream);
            var zipOutput = new ZipArchiveOutputStream(bufferedOutputStream);
            var files = stagingDir.toFile().exists()
                ? Files.walk(stagingDir)
                : emptyFileStream // supports LayerManager.newTopLayer() in case of an empty staging directory
        ) {
            for (var fileToArchive : files.toList()) {
                if (!fileToArchive.equals(stagingDir)) {
                    var entry = new ZipArchiveEntry(fileToArchive, stagingDir.relativize(fileToArchive).toString());
                    var regularFile = Files.isRegularFile(fileToArchive);
                    if (regularFile) {
                        entry.setSize(fileToArchive.toFile().length());
                    }
                    zipOutput.putArchiveEntry(entry);
                    if (regularFile) {
                        try (var fileInputStream = new FileInputStream(fileToArchive.toFile())) {
                            IOUtils.copy(fileInputStream, zipOutput);
                        }
                    }
                    zipOutput.closeArchiveEntry();
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
    @SneakyThrows
    public boolean fileExists(String filePath) {

        try (var zip = ZipFile.builder().setFile(this.zipFile.toFile()).get()) {
            return Collections.list(zip.getEntries()).stream().anyMatch(e ->
                e.getName().equals(filePath)
            );
        }
        catch (NoSuchFileException | FileNotFoundException e) {
            return false;
        }
    }

    @Override
    public Iterator<Item> listAllItems() throws IOException {
        return new ZipArchiveItemIterator(zipFile);
    }
}
