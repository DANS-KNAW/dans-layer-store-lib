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
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.io.IOUtils;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ZipArchive implements Archive {
    @NonNull
    private final Path zipFile;

    private boolean archived;

    private static class EntryInputStream extends InputStream implements Closeable {
        private final ZipFile zipFile;
        private final InputStream inputStream;

        public EntryInputStream(ZipFile zipFile, ZipArchiveEntry entry) throws IOException {
            this.zipFile = zipFile;
            this.inputStream = zipFile.getInputStream(entry);
        }

        @Override
        public int read() throws IOException {
            return inputStream.read();
        }

        public boolean exists() throws IOException {
            return inputStream != null;
        }

        @Override
        public void close() throws IOException {
            if (inputStream != null) // no inputStream if the entry did not exist
                inputStream.close();
            zipFile.close();
        }
    }

    public ZipArchive(Path zipFile) {
        this.zipFile = zipFile;
        this.archived = Files.exists(zipFile);
    }

    @Override
    public InputStream readFile(String filePath) throws IOException {
        var zipFile = new ZipFile.Builder().setCharset(UTF_8).setFile(this.zipFile.toFile()).get();
        var entry = zipFile.getEntry(filePath);
        return new EntryInputStream(zipFile, entry);
    }

    @Override
    public void unarchiveTo(Path stagingDir) {
        try (var zipArchiveInputStream = new ZipArchiveInputStream(new FileInputStream(zipFile.toFile()))) {
            var entry = zipArchiveInputStream.getNextEntry();
            while (entry != null) {
                if (entry.isDirectory()) {
                    Files.createDirectories(stagingDir.resolve(entry.getName()));
                }
                else {
                    Path file = stagingDir.resolve(entry.getName());
                    Files.createDirectories(file.getParent());
                    Files.copy(zipArchiveInputStream, file);
                }
                entry = zipArchiveInputStream.getNextEntry();
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Could not unarchive zip file", e);
        }
    }

    @Override
    public void archiveFrom(Path stagingDir) {
        createZipFile(zipFile.toString(), stagingDir.toString());
    }

    // See: https://simplesolution.dev/java-create-zip-file-using-apache-commons-compress/
    @SneakyThrows
    public void createZipFile(String zipFileName, String directoryToZip) {
        var zipFilePath = Paths.get(zipFileName);
        try (var outputStream = Files.newOutputStream(zipFilePath);
            var bufferedOutputStream = new BufferedOutputStream(outputStream);
            var zipArchiveOutputStream = new ZipArchiveOutputStream(bufferedOutputStream)
        ) {
            File[] files = new File(directoryToZip).listFiles();
            if (files != null) {
                for (File file : files) {
                    addFileToZipStream(zipArchiveOutputStream, file, "");
                }
            }
            archived = true;
        }
    }

    private void addFileToZipStream(ZipArchiveOutputStream zipArchiveOutputStream, File fileToZip, String base) throws IOException {
        var entryName = base + fileToZip.getName();
        var zipArchiveEntry = new ZipArchiveEntry(fileToZip, entryName);
        zipArchiveOutputStream.putArchiveEntry(zipArchiveEntry);
        if (fileToZip.isFile()) {
            FileInputStream fileInputStream = null;
            try {
                fileInputStream = new FileInputStream(fileToZip);
                IOUtils.copy(fileInputStream, zipArchiveOutputStream);
                zipArchiveOutputStream.closeArchiveEntry();
            }
            finally {
                IOUtils.closeQuietly(fileInputStream);
            }
        }
        else {
            zipArchiveOutputStream.closeArchiveEntry();
            var files = fileToZip.listFiles();
            if (files != null) {
                for (File file : files) {
                    addFileToZipStream(zipArchiveOutputStream, file, entryName + "/");
                }
            }
        }
    }

    @Override
    public boolean isArchived() {
        return archived;
    }

    @Override
    public boolean fileExists(String filePath) {
        try (var is = (EntryInputStream) readFile(filePath)) {
            return is.exists();
        }
        catch (IOException e) {
            return false;
        }
    }
}
