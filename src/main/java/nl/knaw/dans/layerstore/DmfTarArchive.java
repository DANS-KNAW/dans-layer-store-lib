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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Iterator;

/**
 * Implementation of {@link Archive} that uses the dmftar command line tool to create and read DMF TAR archives.
 */
@Slf4j
class DmfTarArchive implements Archive {
    private final DmfTarRunner dmfTarRunner;

    private final String path;

    @Getter
    private boolean archived;

    private final DmfTarArchiveOptions options;

    public DmfTarArchive(@NonNull DmfTarRunner dmfTarRunner, @NonNull DmfTarArchiveOptions options, @NonNull String path, boolean archived) {
        this.dmfTarRunner = dmfTarRunner;
        this.options = options;
        this.path = path;
        this.archived = archived;
    }

    @Override
    public InputStream readFile(String filePath) throws IOException {
        return dmfTarRunner.readFile(path, filePath);
    }

    @Override
    public void unarchiveTo(Path stagingDir) {
        dmfTarRunner.untarArchive(path, stagingDir);
    }

    @Override
    public void archiveFrom(Path stagingDir) {
        var localArchive = options.localPackagingDir().resolve(path);
        var marker = options.localPackagingDir().resolve(path + ".complete");
        var failures = 0;
        while (true) {
            try {
                packageLocally(stagingDir, localArchive, marker);
                dmfTarRunner.uploadArchive(localArchive, path);
                if (options.verify()) {
                    dmfTarRunner.verifyUploadedArchive(path, options.invalidOn());
                }
                install();
                deleteLocalPackage(localArchive, marker);
                return;
            }
            catch (MoratoriumException e) {
                log.warn("{}; waiting", e.getMessage());
                sleep(e.getRemaining());
            }
            catch (InvalidArchiveException e) {
                log.error("Uploaded archive {} is invalid, discarding it", path);
                discardQuietly(localArchive, marker);
                throw e;
            }
            catch (RuntimeException e) {
                if (++failures > options.maxRetries()) {
                    throw e;
                }
                log.warn("Archiving {} failed (attempt {} of {}), retrying in {}", path, failures, options.maxRetries() + 1, options.retryInterval(), e);
                sleep(options.retryInterval());
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private void packageLocally(Path stagingDir, Path localArchive, Path marker) throws IOException {
        var fingerprint = fingerprint(stagingDir);
        if (Files.exists(marker) && Files.exists(localArchive) && Files.readString(marker).equals(fingerprint)) {
            log.debug("Reusing complete local package {}", localArchive);
            return;
        }
        deleteLocalPackage(localArchive, marker);
        Files.createDirectories(options.localPackagingDir());
        dmfTarRunner.packageDirectory(stagingDir, localArchive);
        Files.writeString(marker, fingerprint);
    }

    /*
     * Guards against reusing a local package that no longer matches the staging directory.
     */
    private String fingerprint(Path stagingDir) throws IOException {
        try (var files = Files.walk(stagingDir)) {
            var stats = files.filter(Files::isRegularFile).mapToLong(f -> {
                try {
                    return Files.size(f) * 31 + Files.getLastModifiedTime(f).toMillis();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }).summaryStatistics();
            return stats.getCount() + ":" + stats.getSum();
        }
    }

    private void install() {
        String backupPath = null;
        if (archived) {
            backupPath = path + ".bak";
            dmfTarRunner.renameRemoteFile(path, backupPath);
        }
        try {
            dmfTarRunner.installUploadedArchive(path);
        }
        catch (RuntimeException e) {
            if (backupPath != null) {
                dmfTarRunner.renameRemoteFile(backupPath, path);
            }
            throw e;
        }
        archived = true;
        if (backupPath != null) {
            try {
                dmfTarRunner.deleteRemoteFile(backupPath);
            }
            catch (Exception e) {
                log.warn("Could not delete backup {} of replaced archive", backupPath, e);
            }
        }
    }

    private void discardQuietly(Path localArchive, Path marker) {
        try {
            deleteLocalPackage(localArchive, marker);
            dmfTarRunner.deleteUploadedArchive(path);
        }
        catch (Exception e) {
            log.warn("Could not clean up after invalid archive {}", path, e);
        }
    }

    private void deleteLocalPackage(Path localArchive, Path marker) throws IOException {
        Files.deleteIfExists(marker);
        if (Files.exists(localArchive)) {
            // dmftar makes everything read-only, even the directories
            try (var paths = Files.walk(localArchive)) {
                for (var dir : paths.filter(Files::isDirectory).toList()) {
                    dir.toFile().setWritable(true);
                }
            }
            FileUtils.deleteDirectory(localArchive.toFile());
        }
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting to retry archiving", e);
        }
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
