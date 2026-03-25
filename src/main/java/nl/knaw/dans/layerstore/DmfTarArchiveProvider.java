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

import java.io.IOException;
import java.util.List;

/**
 * An {@link ArchiveProvider} that uses the dmftar command line tool to create and read DMF TAR archives.
 */
public class DmfTarArchiveProvider implements ArchiveProvider {
    private final DmfTarRunner dmfTarRunner;
    private final SshRunner sshRunner;

    public DmfTarArchiveProvider(DmfTarRunner dmfTarRunner, SshRunner sshRunner) {
        this.dmfTarRunner = dmfTarRunner;
        this.sshRunner = sshRunner;
    }

    @Override
    public Archive createArchive(long layerId) {
        return new DmfTarArchive(dmfTarRunner, layerId + ".dmftar", exists(layerId));
    }

    @Override
    public boolean exists(long layerId) {
        return sshRunner.fileExists(layerId + ".dmftar");
    }

    @Override
    public List<Long> listLayerIds() throws IOException {
        return sshRunner.listFiles().stream()
            .filter(name -> name.endsWith(".dmftar"))
            .map(name -> name.substring(0, name.length() - ".dmftar".length()))
            .map(Long::valueOf)
            .toList();
    }

    @Override
    public void validateRoot() throws IOException {
        var illegalFiles = sshRunner.listFiles("-F").stream()
            .filter(name -> {
                // ls -F appends / to directories. We're looking for things that aren't directories or don't match our pattern.
                if (!name.endsWith("/")) {
                    return true;
                }
                var dirName = name.substring(0, name.length() - 1);
                return !dirName.endsWith(".dmftar") ||
                    !dirName.substring(0, dirName.length() - ".dmftar".length()).matches("^\\d{13,}$");
            })
            .toList();

        if (!illegalFiles.isEmpty()) {
            throw new IllegalStateException(String.format("Archive root '%s' on '%s' contains illegal files: %s", sshRunner.getRemoteBaseDir(), sshRunner.getHost(), String.join(", ", illegalFiles)));
        }
    }

}
