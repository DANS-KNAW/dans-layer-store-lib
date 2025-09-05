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

import org.apache.commons.collections4.IteratorUtils;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;

public class DmfTarArchiveItemIteratorTest {
    private DmfTarRunner dmfTarRunnerMock = Mockito.mock(DmfTarRunner.class);

    @Test
    public void should_strip_everything_before_dot_slash_from_paths_and_remove_dmftar_cache_entries() {
        Mockito.when(dmfTarRunnerMock.listFiles(any())).thenReturn(
            Arrays.stream("""
                drwxrwxr-x janm/janm         0 2025-09-05 14:12 ./
                drwxrwxr-x janm/janm         0 2025-07-14 11:25 ./text/
                -rw-rw-r-- janm/janm      4777 2025-07-14 11:25 ./text/loro.txt
                -rw-rw-r-- janm/janm   3175083 2025-07-14 11:25 ./Tarantula_Nebula_-_Hubble.jpg
                drwxrwxr-x janm/janm         0 2025-09-05 14:12 ./dmftar-cache.28581/
                drwxrwxr-x janm/janm         0 2025-09-05 14:12 ./dmftar-cache.28581/1757074305443.dmftar/
                drwxr-x--- janm/janm         0 2025-09-05 14:12 ./dmftar-cache.28581/1757074305443.dmftar/0000/
                -rw-rw-r-- janm/janm     52488 2025-07-14 11:25 ./loro.jpeg
                -rw-rw-r-- janm/janm    846800 2025-07-14 11:25 ./space-galaxy.jpg
                -rw-rw-r-- janm/janm    516658 2025-07-14 11:25 ./space-galaxy-1401467040F0s.jpg
                """.split("\n")).iterator());

        var list = IteratorUtils.toList(new DmfTarArchiveItemIterator("some-archive.dmftar", dmfTarRunnerMock));
        assertThat(list).asList()
            .containsExactlyInAnyOrder(
                new Item("", Item.Type.Directory),
                new Item("text", Item.Type.Directory),
                new Item("text/loro.txt", Item.Type.File),
                new Item("Tarantula_Nebula_-_Hubble.jpg", Item.Type.File),
                new Item("loro.jpeg", Item.Type.File),
                new Item("space-galaxy.jpg", Item.Type.File),
                new Item("space-galaxy-1401467040F0s.jpg", Item.Type.File)
            );
    }

}
