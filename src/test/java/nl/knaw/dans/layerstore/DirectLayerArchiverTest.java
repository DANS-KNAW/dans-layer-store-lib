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

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DirectLayerArchiverTest {

    @Test
    public void should_execute_archive_action_synchronously() {
        var archiver = new DirectLayerArchiver();
        var actionExecuted = new AtomicBoolean(false);

        archiver.archive(1L, false, () -> actionExecuted.set(true));

        assertThat(actionExecuted.get()).isTrue();
    }

    @Test
    public void should_propagate_runtime_exception_thrown_by_archive_action() {
        var archiver = new DirectLayerArchiver();

        assertThatThrownBy(() -> archiver.archive(1L, false, () -> {
            throw new RuntimeException("test error");
        }))
            .isInstanceOf(RuntimeException.class)
            .hasMessage("test error");
    }
}
