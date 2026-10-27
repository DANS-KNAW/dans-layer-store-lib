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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class ConsistencyCheckingAsyncLayerArchiverTest {
    @Test
    void should_schedule_check_and_archive_action_on_executor() throws Exception {
        var checker = mock(LayerConsistencyChecker.class);
        var scheduledTask = new AtomicReference<Runnable>();
        var actionExecuted = new AtomicBoolean();
        var archiver = new ConsistencyCheckingAsyncLayerArchiver(checker, scheduledTask::set);

        archiver.archive(42L, () -> actionExecuted.set(true));

        assertThat(actionExecuted).isFalse();
        assertThat(scheduledTask.get()).isNotNull();
        verify(checker, never()).check(42L);
        scheduledTask.get().run();

        verify(checker).check(42L);
        assertThat(actionExecuted).isTrue();
    }

    @Test
    void should_check_layer_and_run_archive_action() throws Exception {
        var checker = mock(LayerConsistencyChecker.class);
        var actionExecuted = new AtomicBoolean();
        var archiver = new ConsistencyCheckingAsyncLayerArchiver(checker, Runnable::run);

        archiver.archive(42L, () -> actionExecuted.set(true));

        verify(checker).check(42L);
        assertThat(actionExecuted).isTrue();
    }

    @Test
    void should_not_run_archive_action_when_consistency_check_fails() throws Exception {
        var checker = mock(LayerConsistencyChecker.class);
        var failure = new IOException("check failed");
        doThrow(failure).when(checker).check(42L);
        var action = mock(Runnable.class);
        var archiver = new ConsistencyCheckingAsyncLayerArchiver(checker, Runnable::run);

        assertThatThrownBy(() -> archiver.archive(42L, action))
            .isInstanceOf(RuntimeException.class)
            .hasCause(failure);

        verify(action, never()).run();
    }
}
