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
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class ConsistencyCheckingAsyncLayerArchiverTest {

    @Test
    public void should_check_consistency_and_execute_archive_action() throws Exception {
        var consistencyChecker = mock(LayerConsistencyChecker.class);
        var executor = mock(Executor.class);
        var archiver = new ConsistencyCheckingAsyncLayerArchiver(consistencyChecker, executor);

        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(executor).execute(Mockito.any(Runnable.class));

        var actionExecuted = new AtomicBoolean(false);
        archiver.archive(42L, false, () -> actionExecuted.set(true));

        verify(consistencyChecker).check(42L);
        assertThat(actionExecuted.get()).isTrue();
    }

    @Test
    public void should_use_default_executor_when_null_passed() throws Exception {
        var consistencyChecker = mock(LayerConsistencyChecker.class);
        var archiver = new ConsistencyCheckingAsyncLayerArchiver(consistencyChecker, null);

        var latch = new CountDownLatch(1);
        var actionExecuted = new AtomicBoolean(false);

        archiver.archive(42L, false, () -> {
            actionExecuted.set(true);
            latch.countDown();
        });

        var completed = latch.await(5, TimeUnit.SECONDS);
        assertThat(completed).isTrue();
        assertThat(actionExecuted.get()).isTrue();
        verify(consistencyChecker).check(42L);
    }

    @Test
    public void should_not_run_archive_action_if_consistency_check_throws_io_exception() throws Exception {
        var consistencyChecker = mock(LayerConsistencyChecker.class);
        doThrow(new IOException("Disk error")).when(consistencyChecker).check(42L);

        var executor = mock(Executor.class);
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            try {
                runnable.run();
            }
            catch (RuntimeException e) {
                assertThat(e).hasCauseInstanceOf(IOException.class);
                assertThat(e.getCause()).hasMessage("Disk error");
            }
            return null;
        }).when(executor).execute(Mockito.any(Runnable.class));

        var archiver = new ConsistencyCheckingAsyncLayerArchiver(consistencyChecker, executor);
        var archiveAction = mock(Runnable.class);

        archiver.archive(42L, false, archiveAction);

        verify(consistencyChecker).check(42L);
        verify(archiveAction, never()).run();
    }

    @Test
    public void should_not_run_archive_action_if_consistency_check_throws_items_mismatch_exception() throws Exception {
        var consistencyChecker = mock(LayerConsistencyChecker.class);
        var mismatchException = new ItemsMismatchException(List.of(), List.of());
        doThrow(mismatchException).when(consistencyChecker).check(42L);

        var executor = mock(Executor.class);
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            try {
                runnable.run();
            }
            catch (RuntimeException e) {
                assertThat(e).hasCause(mismatchException);
            }
            return null;
        }).when(executor).execute(Mockito.any(Runnable.class));

        var archiver = new ConsistencyCheckingAsyncLayerArchiver(consistencyChecker, executor);
        var archiveAction = mock(Runnable.class);

        archiver.archive(42L, false, archiveAction);

        verify(consistencyChecker).check(42L);
        verify(archiveAction, never()).run();
    }
}
