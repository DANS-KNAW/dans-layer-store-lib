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

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class MoratoriumManagerTest {

    @Test
    public void checkOutput_should_start_moratorium_when_trigger_present() {
        var manager = new MoratoriumManager("Connection refused", Duration.ofMinutes(1));

        assertThat(manager.checkOutput("ssh: connect to host x port 22: Connection refused")).isTrue();

        assertThat(manager.isUnderMoratorium()).isTrue();
        assertThatThrownBy(manager::ensureNotUnderMoratorium).isInstanceOf(MoratoriumException.class);
    }

    @Test
    public void checkOutput_should_ignore_other_output() {
        var manager = new MoratoriumManager("Connection refused", Duration.ofMinutes(1));

        assertThat(manager.checkOutput("Permission denied")).isFalse();
        assertThat(manager.checkOutput(null)).isFalse();

        assertThat(manager.isUnderMoratorium()).isFalse();
    }

    @Test
    public void moratorium_should_end() throws Exception {
        var manager = new MoratoriumManager("Connection refused", Duration.ofMillis(50));
        manager.checkOutput("Connection refused");

        Thread.sleep(100);

        assertThat(manager.isUnderMoratorium()).isFalse();
    }

    @Test
    public void disabled_manager_never_starts_moratorium() {
        var manager = MoratoriumManager.disabled();

        assertThat(manager.checkOutput("Connection refused")).isFalse();
        assertThat(manager.isUnderMoratorium()).isFalse();
    }
}
