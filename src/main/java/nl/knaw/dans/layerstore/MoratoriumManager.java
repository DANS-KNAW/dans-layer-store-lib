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

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;

/**
 * Temporarily suspends all connections to the Data Archive when the output of a failed command contains a trigger text, to prevent the hammer protection of the Data Archive from locking us out
 * indefinitely. One instance should be shared by all runners that connect to the same Data Archive.
 */
@Slf4j
public class MoratoriumManager {
    private final String triggerOn;
    private final Duration duration;
    private Instant moratoriumUntil = Instant.MIN;

    /**
     * Creates a manager.
     *
     * @param triggerOn text that, when found in command output, starts a moratorium; if null, no moratorium is ever started
     * @param duration  length of the moratorium
     */
    public MoratoriumManager(String triggerOn, Duration duration) {
        this.triggerOn = triggerOn;
        this.duration = duration;
    }

    /**
     * @return a manager that never starts a moratorium
     */
    public static MoratoriumManager disabled() {
        return new MoratoriumManager(null, Duration.ZERO);
    }

    /**
     * Starts (or extends) a moratorium if the output contains the trigger text.
     *
     * @param output output of a failed command
     * @return true if a moratorium was triggered
     */
    public synchronized boolean checkOutputForTrigger(String output) {
        if (triggerOn == null || output == null || !output.contains(triggerOn)) {
            return false;
        }
        var newUntil = Instant.now().plus(duration);
        if (newUntil.isAfter(moratoriumUntil)) {
            moratoriumUntil = newUntil;
        }
        log.warn("Moratorium on Data Archive connections set until {}", moratoriumUntil);
        return true;
    }

    public synchronized Duration remaining() {
        var now = Instant.now();
        return now.isBefore(moratoriumUntil) ? Duration.between(now, moratoriumUntil) : Duration.ZERO;
    }

    public boolean isUnderMoratorium() {
        return !remaining().isZero();
    }

    /**
     * @throws MoratoriumException if a moratorium is in effect
     */
    public void ensureNotUnderMoratorium() {
        var remaining = remaining();
        if (!remaining.isZero()) {
            throw new MoratoriumException(remaining);
        }
    }
}
