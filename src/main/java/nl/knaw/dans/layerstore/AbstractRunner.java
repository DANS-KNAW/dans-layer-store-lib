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

import java.nio.file.Path;
import java.util.regex.Pattern;

public abstract class AbstractRunner {
    private static final Pattern USER_HOST_PATTERN = Pattern.compile("^[a-zA-Z0-9._-]+$");
    private static final Pattern COMMAND_INJECTION_PATTERN = Pattern.compile("[\\s;|&`$<>]");

    protected static String checkExecutableForSecurity(Path path) {
        var absPath = path.toAbsolutePath().toString();
        if (COMMAND_INJECTION_PATTERN.matcher(absPath).find()) {
            throw new IllegalArgumentException("Invalid executable path: " + absPath);
        }
        return absPath;
    }

    protected static String checkUserOrHostNameForSecurity(String param) {
        if (!USER_HOST_PATTERN.matcher(param).matches()) {
            throw new IllegalArgumentException("Invalid username: " + param);
        }
        return param;
    }

    protected static String checkRemoteBaseDirForSecurity(String param) {
        if (param.contains("..")) {
            throw new IllegalArgumentException("Invalid remote base directory: " + param);
        }
        return param;
    }
}
