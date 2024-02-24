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

import lombok.Value;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

@Value
public class LiveTestProperties {
    Path dmfTarExecutable;
    Path remoteBaseDir;
    String user;
    String host;
    Path inputDir;

    public LiveTestProperties() {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream("live-test.properties"));
        }
        catch (IOException e) {
            throw new RuntimeException("Could not read live-test.properties", e);
        }
        dmfTarExecutable = getPath(properties, "dmftar.executable");
        remoteBaseDir = getPath(properties, "dmftar.remote-base-dir");
        user = properties.getProperty("dmftar.user");
        host = properties.getProperty("dmftar.host");
        inputDir = getPath(properties, "input-dir");
    }

    private static Path getPath(Properties properties, String key) {
        if (properties.getProperty(key) != null) {
            return Path.of(properties.getProperty(key));
        }
        else {
            return null;
        }
    }
}
