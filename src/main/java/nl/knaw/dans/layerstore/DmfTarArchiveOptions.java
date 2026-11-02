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
import java.time.Duration;

/**
 * Settings for creating DMF TAR archives: the layer is first packaged locally, then uploaded with a resumable transfer, and finally verified.
 *
 * @param localPackagingDir directory in which the archive is created locally before it is uploaded
 * @param maxRetries        maximum number of retries after a failed attempt (waiting for a moratorium to end does not count)
 * @param retryInterval     time to wait before retrying
 * @param verify            whether to verify the archive on the Data Archive after the upload and before it gets its final name
 * @param invalidOn         if the output of a failed verification contains this text, the archive is considered corrupt and is not retried
 */
public record DmfTarArchiveOptions(Path localPackagingDir, int maxRetries, Duration retryInterval, boolean verify, String invalidOn) {
}
