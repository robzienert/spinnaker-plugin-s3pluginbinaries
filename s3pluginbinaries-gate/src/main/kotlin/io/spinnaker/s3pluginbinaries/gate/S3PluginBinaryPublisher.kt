/*
 * Copyright 2020 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.spinnaker.s3pluginbinaries.gate

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import io.spinnaker.s3pluginbinaries.core.S3ConfigProperties
import io.spinnaker.s3pluginbinaries.core.pluginBinaryBucketKey
import java.io.InputStream

class S3PluginBinaryPublisher(
  private val s3: AmazonS3,
  private val config: S3ConfigProperties
) {
  fun publish(body: String, pluginId: String, pluginVersion: String) {
    val key = pluginBinaryBucketKey(config.rootDir, pluginId, pluginVersion)
    if (s3.doesObjectExist(config.bucket, key)) {
      throw PluginBinaryAlreadyExistsException("A plugin binary can only be written once: $key")
    }

    s3.putObject(PutObjectRequest(
      config.bucket,
      key,
      body
    ))
  }
}
