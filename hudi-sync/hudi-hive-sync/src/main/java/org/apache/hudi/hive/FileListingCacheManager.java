/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.hive;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.sync.common.HoodieSyncClient;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class FileListingCacheManager {
    private static final Logger LOG = LogManager.getLogger(FileListingCacheManager.class);
    protected HoodieSyncClient syncClient;
    protected String cacheFilePath;
    public FileListingCacheManager(HoodieSyncClient syncClient, String cacheFilePath) {
        this.syncClient = syncClient;
        this.cacheFilePath = cacheFilePath;
    }

    List<String> getAllPartitions() {
        // first, check cacheDir for any cached partitions
        Path path = Paths.get(this.cacheFilePath);
        if (Files.exists(path)) {
            try {
                LOG.info("Found cache file at " + cacheFilePath + ". Reading partitions from file.");
                List<String> partitions = Files.readAllLines(path);
                LOG.info("Found " + partitions.size() + " partitions in local cache.");
                return partitions;
            } catch (IOException e) {
                LOG.warn("Error reading partitions from cache. Continuing to list from storage", e);
            }
        }
        // fetch all partitions and cache them
        LOG.info("Fetching partitions from storage");
        List<String> partitions = syncClient.getWrittenPartitionsSince(Option.empty());
        try {
            LOG.info("Caching " + partitions.size() + "partitions to local storage at path " + cacheFilePath);
            Files.write(path, partitions);
        } catch (IOException e) {
            LOG.warn("Unable to cache partitions.", e);
        }
        return partitions;
    }
}
