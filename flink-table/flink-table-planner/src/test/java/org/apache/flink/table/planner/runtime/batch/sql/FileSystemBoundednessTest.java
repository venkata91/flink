/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.runtime.batch.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

/**
 * Test to reproduce the filesystem connector boundedness detection issue in batch mode.
 *
 * <p>Issue: When creating a filesystem table in BATCH mode without specifying
 * source.monitor-interval, the source should be BOUNDED, but it's being detected as UNBOUNDED in
 * some cases.
 */
public class FileSystemBoundednessTest {

    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    private String testDataPath;

    @Before
    public void setup() throws Exception {
        File dataDir = tempFolder.newFolder("test_data");
        testDataPath = dataDir.getAbsolutePath();

        // Create a simple test file
        File testFile = new File(dataDir, "test.csv");
        java.nio.file.Files.write(testFile.toPath(), "1,test\n2,data\n".getBytes());
    }

    @Test
    public void testFilesystemSourceInBatchMode() throws Exception {
        // Create batch table environment
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Explicitly set BATCH mode
        tEnv.getConfig().getConfiguration().setString("execution.runtime-mode", "BATCH");

        // Create filesystem table WITHOUT source.monitor-interval (should be bounded)
        tEnv.executeSql(
                "CREATE TABLE test_table ("
                        + "  id BIGINT,"
                        + "  name STRING"
                        + ") WITH ("
                        + "  'connector' = 'filesystem',"
                        + "  'path' = '"
                        + testDataPath
                        + "',"
                        + "  'format' = 'csv'"
                        + ")");

        // This should work - filesystem source without monitor-interval should be BOUNDED
        TableResult result = tEnv.executeSql("SELECT COUNT(*) FROM test_table");

        // If we get here without exception, the test passes
        result.collect().close();
    }

    @Test
    public void testFilesystemSourceInBatchModeWithParquet() throws Exception {
        // Create batch table environment
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Explicitly set BATCH mode
        tEnv.getConfig().getConfiguration().setString("execution.runtime-mode", "BATCH");

        // Create filesystem table with parquet format
        tEnv.executeSql(
                "CREATE TABLE parquet_table ("
                        + "  id BIGINT,"
                        + "  name STRING"
                        + ") WITH ("
                        + "  'connector' = 'filesystem',"
                        + "  'path' = '"
                        + testDataPath
                        + "',"
                        + "  'format' = 'parquet'"
                        + ")");

        // This should work - filesystem source without monitor-interval should be BOUNDED
        TableResult result = tEnv.executeSql("SELECT COUNT(*) FROM parquet_table");

        // If we get here without exception, the test passes
        result.collect().close();
    }
}
