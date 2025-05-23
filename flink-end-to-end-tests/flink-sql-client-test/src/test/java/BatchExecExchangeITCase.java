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

import java.util.Arrays;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.HeartbeatManagerOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.connector.testframe.container.FlinkContainers;
import org.apache.flink.connector.testframe.container.FlinkContainersSettings;
import org.apache.flink.test.util.SQLJobSubmission;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.time.Duration;
import java.util.List;


/** E2E Test for BatchExecExchange. */
@Testcontainers
public class BatchExecExchangeITCase {

    private static final Logger LOG = LoggerFactory.getLogger(BatchExecExchangeITCase.class);

    @TempDir private File tempDir;
    private final FlinkContainers flink =
            FlinkContainers.builder()
                    .withFlinkContainersSettings(
                            FlinkContainersSettings.builder()
                                    .numTaskManagers(1)
                                    .setConfigOption(ExecutionOptions.RUNTIME_MODE,
                                            RuntimeExecutionMode.BATCH)
                                    .setConfigOption(CoreOptions.FLINK_JM_JVM_OPTIONS,
                                            "-XX:+UseCompressedOops")
                                    .setConfigOption(CoreOptions.FLINK_TM_JVM_OPTIONS,
                                            "-XX:-UseCompressedOops")
                                    .setConfigOption(
                                            HeartbeatManagerOptions.HEARTBEAT_TIMEOUT, Duration.ofSeconds(10000))
                                    .setConfigOption(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Default)
                                    .setConfigOption(CoreOptions.DEFAULT_PARALLELISM, 2)
                                    .build())
                    .build();

    private static final String TABLE1_FILE_NAME = "table1.csv";
    private static final String TABLE2_FILE_NAME = "table2.csv";

    private static final String OUTPUT_DIR_PATH = "output/";
    private static final String CONTAINER_BASE_MOUNT_PATH = "/flink/";
    private File table1File;
    private File table2File;
    private File outputDir;

    @BeforeEach
    void setup() throws Exception {
        // Prepare shared host directory
        File sharedDir = new File(tempDir, "shared");
        sharedDir.mkdirs();

        // Write test files
        table1File = new File(sharedDir, TABLE1_FILE_NAME);
        table2File = new File(sharedDir, TABLE2_FILE_NAME);
        outputDir = new File(sharedDir, OUTPUT_DIR_PATH);

        try (FileWriter t1 = new FileWriter(table1File);
                FileWriter t2 = new FileWriter(table2File)) {
            for (int i = 0; i < 10; i++) {
                String hex = String.format("%032x", i);
                t1.write(hex + ",T1_val_" + i + "\n");
                t2.write(hex + ",T2_val_" + i + "\n");
            }
        }

        flink.getJobManager().withFileSystemBind(
                sharedDir.getAbsolutePath(), CONTAINER_BASE_MOUNT_PATH, BindMode.READ_WRITE);

        flink.getTaskManagers().get(0).withFileSystemBind(
                sharedDir.getAbsolutePath(), CONTAINER_BASE_MOUNT_PATH, BindMode.READ_WRITE);

        flink.start(); // Start Flink AFTER setting up files and binding
    }

    @AfterEach
    void tearDown() {
        flink.stop();
    }

    @Test
    void testBinaryKeyJoin() throws Exception {
        String table1Path = CONTAINER_BASE_MOUNT_PATH + TABLE1_FILE_NAME;
        String table2Path = CONTAINER_BASE_MOUNT_PATH + TABLE2_FILE_NAME;
        String resultPath = CONTAINER_BASE_MOUNT_PATH + OUTPUT_DIR_PATH;

        List<String> sql = List.of(
                "SET 'execution.runtime-mode' = 'batch';",
                "CREATE TABLE Table1 (" +
                        "  k BINARY(16)," +
                        "  v1 STRING" +
                        ") WITH (" +
                        "  'connector' = 'filesystem'," +
                        "  'format' = 'csv'," +
                        "  'path' = '" + table1Path + "'" +
                        ");",
                "CREATE TABLE Table2 (" +
                        "  k BINARY(16)," +
                        "  v2 STRING" +
                        ") WITH (" +
                        "  'connector' = 'filesystem'," +
                        "  'format' = 'csv'," +
                        "  'path' = '" + table2Path + "'" +
                        ");",
                "CREATE TABLE Output (" +
                        "  k BINARY(16)," +
                        "  cnt BIGINT" +
                        ") WITH (" +
                        "  'connector' = 'filesystem'," +
                        "  'format' = 'csv'," +
                        "  'path' = '" + resultPath + "'" +
                        ");",
                "EXPLAIN SELECT t1.k, t1.v1, t2.v2 FROM Table1 t1 INNER JOIN Table2 t2"
                        + " ON t1.k = t2.k;",
                "INSERT INTO Output " +
                        "SELECT /*+ SHUFFLE_HASH(t1) */ t1.k, COUNT(*) AS cnt FROM Table1 t1,"
                        + " Table2 t2 WHERE t1.k = t2.k GROUP BY t1.k;"
        );

        executeSql(sql);

        // Assert output contains 10 joined rows
        File[] resultFiles = outputDir.listFiles();
        LOG.info("Result files: {}", Arrays.toString(resultFiles));
        long count = 0;
        for (File f : resultFiles) {
            try (BufferedReader reader = new BufferedReader(new FileReader(f))) {
                while (reader.readLine() != null) {
                    count++;
                }
            }
        }
        Assertions.assertEquals(10, count);
    }

    private void executeSql(List<String> sqlLines) throws Exception {
        flink.submitSQLJob(
                new SQLJobSubmission.SQLJobSubmissionBuilder(sqlLines).build());
    }
}
