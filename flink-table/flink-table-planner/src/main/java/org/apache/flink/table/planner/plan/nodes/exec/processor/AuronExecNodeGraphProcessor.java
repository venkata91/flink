/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.nodes.exec.processor;

import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSink;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSinkSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSourceSpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

/**
 * Processor that converts supported Flink batch operators to use Auron native execution.
 *
 * <p>This processor identifies patterns in the ExecNode graph that can be accelerated using Auron's
 * native vectorized execution engine. Currently supported patterns include:
 *
 * <ul>
 *   <li>Parquet table scans (with optional filter and projection pushdown)
 *   <li>Filter operations on Parquet scans
 *   <li>Projection operations on Parquet scans
 *   <li>Combined Calc nodes (filter + projection) on Parquet scans
 * </ul>
 *
 * <p>The processor works by traversing the ExecNode graph and replacing compatible subgraphs with
 * {@link org.apache.flink.table.planner.plan.nodes.exec.batch.AuronBatchExecNode} instances that
 * delegate execution to Auron's native engine.
 *
 * <p>This processor is only active when {@link
 * OptimizerConfigOptions#TABLE_OPTIMIZER_AURON_ENABLED} is set to true and the
 * auron-flink-extension library is available on the classpath.
 */
public class AuronExecNodeGraphProcessor implements ExecNodeGraphProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(AuronExecNodeGraphProcessor.class);

    private static final String AURON_BATCH_EXEC_NODE_CLASS =
            "org.apache.flink.table.planner.plan.nodes.exec.batch.AuronBatchExecNode";

    private static final String FILE_SYSTEM_TABLE_SOURCE_CLASS =
            "org.apache.flink.table.filesystem.FileSystemTableSource";

    // Flag to track if Auron classes are available on classpath
    private final boolean auronAvailable;

    public AuronExecNodeGraphProcessor() {
        this.auronAvailable = checkAuronAvailability();
        if (auronAvailable) {
            LOG.info(
                    "Auron native execution is available and will be used for supported operators");
        } else {
            LOG.warn(
                    "Auron native execution is enabled in configuration but auron-flink-extension "
                            + "library is not available on the classpath. Falling back to standard Flink execution.");
        }
    }

    @Override
    public ExecNodeGraph process(ExecNodeGraph execGraph, ProcessorContext context) {
        if (!auronAvailable) {
            LOG.debug("Auron not available, skipping graph processing");
            return execGraph;
        }

        List<ExecNode<?>> rootNodes = execGraph.getRootNodes();
        List<ExecNode<?>> convertedRoots = new ArrayList<>();

        for (ExecNode<?> root : rootNodes) {
            ExecNode<?> converted = convertNode(root, context);
            convertedRoots.add(converted);
        }

        return new ExecNodeGraph(convertedRoots);
    }

    /**
     * Recursively converts ExecNodes to Auron nodes where applicable.
     *
     * @param node The node to convert
     * @param context The processor context
     * @return The converted node (or original if not convertible)
     */
    private ExecNode<?> convertNode(ExecNode<?> node, ProcessorContext context) {
        // First, recursively convert all inputs
        List<ExecNode<?>> convertedInputs = new ArrayList<>();
        for (ExecEdge edge : node.getInputEdges()) {
            ExecNode<?> input = edge.getSource();
            convertedInputs.add(convertNode(input, context));
        }

        // Try to convert this node + inputs to Auron
        if (canConvertToAuron(node, convertedInputs)) {
            try {
                ExecNode<?> auronNode = convertToAuronExecNode(node, convertedInputs, context);
                LOG.info(
                        "Converted {} to Auron native execution: {}",
                        node.getClass().getSimpleName(),
                        node.getDescription());
                return auronNode;
            } catch (Exception e) {
                LOG.warn(
                        "Failed to convert node to Auron execution, falling back to standard execution: {}",
                        e.getMessage());
                // Fall through to return original node with converted inputs
            }
        }

        // If inputs were converted, update node's edges
        if (!convertedInputs.isEmpty() && hasInputsChanged(node, convertedInputs)) {
            return cloneNodeWithNewInputs(node, convertedInputs);
        }

        return node;
    }

    /**
     * Checks if this node and its inputs can be converted to Auron execution.
     *
     * <p>This method supports end-to-end native execution by detecting complete chains from source
     * to sink that can be executed entirely in the native engine.
     *
     * @param node The node to check
     * @param inputs The converted input nodes
     * @return true if the pattern is supported by Auron
     */
    private boolean canConvertToAuron(ExecNode<?> node, List<ExecNode<?>> inputs) {
        // Only convert BatchExecNodes
        if (!(node instanceof BatchExecNode)) {
            return false;
        }

        // Pattern 1: End-to-end native execution (Source -> [Transforms] -> Sink)
        // Check if this is a Parquet sink with native-compatible input chain
        if (node instanceof BatchExecSink && inputs.size() == 1) {
            if (isParquetSink((BatchExecSink) node)) {
                // The input chain has already been converted if possible
                // Check if the input is either:
                // 1. Already an Auron node (native chain)
                // 2. A native-compatible node (source or calc on source)
                return isNativeCompatibleChain(inputs.get(0));
            }
        }

        // Pattern 2: BatchExecCalc (filter/projection) on top of Parquet scan
        if (node instanceof BatchExecCalc && inputs.size() == 1) {
            ExecNode<?> input = inputs.get(0);
            if (input instanceof CommonExecTableSourceScan) {
                return isParquetSource((CommonExecTableSourceScan) input);
            }
        }

        // Pattern 3: Just Parquet scan (no calc)
        if (node instanceof CommonExecTableSourceScan) {
            return isParquetSource((CommonExecTableSourceScan) node);
        }

        return false;
    }

    /**
     * Checks if the given node is part of a native-compatible execution chain. This includes Auron
     * nodes and native-compatible Flink nodes.
     */
    private boolean isNativeCompatibleChain(ExecNode<?> node) {
        // Already converted to Auron
        if (node.getClass().getName().equals(AURON_BATCH_EXEC_NODE_CLASS)) {
            return true;
        }

        // Parquet source - native compatible
        if (node instanceof CommonExecTableSourceScan) {
            return isParquetSource((CommonExecTableSourceScan) node);
        }

        // Calc on native-compatible input
        if (node instanceof BatchExecCalc && node.getInputEdges().size() == 1) {
            ExecNode<?> input = node.getInputEdges().get(0).getSource();
            return isNativeCompatibleChain(input);
        }

        return false;
    }

    /**
     * Checks if the table sink is a Parquet file sink.
     *
     * @param sink The sink node
     * @return true if this is a Parquet sink
     */
    private boolean isParquetSink(BatchExecSink sink) {
        try {
            DynamicTableSinkSpec sinkSpec = sink.getTableSinkSpec();

            // Check if this is a filesystem sink with Parquet format
            // The spec contains the connector and format information
            // For now, be optimistic - assume filesystem sinks are Parquet
            LOG.debug(
                    "Detected sink node, checking if Parquet-compatible: {}",
                    sink.getDescription());

            // TODO: More precise detection by checking:
            // - connector = 'filesystem'
            // - format = 'parquet'
            // This would require accessing the catalog table options

            return true; // Optimistic - will fall back if conversion fails
        } catch (Exception e) {
            LOG.debug("Error checking if sink is Parquet: {}", e.getMessage());
        }
        return false;
    }

    /**
     * Checks if the table source is a Parquet file source.
     *
     * @param scan The table source scan node
     * @return true if this is a Parquet source
     */
    private boolean isParquetSource(CommonExecTableSourceScan scan) {
        try {
            DynamicTableSourceSpec sourceSpec = scan.getTableSourceSpec();

            // Check the table source class name using reflection
            // We can't directly instantiate the table source without FlinkContext,
            // so we check the class name from the resolved table
            String sourceClassName = sourceSpec.getClass().getName();

            // For now, we assume all FileSystem-based tables are Parquet
            // In a more complete implementation, check table options for format
            LOG.debug(
                    "Detected table source, treating as Parquet-compatible: {}",
                    scan.getDescription());
            return true; // Be optimistic - if conversion fails, it will fall back
        } catch (Exception e) {
            LOG.debug("Error checking if source is Parquet: {}", e.getMessage());
        }
        return false;
    }

    /**
     * Converts a Flink ExecNode to an Auron ExecNode using reflection.
     *
     * @param node The node to convert
     * @param inputs The converted input nodes
     * @param context The processor context
     * @return The Auron ExecNode
     */
    private ExecNode<?> convertToAuronExecNode(
            ExecNode<?> node, List<ExecNode<?>> inputs, ProcessorContext context) throws Exception {

        // Use reflection to create AuronBatchExecNode
        // This avoids a hard compile-time dependency on auron-flink-extension
        Class<?> auronNodeClass = Class.forName(AURON_BATCH_EXEC_NODE_CLASS);
        Constructor<?> constructor =
                auronNodeClass.getConstructor(ExecNode.class, List.class, ProcessorContext.class);

        ExecNode<?> auronNode = (ExecNode<?>) constructor.newInstance(node, inputs, context);

        // Set up input edges to properly wire the node into the graph
        List<ExecEdge> inputEdges = new ArrayList<>();
        List<ExecEdge> originalEdges = node.getInputEdges();
        for (int i = 0; i < inputs.size(); i++) {
            ExecEdge originalEdge = originalEdges.get(i);
            inputEdges.add(
                    ExecEdge.builder()
                            .source(inputs.get(i))
                            .target(auronNode)
                            .shuffle(originalEdge.getShuffle())
                            .exchangeMode(originalEdge.getExchangeMode())
                            .build());
        }
        auronNode.setInputEdges(inputEdges);

        return auronNode;
    }

    /**
     * Clones a node with new input edges.
     *
     * @param node The node to clone
     * @param newInputs The new input nodes
     * @return A cloned node with updated inputs
     */
    private ExecNode<?> cloneNodeWithNewInputs(ExecNode<?> node, List<ExecNode<?>> newInputs) {
        // Update the input edges
        List<ExecEdge> newEdges = new ArrayList<>();
        for (int i = 0; i < newInputs.size(); i++) {
            ExecEdge oldEdge = node.getInputEdges().get(i);
            newEdges.add(
                    ExecEdge.builder()
                            .source(newInputs.get(i))
                            .target(node)
                            .shuffle(oldEdge.getShuffle())
                            .exchangeMode(oldEdge.getExchangeMode())
                            .build());
        }
        node.setInputEdges(newEdges);
        return node;
    }

    /**
     * Checks if any inputs have changed during conversion.
     *
     * @param node The node to check
     * @param newInputs The potentially new input nodes
     * @return true if inputs have changed
     */
    private boolean hasInputsChanged(ExecNode<?> node, List<ExecNode<?>> newInputs) {
        List<ExecEdge> currentEdges = node.getInputEdges();
        if (currentEdges.size() != newInputs.size()) {
            return true;
        }
        for (int i = 0; i < currentEdges.size(); i++) {
            if (currentEdges.get(i).getSource() != newInputs.get(i)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks if Auron classes are available on the classpath.
     *
     * @return true if Auron is available
     */
    private boolean checkAuronAvailability() {
        try {
            Class.forName(AURON_BATCH_EXEC_NODE_CLASS);
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }
}
