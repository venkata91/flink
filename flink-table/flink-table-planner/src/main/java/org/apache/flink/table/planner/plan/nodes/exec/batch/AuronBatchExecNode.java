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

package org.apache.flink.table.planner.plan.nodes.exec.batch;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.processor.ProcessorContext;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Batch ExecNode that delegates execution to Auron native engine.
 *
 * <p>This node wraps a Flink batch operator (or subgraph of operators) and converts it to execute
 * using Auron's native vectorized execution engine. The conversion happens through the Auron
 * converter API which:
 *
 * <ol>
 *   <li>Extracts information from Flink ExecNodes (scan specs, filters, projections)
 *   <li>Converts to Auron's protobuf plan representation
 *   <li>Creates a Flink transformation that executes the Auron plan
 * </ol>
 *
 * <p>This node is created by {@link
 * org.apache.flink.table.planner.plan.nodes.exec.processor.AuronExecNodeGraphProcessor} when
 * compatible patterns are detected in the execution graph.
 */
public class AuronBatchExecNode extends ExecNodeBase<RowData> implements BatchExecNode<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(AuronBatchExecNode.class);

    private static final String AURON_CONVERTER_CLASS =
            "org.apache.auron.flink.planner.AuronExecNodeConverter";
    private static final String AURON_TRANSFORMATION_FACTORY_CLASS =
            "org.apache.auron.flink.planner.AuronTransformationFactory";

    private final ExecNode<?> originalNode;
    private final List<ExecNode<?>> originalInputs;

    /**
     * Creates an Auron batch execution node.
     *
     * @param originalNode The original Flink ExecNode to convert
     * @param originalInputs The input nodes (already recursively converted)
     * @param context The processor context
     */
    public AuronBatchExecNode(
            ExecNode<?> originalNode, List<ExecNode<?>> originalInputs, ProcessorContext context) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(AuronBatchExecNode.class),
                ExecNodeContext.newPersistedConfig(
                        AuronBatchExecNode.class,
                        context.getPlanner().getTableConfig().getConfiguration()),
                createInputProperties(originalInputs), // Create InputProperty for each input
                originalNode.getOutputType(),
                "Auron[" + originalNode.getDescription() + "]");

        this.originalNode = originalNode;
        this.originalInputs = originalInputs;

        // Set the input edges to link this node to its inputs
        setInputEdges(createInputEdges(originalInputs));

        LOG.info(
                "Created AuronBatchExecNode wrapping: {} with {} inputs",
                originalNode.getClass().getSimpleName(),
                originalInputs.size());
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        try {
            // Use reflection to call Auron converter and transformation factory
            // This avoids hard compile-time dependency on auron-flink-extension

            // Step 1: Convert Flink ExecNode to Auron PhysicalPlanNode (protobuf)
            Class<?> converterClass = Class.forName(AURON_CONVERTER_CLASS);
            Method convertMethod = converterClass.getMethod("convert", ExecNode.class, List.class);
            Object auronPlan = convertMethod.invoke(null, originalNode, originalInputs);

            LOG.debug(
                    "Successfully converted Flink ExecNode to Auron plan: {}",
                    originalNode.getDescription());

            // Step 2: Create Flink transformation from Auron plan
            Class<?> factoryClass = Class.forName(AURON_TRANSFORMATION_FACTORY_CLASS);
            Method createMethod =
                    factoryClass.getMethod(
                            "createTransformation",
                            Object.class, // PhysicalPlanNode (avoid direct dependency)
                            RowType.class,
                            PlannerBase.class);

            RowType outputRowType = (RowType) getOutputType();
            Transformation<RowData> transformation =
                    (Transformation<RowData>)
                            createMethod.invoke(null, auronPlan, outputRowType, planner);

            transformation.setName("Auron: " + originalNode.getDescription());
            transformation.setDescription(getDescription());

            LOG.info(
                    "Successfully created Auron transformation for: {}",
                    originalNode.getDescription());

            return transformation;

        } catch (Exception e) {
            LOG.error(
                    "Failed to create Auron transformation, this should not happen as "
                            + "the processor should have validated Auron availability: {}",
                    e.getMessage(),
                    e);
            throw new RuntimeException(
                    "Failed to create Auron transformation for node: "
                            + originalNode.getDescription(),
                    e);
        }
    }

    /**
     * Returns the original Flink ExecNode that this Auron node wraps.
     *
     * @return The original node
     */
    public ExecNode<?> getOriginalNode() {
        return originalNode;
    }

    /**
     * Returns the original input nodes.
     *
     * @return The input nodes
     */
    public List<ExecNode<?>> getOriginalInputs() {
        return originalInputs;
    }

    /**
     * Creates a list of InputProperty for the given input nodes.
     * Each input gets a default InputProperty.
     *
     * @param inputs The input nodes
     * @return List of InputProperty objects
     */
    private static List<InputProperty> createInputProperties(List<ExecNode<?>> inputs) {
        List<InputProperty> inputProperties = new ArrayList<>(inputs.size());
        for (int i = 0; i < inputs.size(); i++) {
            inputProperties.add(InputProperty.DEFAULT);
        }
        return inputProperties;
    }

    /**
     * Creates a list of ExecEdge for the given input nodes.
     * Each edge uses FORWARD shuffle and PIPELINED exchange mode.
     *
     * @param inputs The input nodes
     * @return List of ExecEdge objects
     */
    private List<ExecEdge> createInputEdges(List<ExecNode<?>> inputs) {
        List<ExecEdge> inputEdges = new ArrayList<>(inputs.size());
        for (ExecNode<?> input : inputs) {
            inputEdges.add(ExecEdge.builder()
                    .source(input)
                    .target(this)
                    .build());
        }
        return inputEdges;
    }
}
