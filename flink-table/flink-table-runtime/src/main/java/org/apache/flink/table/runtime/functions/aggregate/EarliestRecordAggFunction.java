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

package org.apache.flink.table.runtime.functions.aggregate;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AggregateFunction;

@FunctionHint(
        input = {
            @DataTypeHint("BIGINT"),
            @DataTypeHint("INT"),
            @DataTypeHint(value = "ROW<`a` STRING, `b` STRING>", bridgedTo = RowData.class) },
        output = @DataTypeHint(value = "ROW<`f0` BIGINT, `f1` INT, f2 ROW<`a` STRING, `b` STRING>>",
                bridgedTo = RowData.class))
public class EarliestRecordAggFunction
        extends AggregateFunction<RowData, EarliestRecordAggFunction.Accumulator> {
    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    public void accumulate(Accumulator acc, Long timestamp, Integer memberId, RowData row) {
        if (acc.timestamp == null || timestamp < acc.timestamp) {
            acc.setTimestamp(timestamp);
            acc.setRow(GenericRowData.of(timestamp, memberId, row));
        }
    }

    @Override
    public RowData getValue(Accumulator accumulator) {
        return accumulator.getRow();
    }

/*    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                // accept a signature (BIGINT, INT, ROW) with arbitrary field types but
                // with internal conversion classes
                .inputTypeStrategy(
                        new InputTypeStrategy() {
                            @Override
                            public ArgumentCount getArgumentCount() {
                                // the argument count is checked before input types are inferred
                                return ConstantArgumentCount.of(3);
                            }

                            @Override
                            public Optional<List<DataType>> inferInputTypes(
                                    CallContext callContext, boolean throwOnFailure) {
                                final List<DataType> args = callContext.getArgumentDataTypes();
                                final DataType arg0 = args.get(0);
                                final DataType arg1 = args.get(1);
                                final DataType arg2 = args.get(2);
                                // keep the original logical type but express that both arguments
                                // should use internal data structures
                                return Optional.of(
                                        Arrays.asList(
                                                arg0.toInternal(),
                                                arg1.toInternal(),
                                                arg2.bridgedTo(RowData.class)));
                            }

                            @Override
                            public List<Signature> getExpectedSignatures(
                                    FunctionDefinition definition) {
                                // this helps in printing nice error messages
                                return Collections.singletonList(
                                        Signature.of(
                                                Signature.Argument.ofGroup(LogicalTypeRoot.BIGINT),
                                                Signature.Argument.ofGroup(LogicalTypeRoot.INTEGER),
                                                Signature.Argument.ofGroup(LogicalTypeRoot.ROW)));
                            }
                        })
                .outputTypeStrategy(
                        callContext -> {
                            final List<DataType> args = callContext.getArgumentDataTypes();
                            final List<DataType> allFieldDataTypes = new ArrayList<>();
                            allFieldDataTypes.add(args.get(0).toInternal());
                            allFieldDataTypes.add(args.get(1).toInternal());
                            allFieldDataTypes.add(args.get(2).toInternal());
                            final DataTypes.Field[] fields =
                                    IntStream.range(0, allFieldDataTypes.size())
                                            .mapToObj(
                                                    i ->
                                                            DataTypes.FIELD(
                                                                    "f" + i,
                                                                    allFieldDataTypes.get(i)))
                                            .toArray(DataTypes.Field[]::new);
                            // create a new row with the merged fields and express that the return
                            // type will use an internal data structure
                            return Optional.of(DataTypes.ROW(fields).bridgedTo(RowData.class));
                        })
                .build();
    }*/

    public static class Accumulator {
        private Long timestamp;

        @DataTypeHint(value = "ROW<`a` STRING, `b` STRING>", bridgedTo = RowData.class)
        private RowData row;

        public Long getTimestamp() {
            return timestamp;
        }

        public RowData getRow() {
            return row;
        }

        public void setTimestamp(Long timestamp) {
            this.timestamp = timestamp;
        }

        public void setRow(RowData row) {
            this.row = row;
        }
    }
}
