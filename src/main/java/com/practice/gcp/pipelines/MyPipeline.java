package com.practice.gcp.pipelines;

import com.google.api.services.bigquery.model.TableRow;
import com.practice.gcp.options.MyOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.PCollection;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.Gson;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class MyPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(MyPipeline.class);

    public static class TransformFn extends DoFn<TableRow, TableRow> {
        ValueProvider<String> schema;

        public TransformFn(ValueProvider<String> schema) {
            this.schema = schema;
        }

        @ProcessElement
        public void processElement(@Element TableRow inputRow, ProcessContext c, OutputReceiver<TableRow> outputReceiver) {
            LOG.info("MyPipelineTransformLog - Inside TransformFn - ");

            LOG.info(String.format("MyPipelineTransformLog - Inside TransformFn - inputRow: ", new Gson().toJson(inputRow)));
            LOG.info(String.format("MyPipelineTransformLog - Inside TransformFn - inputRow keys: %s", inputRow.keySet()));
            LOG.info(String.format("MyPipelineTransformLog - Inside TransformFn - inputRow values: %s", inputRow.values()));

            TableRow transformedRow = new TableRow();
            if (schema.isAccessible()) {
                LOG.info("MyPipelineTransformLog - Inside TransformFn.. inputSchema - " + schema);
                String[] fields = schema.get().split("\\|");

                LOG.info("MyPipelineTransformLog - Inside TransformFn - fields from schema string split('\\|') - " + Arrays.toString(fields));

                for(String field: fields) {
                    String[] fieldNameAndfieldType = field.split(":");
                    LOG.info("MyPipelineTransformLog - Inside TransformFn - fieldNameAndfieldType from field string split(':') - " + Arrays.toString(fieldNameAndfieldType));
                    String fieldName = fieldNameAndfieldType[0];
                    String fieldType = fieldNameAndfieldType[1];
                    LOG.info(String.format("MyPipelineTransformLog - Inside TransformFn - fieldName : %s, fieldType: %s ", fieldName, fieldType));
                    LOG.info(String.format("MyPipelineTransformLog - Inside TransformFn - inputRow field value: %s", (String) inputRow.get(fieldName)));
                    try {
                        switch (fieldType) {
                            case "INTEGER":
                            case "LONG":
                                transformedRow.set(fieldName, Long.parseLong((String) inputRow.get(fieldName)));
                                break;
                            case "DATE":
                                transformedRow.set(fieldName, LocalDate.parse((String) inputRow.get(fieldName)).toString());
                                break;
                            case "DATETIME":
                                transformedRow.set(fieldName, LocalDateTime.parse((String) inputRow.get(fieldName)).toString());
                                break;
                            case "TIMESTAMP":
                                transformedRow.set(fieldName, DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse((String) inputRow.get(fieldName), Instant::from).toString());
                                break;
                            case "STRING":
                                transformedRow.set(fieldName, inputRow.get(fieldName));
                                break;
                            case "BOOLEAN":
                                transformedRow.set(fieldName, Boolean.parseBoolean((String) inputRow.get(fieldName)));
                                break;
                            case "FLOAT":
                                transformedRow.set(fieldName, Double.parseDouble((String) inputRow.get(fieldName)));
                                break;
                            case "NUMERIC":
                                transformedRow.set(fieldName, new BigDecimal((String) inputRow.get(fieldName)).toString());
                                break;
                            default:
                                throw new Exception(String.format("Data type %s of field %s is not supported.. Please use one of the below data types: \n [INTEGER|LONG|DATE|DATETIME|TIMESTAMP|STRING|BOOLEAN|FLOAT|NUMERIC]", fieldType, fieldName));
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                // transformedRow.set("u_desc", Instant.now());
                transformedRow.set("record_load_ts", DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(Instant.now().toString(), Instant::from).toString());
                LOG.info(String.format("MyPipelineTransformLog - Inside TransformFn - transformedRow keys: %s", transformedRow.keySet()));
                LOG.info(String.format("MyPipelineTransformLog - Inside TransformFn - transformedRow values: %s", transformedRow.values()));
            }

            outputReceiver.output(transformedRow);
        }
    }

    public static void main(String[] args) {
        PipelineOptionsFactory.register(MyOptions.class);
        MyOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        Pipeline p = Pipeline.create(pipelineOptions);

        LOG.info("MyPipelineTransformLog - main() - pipelineOptions: \n" + pipelineOptions);

        PCollection<TableRow> rawInput = p.apply(
                "Read from Raw Input Table",
                BigQueryIO
                        .readTableRows()
                        .from(pipelineOptions.getInput())
                        .withTemplateCompatibility()
                        .withoutValidation()
        );

        PCollection<TableRow> transformedData = rawInput.apply(
                "Transform Raw Input",
                ParDo.of(new TransformFn(pipelineOptions.getSchema()))
        );

        transformedData.apply(
                BigQueryIO
                        .writeTableRows()
                        .to(pipelineOptions.getOutput())
                        .withSchema(NestedValueProvider.of(pipelineOptions.getSchema(), new SerializableFunction<String, TableSchema>() {
                            @Override
                            public TableSchema apply(String schema) {
                                LOG.info("MyPipelineTransformLog - Input schema: " + schema);
                                TableSchema tableSchema = new TableSchema();
                                List<String> inputFields = Arrays.asList(schema.split("\\|"));
                                LOG.info("MyPipelineTransformLog - input fields: "+inputFields);
                                List<TableFieldSchema> tableSchemaFields = new ArrayList<>();
                                for (String inputField : inputFields) {
                                    LOG.info("\n\nMyPipelineTransformLog - inputField: "+ inputField);
                                    String[] fieldNameAndType = inputField.split(":");
                                    String fieldName = fieldNameAndType[0];
                                    String fieldType = fieldNameAndType[1];
                                    LOG.info(String.format("MyPipelineTransformLog - Field Name: %s, Field Type: %s", fieldName, fieldType));
                                    tableSchemaFields.add(new TableFieldSchema().setName(fieldName).setType(fieldType));
                                }
                                tableSchema.setFields(tableSchemaFields);
                                LOG.info("MyPipelineTransformLog - Returned Table Schema - " + tableSchema);
                                return tableSchema;
                            }
                        }))
                        .withCreateDisposition(pipelineOptions.getCreateDisposition())
                        .withWriteDisposition(pipelineOptions.getWriteDisposition())
        );

        try {
            p.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
