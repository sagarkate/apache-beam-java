package com.practice.gcp.pipelines;

import com.google.api.services.bigquery.model.TableRow;
import com.practice.gcp.options.MyOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class MyPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(MyPipeline.class);

    // public static class SchemaGeneratorFn implements SerializableFunction<String, TableSchema> {
    //     @Override
    //     public TableSchema apply(String inputSchema) {
    //         TableSchema tableSchema = new TableSchema();
    //         try {
    //             List<String> inputFields = Arrays.asList(inputSchema.split("|"));
    //             List<TableFieldSchema> tableSchemaFields = new ArrayList<>();
    //             for (String inputField : inputFields) {
    //                 String[] fieldNameAndType = inputField.split(":");
    //                 String fieldName = fieldNameAndType[0];
    //                 String fieldType = fieldNameAndType[1];
    //                 tableSchemaFields.add(new TableFieldSchema().setName(fieldName).setType(fieldType));
    //             }
    //             tableSchema.setFields(tableSchemaFields);
    //         } catch (Exception e) {
    //             e.printStackTrace();
    //         }

    //         return tableSchema;
    //     }
    // }

    // public static class ProjectedColumnsFn implements SerializableFunction<String, List<String>> {
    //     @Override
    //     public List<String> apply(String inputSchema) {
    //         List<String> fields = new ArrayList<>();
    //         try {
    //             List<String> inputFields = Arrays.asList(inputSchema.split("|"));
    //             for (String inputField : inputFields) {
    //                 String[] fieldNameAndType = inputField.split(":");
    //                 String fieldName = fieldNameAndType[0];
    //                 inputFields.add(fieldName);
    //             }
    //         } catch (Exception e) {
    //             e.printStackTrace();
    //         }

    //         return fields;
    //     }
    // }

    public static void main(String[] args) {
        PipelineOptionsFactory.register(MyOptions.class);
        MyOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).as(MyOptions.class);
        Pipeline p = Pipeline.create(pipelineOptions);

        PCollection<TableRow> rawInput = p.apply(
                "Read from Raw Input Table",
                BigQueryIO
                        .readTableRows()
                        .from(pipelineOptions.getInput())
                        .withMethod(pipelineOptions.getReadMethod())
                        // .withSelectedFields(ValueProvider.NestedValueProvider.of(pipelineOptions.getSchema(), new ProjectedColumnsFn()))
                        .withTemplateCompatibility()
                        .withoutValidation()
        );

        rawInput.apply(
                BigQueryIO
                        .writeTableRows()
                        .to(pipelineOptions.getOutput())
                        // .withSchema(schema)
                        // .withSchema(ValueProvider.NestedValueProvider.of(pipelineOptions.getSchema(), new SchemaGeneratorFn()))
                        .withSchema(NestedValueProvider.of(pipelineOptions.getSchema(), new SerializableFunction<String, TableSchema>() {
                            @Override
                            public TableSchema apply(String schema) {
                                LOG.info("MyPipelineLog - Input schema: " + schema);
                                TableSchema tableSchema = new TableSchema();
                                List<String> inputFields = Arrays.asList(schema.split("\\|"));
                                LOG.info("MyPipelineLog - input fields: "+inputFields);
                                List<TableFieldSchema> tableSchemaFields = new ArrayList<>();
                                for (String inputField : inputFields) {
                                    LOG.info("\n\nMyPipelineLog - inputField: "+ inputField);
                                    String[] fieldNameAndType = inputField.split(":");
                                    String fieldName = fieldNameAndType[0];
                                    String fieldType = fieldNameAndType[1];
                                    LOG.info(String.format("MyPipelineLog - Field Name: %s, Field Type: %s", fieldName, fieldType));
                                    tableSchemaFields.add(new TableFieldSchema().setName(fieldName).setType(fieldType));
                                }
                                tableSchema.setFields(tableSchemaFields);
                                LOG.info("MyPipelineLog - Returned Table Schema - " + tableSchema);
                                return tableSchema;
                            }
                        }))
                        .withCreateDisposition(pipelineOptions.getCreateDisposition())
                        .withWriteDisposition(pipelineOptions.getWriteDisposition())
                        .withMethod(pipelineOptions.getWriteMethod())
        );

        PipelineResult pr = p.run();
        try {
            pr.waitUntilFinish();
        } catch (Exception e) {
            e.printStackTrace();
        }
        // p.run().waitUntilFinish();
    }
}
