package com.practice.gcp.pipelines;

import com.google.api.services.bigquery.model.TableRow;
import com.practice.gcp.options.MyOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.practice.gcp.transform.BqRawToStagingTransformFn;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class MyPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(MyPipeline.class);
    private static final String SCHEMA_FIELD_DELIMITER = "\\|";

    public static TableSchema getTableSchema(String schema) {
        TableSchema tableSchema = new TableSchema();
        List<String> inputFields = Arrays.asList(schema.split(SCHEMA_FIELD_DELIMITER));
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

        return tableSchema.setFields(tableSchemaFields);
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
                ParDo.of(new BqRawToStagingTransformFn(
                        pipelineOptions.getSchema(),
                        SCHEMA_FIELD_DELIMITER))
        );

        transformedData.apply(
                BigQueryIO
                        .writeTableRows()
                        .to(pipelineOptions.getOutput())
                        .withSchema(NestedValueProvider.of(pipelineOptions.getSchema(), new SerializableFunction<String, TableSchema>() {
                            @Override
                            public TableSchema apply(String schema) {
                                LOG.info("MyPipelineTransformLog - Input schema: " + schema);
                                TableSchema tableSchema = getTableSchema(schema);
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
