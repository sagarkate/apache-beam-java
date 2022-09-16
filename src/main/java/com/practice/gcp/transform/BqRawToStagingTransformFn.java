package com.practice.gcp.transform;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BqRawToStagingTransformFn extends DoFn<TableRow, TableRow> {
    private static final String RECORD_LOAD_TS = "record_load_ts";
    private static final Logger LOG = LoggerFactory.getLogger(BqRawToStagingTransformFn.class);
    ValueProvider<String> schema;
    String schemaFieldDelimiter;

    public BqRawToStagingTransformFn(ValueProvider<String> schema, String schemaFieldDelimiter) {
        this.schema = schema;
        this.schemaFieldDelimiter = schemaFieldDelimiter;
    }

    public TableRow setField(TableRow outputRow, TableRow inputRow, String fieldType, String fieldName) {
        TableRow transformedRow = new TableRow();
        try {
            switch (fieldType) {
                case "INTEGER":
                case "LONG":
                    transformedRow = outputRow.set(fieldName, Long.parseLong((String) inputRow.get(fieldName)));
                    break;
                case "DATE":
                    transformedRow = outputRow.set(fieldName, LocalDate.parse((String) inputRow.get(fieldName)).toString());
                    break;
                case "DATETIME":
                    transformedRow = outputRow.set(fieldName, LocalDateTime.parse((String) inputRow.get(fieldName)).toString());
                    break;
                case "TIMESTAMP":
                    transformedRow = outputRow.set(fieldName, DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse((String) inputRow.get(fieldName), Instant::from).toString());
                    break;
                case "STRING":
                    transformedRow = outputRow.set(fieldName, inputRow.get(fieldName));
                    break;
                case "BOOLEAN":
                    transformedRow = outputRow.set(fieldName, Boolean.parseBoolean((String) inputRow.get(fieldName)));
                    break;
                case "FLOAT":
                    transformedRow = outputRow.set(fieldName, Double.parseDouble((String) inputRow.get(fieldName)));
                    break;
                case "NUMERIC":
                    transformedRow = outputRow.set(fieldName, new BigDecimal((String) inputRow.get(fieldName)).toString());
                    break;
                default:
                    throw new Exception(String.format("Data type %s of field %s is not supported.. Please use one of the below data types: \n [INTEGER|LONG|DATE|DATETIME|TIMESTAMP|STRING|BOOLEAN|FLOAT|NUMERIC]", fieldType, fieldName));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return transformedRow;
    }

    public TableRow getTransformedRow(TableRow inputRow, String[] fields) {
        TableRow transformedRow = new TableRow();
        for(String field: fields) {
            String[] fieldNameAndfieldType = field.split(":");

            LOG.info("MyPipelineTransformLog - Inside TransformFn - fieldNameAndfieldType from field string split(':') - " + Arrays.toString(fieldNameAndfieldType));

            String fieldName = fieldNameAndfieldType[0];
            String fieldType = fieldNameAndfieldType[1];

            LOG.info(String.format("MyPipelineTransformLog - Inside TransformFn - fieldName : %s, fieldType: %s ", fieldName, fieldType));
            LOG.info(String.format("MyPipelineTransformLog - Inside TransformFn - inputRow field value: %s", (String) inputRow.get(fieldName)));
            if (fieldName.equals(RECORD_LOAD_TS)) {
                transformedRow.set("record_load_ts", DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(Instant.now().toString(), Instant::from).toString());
            } else {
                transformedRow = setField(transformedRow, inputRow, fieldType, fieldName);
            }

            LOG.info(String.format("MyPipelineTransformLog - Inside TransformFn - transformedRow keys: %s", transformedRow.keySet()));
            LOG.info(String.format("MyPipelineTransformLog - Inside TransformFn - transformedRow values: %s", transformedRow.values()));
        }

        return transformedRow;
    }

    public String[] getFields(ValueProvider<String> schema) {
        return schema.get().split(schemaFieldDelimiter);
    }

    @ProcessElement
    public void processElement(@Element TableRow inputRow, ProcessContext c, OutputReceiver<TableRow> outputReceiver) {
        LOG.info("MyPipelineTransformLog - Inside TransformFn - ");
        LOG.info(String.format("MyPipelineTransformLog - Inside TransformFn - inputRow keys: %s", inputRow.keySet()));
        LOG.info(String.format("MyPipelineTransformLog - Inside TransformFn - inputRow values: %s", inputRow.values()));
        TableRow transformedRow = new TableRow();
        if (schema.isAccessible()) {
            LOG.info("MyPipelineTransformLog - Inside TransformFn.. inputSchema - " + schema);
            String[] fields = getFields(schema);
            LOG.info("MyPipelineTransformLog - Inside TransformFn - fields from schema string split('\\|') - " + Arrays.toString(fields));
            transformedRow = getTransformedRow(inputRow, fields);
        }
        outputReceiver.output(transformedRow);
    }
}
