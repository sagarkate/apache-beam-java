package com.practice.gcp.transform;

import java.math.BigDecimal;
import java.sql.Time;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import com.google.api.services.bigquery.model.TableRow;

import com.practice.gcp.utils.SchemaParser;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class BigqueryRawToStagingTransformFnWithGcs extends DoFn<TableRow, TableRow> {
    private static final String RECORD_LOAD_TS = "record_load_ts";
    private static final Logger LOG = LoggerFactory.getLogger(BigqueryRawToStagingTransformFnWithGcs.class);
    ValueProvider<String> schemaGcsPath;

    public BigqueryRawToStagingTransformFnWithGcs(ValueProvider<String> schemaGcsPath) {
        this.schemaGcsPath = schemaGcsPath;
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
                case "TIME":
                    transformedRow = outputRow.set(fieldName, Time.valueOf((String) inputRow.get(fieldName)).toString());
                    break;
                default:
                    throw new Exception(String.format("Data type %s of field %s is not supported.. Please use one of the below data types: \n [INTEGER|LONG|DATE|DATETIME|TIMESTAMP|STRING|BOOLEAN|FLOAT|NUMERIC]", fieldType, fieldName));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return transformedRow;
    }

    public TableRow setFields(TableRow inputRow, JSONArray schemaJsonArray) {
        TableRow transformedRow = new TableRow();
        for (Object o : schemaJsonArray) {
            JSONObject field = (JSONObject) o;
            String fieldName = (String) field.get("name");
            String fieldType = (String) field.get("type");
            LOG.info(String.format("MyPipelineTransformLog - Inside TransformFn - fieldName : %s, fieldType: %s ", fieldName, fieldType));
            LOG.info(String.format("MyPipelineTransformLog - Inside TransformFn - inputRow field value: %s", (String) inputRow.get(fieldName)));

            if (fieldName.equals(RECORD_LOAD_TS)) {
                transformedRow.set(RECORD_LOAD_TS, DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(Instant.now().toString(), Instant::from).toString());
            } else {
                transformedRow = setField(transformedRow, inputRow, fieldType, fieldName);
            }
            LOG.info(String.format("MyPipelineTransformLog - Inside TransformFn - transformedRow keys: %s", transformedRow.keySet()));
            LOG.info(String.format("MyPipelineTransformLog - Inside TransformFn - transformedRow values: %s", transformedRow.values()));
        }

        return transformedRow;
    }

    public TableRow getTransformedRow(TableRow inputRow, ValueProvider<String> schemaGcsPath) {
        JSONArray schemaJsonArray = new JSONArray();
        SchemaParser schemaParser = new SchemaParser();
        try {
            schemaJsonArray = schemaParser.getSchemaJsonArray(schemaGcsPath);
        } catch (Exception e) {
            LOG.info("MyPipelineTransformLog - Inside TransformFn - getTransformedRow Exception Catch Block - " + e.getMessage());
            e.printStackTrace();
        }
        TableRow transformedRow = setFields(inputRow, schemaJsonArray);

        return transformedRow;
    }

    @ProcessElement
    public void processElement(@Element TableRow inputRow, ProcessContext c, OutputReceiver<TableRow> outputReceiver) {
        LOG.info("MyPipelineTransformLog - Inside TransformFn - ");
        LOG.info(String.format("MyPipelineTransformLog - Inside TransformFn - inputRow keys: %s", inputRow.keySet()));
        LOG.info(String.format("MyPipelineTransformLog - Inside TransformFn - inputRow values: %s", inputRow.values()));
        TableRow transformedRow = new TableRow();
        if (schemaGcsPath.isAccessible()) {
            LOG.info("MyPipelineTransformLog - Inside TransformFn.. inputSchema Path - " + schemaGcsPath);
            transformedRow = getTransformedRow(inputRow, schemaGcsPath);
        }
        outputReceiver.output(transformedRow);
    }
}
