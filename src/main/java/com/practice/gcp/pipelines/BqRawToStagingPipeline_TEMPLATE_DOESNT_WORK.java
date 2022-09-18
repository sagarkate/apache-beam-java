package com.practice.gcp.pipelines;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.practice.gcp.options.BqToBqPipelineOptions_TEMPLATE_DOESNT_WORK;
import com.practice.gcp.utils.SchemaParser_TEMPLATE_DOESNT_WORK;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class BqRawToStagingPipeline_TEMPLATE_DOESNT_WORK {

    private static final Logger LOG = LoggerFactory.getLogger(BqRawToStagingPipeline_TEMPLATE_DOESNT_WORK.class);
    private static final SchemaParser_TEMPLATE_DOESNT_WORK schemaParser = new SchemaParser_TEMPLATE_DOESNT_WORK();
    private static final String RECORD_LOAD_TS = "record_load_ts";

    public static void setField(TableRow inputRow, TableRow outputRow, String fieldName, String fieldType) {
        if (fieldType.equals("INTEGER") || fieldType.equals("LONG")) {
            outputRow.set(fieldName, Long.parseLong((String) inputRow.get(fieldName)));
        } else if (fieldType.equals("NUMERIC")) {
            outputRow.set(fieldName, new BigDecimal((String) inputRow.get(fieldName)).toString());
        } else if (fieldType.equals("DATE")) {
            outputRow.set(fieldName, LocalDate.parse((String) inputRow.get(fieldName)).toString());
        } else if (fieldType.equals("DATETIME")) {
            outputRow.set(fieldName, LocalDateTime.parse((String) inputRow.get(fieldName)).toString());
        } else if (fieldType.equals("TIMESTAMP")) {
//            outputRow.set(fieldName, Instant.parse((String) inputRow.get(fieldName)).toString());
            // This doesn't work.
            // It causes below error:
//            org.apache.beam.sdk.Pipeline$PipelineExecutionException: java.time.format.DateTimeParseException: Text '2022-05-23T13:25:34.123+05:30' could not be parsed at index 23
            outputRow.set(fieldName, DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse((String) inputRow.get(fieldName), Instant::from).toString());
        } else if (fieldType.equals("BOOLEAN")) {
            outputRow.set(fieldName, Boolean.parseBoolean((String) inputRow.get(fieldName)));
        } else if (fieldType.equals("FLOAT")) {
            outputRow.set(fieldName, Double.parseDouble((String) inputRow.get(fieldName)));
        }
        else {
            outputRow.set(fieldName, inputRow.get(fieldName));
        }
    }

    public static void getTransformedRow(TableRow inputRow, TableRow outputRow, JSONArray jsonArray) {
        for(Object o: jsonArray) {
            JSONObject field = (JSONObject) o;
            String fieldName = (String) field.get("name");
            String fieldType = (String) field.get("type");
            if (fieldName.equals(RECORD_LOAD_TS)) {
                outputRow.set(RECORD_LOAD_TS, DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(Instant.now().toString(), Instant::from).toString());
            } else {
                setField(inputRow, outputRow, fieldName, fieldType);
            }
        }

    }

    public static List<String> getProjectedColumns(JSONArray jsonArray) {
        List<String> projectedColumns = new ArrayList<>();
        for(Object o: jsonArray) {
            String column = (String) ((JSONObject) o).get("name");
            if (!column.equals(RECORD_LOAD_TS)) {
                projectedColumns.add((String) ((JSONObject) o).get("name"));
            }
        }

        return projectedColumns;
    }

    public static class FormatRowsFn extends DoFn<TableRow, TableRow> {
        JSONArray jsonArray;
        public FormatRowsFn(JSONArray jsonArray) {
            this.jsonArray = jsonArray;
        }

        @ProcessElement
        public void processElement(@Element TableRow inputRow, OutputReceiver<TableRow> outputRow) {
            TableRow transformedRow = new TableRow();
            getTransformedRow(inputRow, transformedRow, jsonArray);
            outputRow.output(transformedRow);
        }
    }

    public static class TransformFn extends PTransform<PCollection<TableRow>, PCollection<TableRow>> {
        JSONArray jsonArray;

        public TransformFn(JSONArray jsonArray) {
            this.jsonArray = jsonArray;
        }

        @Override
        public PCollection<TableRow> expand(PCollection<TableRow> inputRows) {
            return inputRows.apply(
                    "Convert Data Type",
                    ParDo.of(new FormatRowsFn(jsonArray))
            );
        }
    }

    public static void createPipeline(Pipeline pipeline, BqToBqPipelineOptions_TEMPLATE_DOESNT_WORK pipelineOptions) throws IOException, ParseException {
        TableSchema schema = schemaParser.parse(pipelineOptions.getSchemaJsonPath());
        JSONArray jsonArray = schemaParser.toJsonArray(schemaParser.toJsonString(pipelineOptions.getSchemaJsonPath()));
        List<String> projectedColumns = getProjectedColumns(jsonArray);

        BigQueryIO.TypedRead<TableRow> rawInputIO = BigQueryIO
                .readTableRows()
                .from(pipelineOptions.getInput())
                .withMethod(pipelineOptions.getReadMethod())
                .withSelectedFields(projectedColumns)
                .withTemplateCompatibility()
                .withoutValidation()
                ;

        PCollection<TableRow> rawInput = pipeline.apply(rawInputIO);

        PCollection<TableRow> transformedData = rawInput.apply(new TransformFn(jsonArray));

        WriteResult output = transformedData.apply(
                BigQueryIO
                        .writeTableRows()
                        .to(pipelineOptions.getOutput())
                        .withSchema(schema)
                        .withCreateDisposition(pipelineOptions.getCreateDisposition())
                        .withWriteDisposition(pipelineOptions.getWriteDisposition())
                        .withMethod(pipelineOptions.getWriteMethod())
        );
    }

    public static void runPipeline(BqToBqPipelineOptions_TEMPLATE_DOESNT_WORK pipelineOptions) throws IOException, ParseException {
        LOG.info("Running BigQuery Simple Pipeline with options " + pipelineOptions.toString());
        Pipeline pipeline = Pipeline.create(pipelineOptions);
        createPipeline(pipeline, pipelineOptions);
        PipelineResult pipelineResult = pipeline.run();

        try{
            pipelineResult.waitUntilFinish();
        } catch (UnsupportedOperationException e) {
            LOG.info("UnsupportedOperationException caught..");
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, ParseException {
        PipelineOptionsFactory.register(BqToBqPipelineOptions_TEMPLATE_DOESNT_WORK.class);
        BqToBqPipelineOptions_TEMPLATE_DOESNT_WORK pipelineOptions = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(BqToBqPipelineOptions_TEMPLATE_DOESNT_WORK.class);

        runPipeline(pipelineOptions);
    }
}
