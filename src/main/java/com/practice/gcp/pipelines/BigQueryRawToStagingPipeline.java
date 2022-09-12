package com.practice.gcp.pipelines;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.practice.gcp.options.BigQueryPipelineOptions;
import com.practice.gcp.utils.SchemaParser;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
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
import java.util.ArrayList;
import java.util.List;

public class BigQueryRawToStagingPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryRawToStagingPipeline.class);
    private static final SchemaParser schemaParser = new SchemaParser();

    public static void setTableRowField(TableRow inputRow, TableRow outputRow, String fieldName, String fieldType) {
        if (fieldType.equals("INTEGER") || fieldType.equals("LONG")) {
            outputRow.set(fieldName, Long.parseLong((String) inputRow.get(fieldName)));
        } else {
            outputRow.set(fieldName, inputRow.get(fieldName));
        }
    }

    public static void getOutputRow(TableRow inputRow, TableRow outputRow, JSONArray jsonArray) {
        for(Object o: jsonArray) {
            JSONObject field = (JSONObject) o;
            String fieldName = (String) field.get("name");
            String fieldType = (String) field.get("type");
            setTableRowField(inputRow, outputRow, fieldName, fieldType);
        }

    }

    public static List<String> getSelectedFields(JSONArray jsonArray) {
        List<String> selectedFields = new ArrayList<>();
        for(Object o: jsonArray) {
            selectedFields.add((String) ((JSONObject) o).get("name"));
        }

        return selectedFields;
    }

    public static class FormatRowsFn extends DoFn<TableRow, TableRow> {
        JSONArray jsonArray;
        public FormatRowsFn(JSONArray jsonArray) {
            this.jsonArray = jsonArray;
        }

        @ProcessElement
        public void processElement(@Element TableRow inputRow, OutputReceiver<TableRow> outputRow) {
            TableRow transformedRow = new TableRow();
            getOutputRow(inputRow, transformedRow, jsonArray);
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

    public static void createPipeline(Pipeline pipeline, BigQueryPipelineOptions pipelineOptions) throws IOException, ParseException {
        // Create Schema
        TableSchema schema = schemaParser.parse(pipelineOptions.getSchemaJsonPath());
        JSONArray jsonArray = schemaParser.getJsonSimpleArray(schemaParser.getJsonSchema(pipelineOptions.getSchemaJsonPath()));
        List<String> selectedFields = getSelectedFields(jsonArray);

        BigQueryIO.TypedRead<TableRow> rawInputIO = BigQueryIO
                .readTableRows()
                .from(pipelineOptions.getInput())
                .withMethod(pipelineOptions.getReadMethod()) //TypedRead.Method.DIRECT_READ
                .withSelectedFields(selectedFields)
                .withTemplateCompatibility()
                .withoutValidation()
                ;

        PCollection<TableRow> rawInput = pipeline.apply(rawInputIO);
        PCollection<TableRow> transformedData = rawInput.apply(new TransformFn(jsonArray));

        transformedData.apply(
                BigQueryIO
                        .writeTableRows()
                        .to(pipelineOptions.getOutput()) // <projet-id>:<dataset-id>.<table-name>
                        .withSchema(schema)
                        .withCreateDisposition(pipelineOptions.getCreateDisposition()) // CREATE_NEVER
                        .withWriteDisposition(pipelineOptions.getWriteDisposition()) // WRITE_APPEND
                        .withMethod(pipelineOptions.getWriteMethod()) // DEFAULT
        );
    }

    public static void runPipeline(BigQueryPipelineOptions pipelineOptions) throws IOException, ParseException {
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
        PipelineOptionsFactory.register(BigQueryPipelineOptions.class);
        BigQueryPipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(BigQueryPipelineOptions.class);
        runPipeline(pipelineOptions);
    }
}
