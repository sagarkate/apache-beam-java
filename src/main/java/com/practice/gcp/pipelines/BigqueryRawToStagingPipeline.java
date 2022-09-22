package com.practice.gcp.pipelines;

import com.practice.gcp.options.BigqueryPipelineOptions;
import com.practice.gcp.transform.BigqueryPartitionWriterFn;
import com.practice.gcp.transform.BigqueryRawToStagingTransformFnWithGcs;
import com.practice.gcp.utils.SchemaParser;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

public class BigqueryRawToStagingPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(BigqueryRawToStagingPipeline.class);

    public static void main(String[] args) {
        PipelineOptionsFactory.register(BigqueryPipelineOptions.class);
        BigqueryPipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(BigqueryPipelineOptions.class);
        Pipeline p = Pipeline.create(pipelineOptions);

        LOG.info("MyPipelineTransformLog - main() - pipelineOptions: \n" + pipelineOptions);

        PCollection<TableRow> rawInput = p.apply(
                "Read from Raw Input Table",
                BigQueryIO
                        .readTableRows()
//                        .from(pipelineOptions.getInput())
                        .fromQuery(pipelineOptions.getInputQuery())
                        .usingStandardSql()
                        .withTemplateCompatibility()
                        .withoutValidation()
        );

        PCollection<TableRow> transformedData = rawInput.apply(
                "Transform Raw Input",
                ParDo.of(new BigqueryRawToStagingTransformFnWithGcs(pipelineOptions.getSchemaGcsPath()))
        );

        transformedData.apply(
                BigQueryIO
                        .writeTableRows()
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                        .to(new BigqueryPartitionWriterFn(pipelineOptions.getOutput(), pipelineOptions.getPartitionColumn()))
                        .withSchema(NestedValueProvider.of(pipelineOptions.getSchemaGcsPath(), new SerializableFunction<String, TableSchema>() {
                            @Override
                            public TableSchema apply(String schemaGcsPath) {
                                LOG.info("MyPipelineTransformLog - Input schema Path: " + schemaGcsPath);
                                SchemaParser schemaParser = new SchemaParser();
                                TableSchema tableSchema = schemaParser.getTableSchema(schemaGcsPath);
                                LOG.info("MyPipelineTransformLog - Returned Table Schema - " + tableSchema);
                                return tableSchema;
                            }
                        }))


        );

        try {
            p.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
