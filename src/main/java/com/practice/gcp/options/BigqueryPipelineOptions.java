package com.practice.gcp.options;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ValueProvider;

public interface BigqueryPipelineOptions extends BigQueryOptions {
    @Description("Fully Qualified Name of Input BigQuery Table in format : <project-id>:<dataset-id>.<table-name> ")
    ValueProvider<String> getInput();
    void setInput(ValueProvider<String> input);

    @Description("SQL Query to read from, will be used if Input is not set.")
    ValueProvider<String> getInputQuery();
    void setInputQuery(ValueProvider<String> value);

    @Description(
            "BigQuery table to write to, specified as "
                    + "<project_id>:<dataset_id>.<table_id>. The dataset must already exist.")
    ValueProvider<String> getOutput();
    void setOutput(ValueProvider<String> value);

    @Description("Pipe Separated Schema e.g. id:INTEGER|name:STRING")
    ValueProvider<String> getSchema();
    void setSchema(ValueProvider<String> schema);

    @Description("Resources path for Schema Json File")
    ValueProvider<String> getSchemaGcsPath();
    void setSchemaGcsPath(ValueProvider<String> schemaGcsPath);

    @Description("Partition Column Name")
    ValueProvider<String> getPartitionColumn();
    void setPartitionColumn(ValueProvider<String> partitionColumn);

}
