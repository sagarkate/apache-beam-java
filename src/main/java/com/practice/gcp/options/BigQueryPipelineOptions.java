package com.practice.gcp.options;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface BigQueryPipelineOptions extends PipelineOptions, BigQueryOptions {
    @Description("Fully Qualified Name of Input BigQuery Table in format : <project-id>:<dataset-id>.<table-name> ")
    void setInput(ValueProvider<String> input);
    ValueProvider<String> getInput();

    @Description("SQL Query to read from, will be used if Input is not set.")
    @Default.String("")
    ValueProvider<String> getInputQuery();
    void setInputQuery(ValueProvider<String> value);

    @Description("Read method to use to read from BigQuery")
    @Default.Enum("EXPORT")
    BigQueryIO.TypedRead.Method getReadMethod();
    void setReadMethod(BigQueryIO.TypedRead.Method value);

    @Description("Write method to use to write to BigQuery")
    @Default.Enum("DEFAULT")
    BigQueryIO.Write.Method getWriteMethod();
    void setWriteMethod(BigQueryIO.Write.Method value);

    @Description("Write disposition to use to write to BigQuery")
    @Default.Enum("WRITE_APPEND")
    BigQueryIO.Write.WriteDisposition getWriteDisposition();
    void setWriteDisposition(BigQueryIO.Write.WriteDisposition value);

    @Description("Create disposition to use to write to BigQuery")
    @Default.Enum("CREATE_IF_NEEDED")
    BigQueryIO.Write.CreateDisposition getCreateDisposition();
    void setCreateDisposition(BigQueryIO.Write.CreateDisposition value);

    @Description(
            "BigQuery table to write to, specified as "
                    + "<project_id>:<dataset_id>.<table_id>. The dataset must already exist.")
    ValueProvider<String> getOutput();
    void setOutput(ValueProvider<String> value);

    @Description("Resources path for Schema Json File")
    String getSchemaJsonPath();
    void setSchemaJsonPath(String schemaJsonPath);
}
