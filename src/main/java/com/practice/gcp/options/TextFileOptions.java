package com.practice.gcp.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface TextFileOptions extends PipelineOptions {
    @Description("Input File Path")
    @Default.String("gs://mybucket/input")
    ValueProvider<String> getInput();
    void setInput(ValueProvider<String> input);

    @Description("Output File Path")
    @Default.String("gs://mybucket/output")
    ValueProvider<String> getOutput();
    void setOutput(ValueProvider<String> output);
}
