package com.practice.gcp.other_examples;

import org.apache.beam.sdk.options.PipelineOptions;

public interface BeamInMemoryDataToTextFileWriterOptions_IMPL extends PipelineOptions {

    String getOutput();
    void setOutput(String output);

    String getSuffix();
    void setSuffix(String suffix);
}
