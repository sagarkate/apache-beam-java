import org.apache.beam.sdk.options.PipelineOptions;

public interface BeamTextFileReaderWriterOptions_IMPL extends PipelineOptions {
    String getInput();
    void setInput(String input);

    String getOutput();
    void setOutput(String output);

    String getSuffix();
    void setSuffix(String suffix);
}
