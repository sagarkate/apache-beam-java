import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface BeamTextFileReaderWriterOptions_IMPL extends PipelineOptions {
    @Description("Input File Path")
    ValueProvider<String> getInput();
    void setInput(ValueProvider<String> input);

    ValueProvider<String> getOutput();
    void setOutput(ValueProvider<String> output);
}
