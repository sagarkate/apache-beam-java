import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class BeamMapElementsWithTypeDescriptors {
    public static void main(String[] args) {

        BeamTextFileReaderWriterOptions_IMPL pipelineOptions =
                PipelineOptionsFactory
                        .fromArgs(args)
                        .withValidation()
                        .as(BeamTextFileReaderWriterOptions_IMPL.class);
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        PCollection<String> inputData = pipeline.apply(
                "Read Input File",
                TextIO.read().from(pipelineOptions.getInput()));

        PCollection<String> onlyData = inputData
                .apply(
                        "Skip Header",
                        Filter.by((String line) -> !line.toLowerCase().startsWith("sessionid")));

        PCollection<String> outputData = onlyData.apply(
                "Convert Names to UpperCase",
                MapElements.into(TypeDescriptors.strings())
                        .via((String line) -> line.split(",")[2].toUpperCase()));

        outputData.apply(
                "Write Data",
                TextIO.write()
                        .to(pipelineOptions.getOutput())
                        .withSuffix(".CSV")
                        .withHeader("username")
                        .withNumShards(1));

        pipeline.run().waitUntilFinish();

    }
}

// Set arguments in Run Configuration of IDE in below fashion
// --input="C:\\code\\gcp_code\\dataflow_java\\apache_beam_java_input\\user_map_elements.csv" --output="C:\\code\\gcp_code\\dataflow_java\\apache_beam_java_output\\user_map_elements_typedescriptors_output"