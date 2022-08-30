import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

class FilterDoFn extends DoFn<String, String> {
    String header;
    public FilterDoFn(String header) {
        this.header = header;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        String line = c.element();
        if (line != null && !line.toLowerCase().equals(this.header)) {
            String city = line.split(",")[3];
            if (city.equals("Los Angeles")) {
                c.output(line);
            }
        }
    }
}

public class BeamPardoFilter {
    public static void main(String[] args) {
        BeamTextFileReaderWriterOptions_IMPL pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(BeamTextFileReaderWriterOptions_IMPL.class);
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        String HEADER = "id,name,last name,city";
        PCollection<String> rawInputData = pipeline.apply(
                "Read Raw Input",
                TextIO.read().from(pipelineOptions.getInput()));

        PCollection<String> losAngelesCustomers = rawInputData.apply(
                "Fetch only Los Angeles Customers",
                ParDo.of(new FilterDoFn(HEADER)));

        losAngelesCustomers.apply(
                "Write Output to CSV",
                TextIO
                        .write()
                        .withHeader(HEADER)
                        .to(pipelineOptions.getOutput())
                        .withSuffix(pipelineOptions.getSuffix())
                        .withNumShards(1));

        pipeline.run().waitUntilFinish();
    }
}
// Set arguments in Run Configuration of IDE in below fashion
// --input="C:\\code\\gcp_code\\dataflow_java\\apache_beam_java_input\\customer_pardo.csv" --output="C:\\code\\gcp_code\\dataflow_java\\apache_beam_java_output\\customer_pardo_filter_output" --suffix=".csv"