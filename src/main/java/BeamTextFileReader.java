import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class BeamTextFileReader {

    static class FilterCustomer extends DoFn<String, String> {
        String customer_id;

        public FilterCustomer(String customer_id) {
            this.customer_id = customer_id;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String line = c.element();
            // line != null condition is used to handle NullPointerException
            // !line.startsWith("#") condition is used to skip header record
            if (line != null && !line.startsWith("#") && line.startsWith(customer_id)) {
                c.output(line);
            }
        }
    }

    // If you make below interface private, it raises IllegalArgumentException
    //    java.lang.IllegalArgumentException: Please mark non-public interface Beam TextFileReader$Options as public
    public interface Options extends PipelineOptions {
        @Description("Input CSV File Path")
        String getInput();
        void setInput(String value);

        @Description("Output CSV File Path")
        String getOutput();
        void setOutput(String value);

        @Description("ID of the customer whose data needs to be fetched")
        String getCustomerId();
        void setCustomerId(String value);
    }

    public static void main(String[] args) {
        Options pipeline_options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline pipeline = Pipeline.create(pipeline_options);
        PCollection<String> lines = pipeline.apply("Read from TextFile", TextIO.read().from(pipeline_options.getInput()));
        PCollection<String> onlyData = lines.apply("Filter Customer", ParDo.of(new FilterCustomer(pipeline_options.getCustomerId())));

        onlyData.apply(
                TextIO.write()
                        .to(pipeline_options.getOutput())
                        .withNumShards(1)
                        .withSuffix(".csv"));

        pipeline.run().waitUntilFinish();
    }
}

// Set arguments in Run Configuration of IntelliJ IDEA in below fashion
//--input="C:\\data\\input\\cards.txt" --output="C:\\data\\output\\cards_customer_output" --customerId="CT42260"