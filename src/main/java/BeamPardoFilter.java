import com.practice.gcp.options.TextFileOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

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
        TextFileOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(TextFileOptions.class);
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        String HEADER = "id,name,last name,city";
        PCollection<String> rawInputData = pipeline.apply(
                "Read Raw Input",
                TextIO.read().from(pipelineOptions.getInput()));

        PCollection<String> losAngelesCustomers = rawInputData.apply(
                "Fetch only Los Angeles Customers",
                ParDo.of(new FilterDoFn(HEADER)));

        PDone customerOutput = losAngelesCustomers.apply(
                "Write Output to CSV",
                TextIO
                        .write()
                        .withHeader(HEADER)
                        .to(pipelineOptions.getOutput())
                        .withSuffix(".CSV")
                        .withNumShards(1));

        PipelineResult pipelineResult = pipeline.run();
        try{
            pipelineResult.waitUntilFinish();
        } catch (UnsupportedOperationException e) {
            System.out.println("UnsupportedOperationException caught..");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
// Set arguments in Run Configuration of IDE in below fashion
// --input="C:\\code\\gcp_code\\dataflow_java\\apache_beam_java_input\\customer_pardo.csv" --output="C:\\code\\gcp_code\\dataflow_java\\apache_beam_java_output\\customer_pardo_filter_output"

/*
* pipeline.run().waitUntilFinish(); is causing below error
* java.lang.UnsupportedOperationException: The result of template creation should not be used.
    at org.apache.beam.runners.dataflow.util.DataflowTemplateJob.getJobId (DataflowTemplateJob.java:40)
    at org.apache.beam.runners.dataflow.DataflowPipelineJob.getJobWithRetries (DataflowPipelineJob.java:555)
    at org.apache.beam.runners.dataflow.DataflowPipelineJob.getStateWithRetries (DataflowPipelineJob.java:536)
    at org.apache.beam.runners.dataflow.DataflowPipelineJob.waitUntilFinish (DataflowPipelineJob.java:320)
    at org.apache.beam.runners.dataflow.DataflowPipelineJob.waitUntilFinish (DataflowPipelineJob.java:249)
    at org.apache.beam.runners.dataflow.DataflowPipelineJob.waitUntilFinish (DataflowPipelineJob.java:208)
    at org.apache.beam.runners.dataflow.DataflowPipelineJob.waitUntilFinish (DataflowPipelineJob.java:202)
    at BeamPardoFilter.main (BeamPardoFilter.java:49)
*
* Solution: Add try-catch block
* */
