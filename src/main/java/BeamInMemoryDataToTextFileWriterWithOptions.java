import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.ArrayList;

public class BeamInMemoryDataToTextFileWriterWithOptions {
    public static void main(String[] args) {
        BeamInMemoryDataToTextFileWriterOptions_IMPL pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(BeamInMemoryDataToTextFileWriterOptions_IMPL.class);
        Pipeline p = Pipeline.create(pipelineOptions);
        PCollection<Customer> customerPcoll = p.apply("Read from Customer List", Create.of(getCustomers()));
        PCollection<String> customerNamesPcoll = customerPcoll.apply(
                MapElements
                        .into(TypeDescriptors.strings())
                        .via((Customer c) -> c.getName()));
        customerNamesPcoll.apply(TextIO.write().to(pipelineOptions.getOutput()).withSuffix(pipelineOptions.getSuffix()).withNumShards(1));
//        customerNamesPcoll.apply("Print", ParDo.of(new PrintElementFn()));
        p.run().waitUntilFinish();
    }

    private static ArrayList<Customer> getCustomers() {
        ArrayList<Customer> customerList = new ArrayList<>();
        Customer customer1 = new Customer("1001", "Sagar");
        Customer customer2 = new Customer("1002", "John");
        customerList.add(customer1);
        customerList.add(customer2);
        return customerList;
    }

    public static class PrintElementFn extends DoFn<String,Void> {
        @ProcessElement
        public void processElement(@Element String name) {
            System.out.println(name);
        }
    }
}

/*
* IntelliJ Configuration Arguments:
* --output="C:\\code\\gcp_code\\dataflow_java\\apache_beam_java_output\\customer_names" --suffix=".txt"
* */