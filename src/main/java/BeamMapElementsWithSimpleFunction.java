import com.practice.gcp.options.TextFileOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;

class UserSimpleFn extends SimpleFunction<String, String> {
    @Override
    public String apply(String input) {
        String[] userRecord = input.split(",");
        String sessionId = userRecord[0];
        String userId = userRecord[1];
        String userName = userRecord[2];
        String videoId = userRecord[3];
        String duration = userRecord[4];
        String startTime = userRecord[5];
        String gender = userRecord[6];

        if (gender.equals("1")) {
            gender = "M";
        } else if (gender.equals("2")) {
            gender = "F";
        }

//        return Arrays
//                .stream(new String[]{sessionId, userId, userName, videoId, duration, startTime, gender})
//                .reduce("", (result, field) -> result + "," + field); -- It returns the row with comma prefix

        // a better solution
        return String.join(
                ",",
                Arrays.asList(sessionId, userId, userName, videoId, duration, startTime, gender));
    }
}

public class BeamMapElementsWithSimpleFunction {
    public static void main(String[] args) {
        TextFileOptions pipelineOptions =
                PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(TextFileOptions.class);
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        String HEADER = "sessionid,userid,username,videoid,duration,starttime,sex";
        PCollection<String> inputRawData = pipeline
                .apply(
                        "Read Input CSV",
                        TextIO.read().from(pipelineOptions.getInput()));

        PCollection<String> dataWithoutHeader = inputRawData
                .apply(
                        "Skip Header",
                        Filter.by((String line) -> !line.toLowerCase().startsWith("sessionid")));

        PCollection<String> outputData = dataWithoutHeader
                .apply(
                        "Transform gender values",
                        MapElements.via(new UserSimpleFn()));

        outputData.apply(
                "Write Output to CSV File",
                TextIO
                        .write()
                        .to(pipelineOptions.getOutput())
                        .withSuffix(".CSV")
                        .withHeader(HEADER)
                        .withNumShards(1));

        pipeline.run().waitUntilFinish();
    }
}

// Set arguments in Run Configuration of IDE in below fashion
// --input="C:\\code\\gcp_code\\dataflow_java\\apache_beam_java_input\\user_map_elements.csv" --output="C:\\code\\gcp_code\\dataflow_java\\apache_beam_java_output\\user_map_elements_simple_function_output"