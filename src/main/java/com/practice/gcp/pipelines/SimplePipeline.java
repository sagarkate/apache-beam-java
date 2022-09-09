package com.practice.gcp.pipelines;

import com.practice.gcp.options.TextFileOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;
import java.util.List;

public class SimplePipeline {
    public static void main(String[] args) {
        PipelineOptionsFactory.register(TextFileOptions.class);
        TextFileOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(TextFileOptions.class);
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        List<String> lines = Arrays.asList(
                "This is first line",
                "This is second line",
                "This is 3rd line");

        /*
        * - The Coder specifies how the elements in the Collection should be encoded.
        * - Here, we are specifying that String should encoded using StringUtf8Coder.
        * - Beam needs to be able to encode each element as a byte string so that
        *   elements can be passed around to distributed workers.
        * */
        PCollection<String> linesPcoll = pipeline.apply("Create PCollection", Create.of(lines)).setCoder(StringUtf8Coder.of());
        linesPcoll.apply(
                "Print",
                ParDo.of(new PrintElementsFn()));

        pipeline.run().waitUntilFinish();
    }

    public static class PrintElementsFn extends DoFn<String, Void> {
        @ProcessElement
        public void processElement(@Element String line) {
            System.out.println(line);
        }
    }
}
