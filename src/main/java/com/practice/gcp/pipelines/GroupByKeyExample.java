package com.practice.gcp.pipelines;

import com.practice.gcp.options.TextFileOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

public class GroupByKeyExample {
    public static void main(String[] args) {
        PipelineOptionsFactory.register(TextFileOptions.class);
        TextFileOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(TextFileOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> rawInput = pipeline.apply("Read Lines", TextIO.read().from(options.getInput()));

        PCollection<KV<String, Long>> wordLineNumberPair = rawInput.apply(
                "Split line into key value pair",
                ParDo.of(new SplitLinesFn()));

        PCollection<KV<String, Iterable<Long>>> groupByKeyOutput = wordLineNumberPair.apply(
                "Group By Key",
                GroupByKey.create());

        PCollection<String> formattedOutput = groupByKeyOutput.apply(
                "Format GroupByKey Output",
                ParDo.of(new FormatOutputFn()));

        PDone result = formattedOutput.apply(
                "Write Output to TextFile",
                TextIO.write()
                        .to(options.getOutput())
                        .withSuffix(".txt")
                        .withNumShards(1));

        pipeline.run().waitUntilFinish();
    }

    public static class SplitLinesFn extends DoFn<String, KV<String, Long>> {
        @ProcessElement
        public void processElement(@Element String line, OutputReceiver<KV<String, Long>> out) {
            String[] lineSplits = line.split(",");
            out.output(KV.of(lineSplits[0], Long.parseLong(lineSplits[1].strip())));
        }
    }

    public static class FormatOutputFn extends DoFn<KV<String, Iterable<Long>>, String> {
        @ProcessElement
        public void processElement(@Element KV<String, Iterable<Long>> groupedKvPair, OutputReceiver<String> out) {
            out.output(groupedKvPair.getKey() + ":" + groupedKvPair.getValue());
        }
    }
}

/*
* Input:
cat, 1
dog, 5
and, 1
jump, 3
tree, 2
cat, 5
dog, 2
and, 2
cat, 9
and, 6

* Output:
jump:[3]
tree:[2]
dog:[2, 5]
and:[6, 1, 2]
cat:[5, 9, 1]
* */