package com.practice.gcp.pipelines;

import com.practice.gcp.options.TextFileOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

public class CompositePTransformExample {
    public static void main(String[] args) {
        PipelineOptionsFactory.register(TextFileOptions.class);
        TextFileOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(TextFileOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> rawInput = pipeline.apply(
                "Read Lines",
                TextIO.read().from(options.getInput()));

        PCollection<String> formattedWordCountOutput = rawInput.apply(
                "Get Formatted Word Count Output",
                new WordCounter());

        PDone result = formattedWordCountOutput.apply(
                "Write to Output",
                TextIO.write().to(options.getOutput()).withSuffix(".txt").withNumShards(1));

        pipeline.run().waitUntilFinish();
    }

    public static class SplitLinesFn extends DoFn<String, KV<String, Long>> {
        @ProcessElement
        public void processElement(@Element String line, OutputReceiver<KV<String, Long>> out) {
            String[] lineSplits = line.split(",");
            out.output(KV.of(lineSplits[0], Long.parseLong(lineSplits[1].strip())));
        }
    }

    public static class FormatOutputFn extends DoFn<KV<String, Long>, String> {
        @ProcessElement
        public void processElement(@Element KV<String, Long> groupedKvPair, OutputReceiver<String> out) {
            out.output(groupedKvPair.getKey() + ":" + groupedKvPair.getValue());
        }
    }

    public static class WordCounter extends PTransform<PCollection<String>, PCollection<String>> {

        @Override
        public PCollection<String> expand(PCollection<String> input) {
            PCollection<KV<String, Long>> wordIndividualCount =
                    input.apply(
                            "Split the lines",
                            ParDo.of(new SplitLinesFn()));

            PCollection<KV<String, Long>> wordTotalCount =
                    wordIndividualCount.apply(
                            "Get Total Count",
                            Sum.longsPerKey());

            PCollection<String> formattedOutput = wordTotalCount.apply(
                    "Format Output",
                    ParDo.of(new FormatOutputFn()));

            return formattedOutput;
        }
    }
}
