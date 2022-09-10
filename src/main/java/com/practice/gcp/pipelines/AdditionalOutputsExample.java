package com.practice.gcp.pipelines;

import com.practice.gcp.options.TextFileOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import java.util.Objects;
import java.util.function.LongConsumer;

public class AdditionalOutputsExample {
    public static void main(String[] args) {
        PipelineOptionsFactory.register(TextFileOptions.class);
        TextFileOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(TextFileOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        long CUT_OFF_LENGTH = 5;
        String MARKER_CHARACTER = "s";

        // Since our output is KV<String, Long>, we need TupleTag<KV<String, Long>>
        TupleTag<KV<String, Iterable<Long>>> wordsBelowCutOffTag = new TupleTag<>();
        TupleTag<KV<String, Iterable<Long>>> wordsAboveCutOffTag = new TupleTag<>();
        TupleTag<KV<String, Iterable<Long>>> wordsStartWithMarkerCharacterTag = new TupleTag<>();

        PCollection<String> rawInput = pipeline.apply("Read Lines", TextIO.read().from(options.getInput()));

        PCollection<KV<String, Long>> wordLineNumberPairs = rawInput.apply(
                "Split line into key value pair",
                ParDo.of(new SideInputExample.SplitLinesFn()));

        PCollection<KV<String, Iterable<Long>>> groupByKeyOutput = wordLineNumberPairs.apply(
                "Group By Key",
                GroupByKey.create());

        PCollectionTuple wordPCollections = groupByKeyOutput.apply(
                "Get multiple outputs",
                ParDo.of(new MultipleOutputsFn(
                                CUT_OFF_LENGTH,
                                MARKER_CHARACTER,
                                wordsBelowCutOffTag,
                                wordsAboveCutOffTag,
                                wordsStartWithMarkerCharacterTag))
                        .withOutputTags(
                                wordsBelowCutOffTag,
                                TupleTagList.of(wordsAboveCutOffTag)
                                        .and(wordsStartWithMarkerCharacterTag)));

        PCollection<String> wordsBelowCutOffTagFormattedOutput = wordPCollections
                .get(wordsBelowCutOffTag)
                .setCoder(KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(BigEndianLongCoder.of())))
                .apply(
                        "wordsBelowCutOffTag - Format Output",
                        ParDo.of(new FormatOutputFn()));

        PDone wordsBelowCutOffTagResult = wordsBelowCutOffTagFormattedOutput.apply(
                "wordsBelowCutOffTag - Write Output to TextFile",
                TextIO.write()
                        .to(options.getOutput() + "-wordsBelowCutOffTag")
                        .withSuffix(".txt")
                        .withNumShards(1));

        PCollection<String> wordsAboveCutOffTagFormattedOutput = wordPCollections
                .get(wordsAboveCutOffTag)
                .setCoder(KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(BigEndianLongCoder.of())))
                .apply(
                         "wordsAboveCutOffTag - Format Output",
                        ParDo.of(new FormatOutputFn()));

        PDone wordsAboveCutOffTagResult = wordsAboveCutOffTagFormattedOutput.apply(
                "wordsAboveCutOffTag - Write Output to TextFile",
                TextIO.write()
                        .to(options.getOutput() + "-wordsAboveCutOffTag")
                        .withSuffix(".txt")
                        .withNumShards(1));

        PCollection<String> wordsStartWithMarkerCharacterTagFormattedOutput = wordPCollections
                .get(wordsStartWithMarkerCharacterTag)
                .setCoder(KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(BigEndianLongCoder.of())))
                .apply(
                         "wordsStartWithMarkerCharacterTag - Format Output",
                        ParDo.of(new FormatOutputFn()));

        PDone wordsStartWithMarkerCharacterTagResult = wordsStartWithMarkerCharacterTagFormattedOutput
                .apply(
                "wordsStartWithMarkerCharacterTag - Write Output to TextFile",
                        TextIO.write()
                                .to(options.getOutput() + "-wordsStartWithMarkerCharacterTag")
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

    public static class MultipleOutputsFn extends DoFn<KV<String, Iterable<Long>>, KV<String, Iterable<Long>>> {
        Long cutOffLength;
        String markerCharacter;
        TupleTag<KV<String, Iterable<Long>>> wordsBelowCutOffTag;
        TupleTag<KV<String, Iterable<Long>>> wordsAboveCutOffTag;
        TupleTag<KV<String, Iterable<Long>>> wordsStartWithMarkerCharacterTag;

        public MultipleOutputsFn(
                Long cutOffLength,
                String markerCharacter,
                TupleTag<KV<String, Iterable<Long>>> wordsBelowCutOffTag,
                TupleTag<KV<String, Iterable<Long>>> wordsAboveCutOffTag,
                TupleTag<KV<String, Iterable<Long>>> wordsStartWithMarkerCharacterTag) {
            this.cutOffLength = cutOffLength;
            this.markerCharacter = markerCharacter;
            this.wordsBelowCutOffTag = wordsBelowCutOffTag;
            this.wordsAboveCutOffTag = wordsAboveCutOffTag;
            this.wordsStartWithMarkerCharacterTag = wordsStartWithMarkerCharacterTag;
        }

        @ProcessElement
        public void processElement(@Element KV<String, Iterable<Long>> inputPair, MultiOutputReceiver out) {
            if (Objects.requireNonNull(inputPair.getKey()).length() < cutOffLength) {
                out.get(wordsBelowCutOffTag).output(inputPair);
            }
            else {
                out.get(wordsAboveCutOffTag).output(inputPair);
            }

            // Since we have already checked for Null value of inputPair.getKey(),
            // we don't need to check again.
            if (inputPair.getKey().toLowerCase().startsWith(markerCharacter)) {
                out.get(wordsStartWithMarkerCharacterTag).output(inputPair);
            }
        }
    }

}

// Errors:
/*
* Error1: Unable to return a default Coder
*
    Exception in thread "main" java.lang.IllegalStateException: Unable to return a default Coder for Get multiple outputs.out2 [PCollection@248483913]. Correct one of the following root causes:
    No Coder has been manually specified;  you may do so using .setCoder().
    Inferring a Coder from the CoderRegistry failed: Unable to provide a Coder for V.
    Building a Coder using a registered CoderProvider failed.
    See suppressed exceptions for detailed failures.
    Using the default output Coder from the producing PTransform failed: PTransform.getOutputCoder called.
*
* Solution: Set Coder after fetching PCollection from PCollectionTuple
*   wordPCollections.get(wordsStartWithMarkerCharacterTag)
*                   .setCoder(KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(BigEndianLongCoder.of())))
* */

/*
* Output:
* additionaloutputs_example_output-wordsAboveCutOffTag-00000-of-00001
    shark:[2]
    dinosaur:[1]
    sherlock:[1]
    squirrel:[2]
    tiger:[5, 8]
    elephant:[7, 10]

* additionaloutputs_example_output-wordsBelowCutOffTag-00000-of-00001
    tree:[2]
    and:[6, 1, 2]
    dog:[5, 2]
    jump:[3]
    cat:[1, 5, 9]

* additionaloutputs_example_output-wordsStartWithMarkerCharacterTag-00000-of-00001
    squirrel:[2]
    sherlock:[1]
    shark:[2]

* */