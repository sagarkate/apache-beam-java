package com.practice.gcp.pipelines;

import com.practice.gcp.options.TextFileOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class CoGroupByKeyExample {
    public static void main(String[] args) {
        TextFileOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(TextFileOptions.class);
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        final List<KV<String, String>> emailsList =
                Arrays.asList(
                        KV.of("amy", "amy@example.com"),
                        KV.of("carl", "carl@example.com"),
                        KV.of("julia", "julia@example.com"),
                        KV.of("carl", "carl@email.com"));

        final List<KV<String, String>> phonesList =
                Arrays.asList(
                        KV.of("amy", "111-222-3333"),
                        KV.of("james", "222-333-4444"),
                        KV.of("amy", "333-444-5555"),
                        KV.of("carl", "444-555-6666"));

        PCollection<KV<String, String>> emails = pipeline.apply("CreateEmails", Create.of(emailsList));
        PCollection<KV<String, String>> phones = pipeline.apply("CreatePhones", Create.of(phonesList));

        final TupleTag<String> emailsTag = new TupleTag<>();
        final TupleTag<String> phonesTag = new TupleTag<>();

        KeyedPCollectionTuple<String> emailsAndPhonesKeyedTuple = KeyedPCollectionTuple.of(emailsTag, emails).and(phonesTag, phones);

        PCollection<KV<String, CoGbkResult>> coGroupedData = emailsAndPhonesKeyedTuple.apply("Co Group", CoGroupByKey.create());

        PCollection<String> formattedOutput = coGroupedData.apply("Format output", ParDo.of(new FormatOutputFn(emailsTag, phonesTag)));

        PDone result = formattedOutput.apply(
                "Write output to TextFile",
                TextIO.write()
                        .to(pipelineOptions.getOutput())
                        .withNumShards(1).withSuffix(".txt"));

        pipeline.run().waitUntilFinish();
    }

    public static class FormatOutputFn extends DoFn<KV<String, CoGbkResult>, String> {
        TupleTag<String> left;
        TupleTag<String> right;

        public FormatOutputFn(TupleTag<String> left, TupleTag<String> right) {
            this.left = left;
            this.right = right;
        }

        @ProcessElement
        public void processElement(@Element KV<String, CoGbkResult> coGroupedKvPair, OutputReceiver<String> out) {
            List<String> leftData = new ArrayList<>();
            Objects.requireNonNull(coGroupedKvPair.getValue()).getAll(left).iterator().forEachRemaining(leftData::add);
            List<String> rightData = new ArrayList<>();
            Objects.requireNonNull(coGroupedKvPair.getValue().getAll(right)).iterator().forEachRemaining(rightData::add);
            out.output(coGroupedKvPair.getKey() + " : " + leftData + ";" + rightData);

//            // If we don't wan't [] in the output when the list if empty, we can use below logic
//            String leftDataInString = leftData.isEmpty() ? "" : leftData.toString();
//            String rightDataInString = rightData.isEmpty() ? "" : rightData.toString();
//            out.output(coGroupedKvPair.getKey() + " : " + leftDataInString + ";" + rightDataInString);
        }
    }
}

/*
* Output:
    amy : [amy@example.com];[111-222-3333, 333-444-5555]
    james : [];[222-333-4444]
    carl : [carl@example.com, carl@email.com];[444-555-6666]
    julia : [julia@example.com];[]
* */