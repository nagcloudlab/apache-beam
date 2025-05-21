package day14;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.*;
import org.apache.beam.sdk.transforms.splittabledofn.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.util.List;
import java.util.stream.Collectors;

public class SimpleSDFFileReader {

    public static class LineByLineSDF extends DoFn<String, String> {

        @GetInitialRestriction
        public OffsetRange getInitialRestriction(@Element String filePath) throws Exception {
            MatchResult.Metadata metadata = FileSystems.matchSingleFileSpec(filePath);
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(Channels.newInputStream(FileSystems.open(metadata.resourceId()))))) {
                long lineCount = reader.lines().count();
                return new OffsetRange(0, lineCount);
            }
        }

        @NewTracker
        public OffsetRangeTracker newTracker(@Restriction OffsetRange range) {
            return new OffsetRangeTracker(range);
        }

        @ProcessElement
        public void processElement(ProcessContext context,
                                   RestrictionTracker<OffsetRange, Long> tracker) throws Exception {
            String filePath = context.element();
            MatchResult.Metadata metadata = FileSystems.matchSingleFileSpec(filePath);
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(Channels.newInputStream(FileSystems.open(metadata.resourceId()))))) {
                List<String> lines = reader.lines().collect(Collectors.toList());
                for (long i = tracker.currentRestriction().getFrom(); i < tracker.currentRestriction().getTo(); i++) {
                    if (!tracker.tryClaim(i)) {
                        return;
                    }
                    context.output(lines.get((int) i));
                }
            }
        }
    }

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.fromArgs(args).create());

        PCollection<String> lines = pipeline
                .apply(Create.of("input/sample.txt"))
                .apply(ParDo.of(new LineByLineSDF()));

        lines.apply("PrintLines", ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(@Element String line) {
                System.out.println("Line: " + line);
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
