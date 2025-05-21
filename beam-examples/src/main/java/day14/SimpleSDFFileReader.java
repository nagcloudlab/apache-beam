package day14;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn.*;
import org.apache.beam.sdk.transforms.splittabledofn.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.util.Collections;

public class SimpleSDFFileReader {

    public static class FileLineReader extends DoFn<String, String> {

        @GetInitialRestriction
        public OffsetRange getInitialRange(@Element String filePath) {
            return new OffsetRange(0, 1); // Just a dummy range
        }

        @NewTracker
        public OffsetRangeTracker newTracker(@Restriction OffsetRange range) {
            return new OffsetRangeTracker(range);
        }

        @ProcessElement
        public void processElement(@Element String filePath,
                                   OutputReceiver<String> out) throws Exception {
            MatchResult.Metadata metadata = FileSystems.matchSingleFileSpec(filePath);
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(Channels.newInputStream(FileSystems.open(metadata.resourceId()))))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    out.output(line);
                }
            }
        }
    }

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        PCollection<String> fileLines = pipeline
                .apply("FilePath", Create.of("input/sample.txt"))
                .apply("ReadLines", ParDo.of(new FileLineReader()));

        fileLines.apply("Print", ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(@Element String line) {
                System.out.println("Line: " + line);
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
