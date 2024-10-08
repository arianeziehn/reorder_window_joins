import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import util.*;

import java.util.concurrent.TimeUnit;

import static java.lang.Math.max;

/**
 * This class runs the combinations of a two-way Sliding Window Join Queries [[A X B]^w1 x C]^w2
 */

public class SWJClusterT4 {
    public static void main(String[] args) throws Exception {

        final ParameterTool parameters = ParameterTool.fromArgs(args);
        // Checking input parameters
        // the number of keys, should be equals or more as parallelism
        int numberOfKeys = parameters.getInt("keys", 16);
        // we except minutes
        int w1Size = parameters.getInt("w1size", 20);
        int w1Slide = parameters.getInt("w1slide", 10);
        int w2Size = parameters.getInt("w2size", 20);
        int w2Slide = parameters.getInt("w2slide", 10);
        long throughput = parameters.getLong("tput", 100); //for me at least rather heavy already
        int freqA = parameters.getInt("freqA", 30); // 1 tuples per minute that is how to play with selective
        int freqB = parameters.getInt("freqB", 15);
        int freqC = parameters.getInt("freqC", 1);
        int parallelism = parameters.getInt("para", 16);
        int runtime = parameters.getInt("run", 1);
        boolean filter = parameters.getBoolean("filter", false);
        String joinOrder = parameters.get("order", "ABC");
        // 1 * 20 = 20 tuples per window per stream
        // 20*20 for window one * 20 for third window
        int maxFreq = max(max(freqA, freqB), freqC);
        long throughputA = (long) (throughput * ((double) (freqA) / (maxFreq)));
        long throughputB = (long) (throughput * ((double) (freqB) / (maxFreq)));
        long throughputC = (long) (throughput * ((double) (freqC) / (maxFreq)));
        String timePropagation = parameters.get("tProp", "A");

        String outputPath;
        if (!parameters.has("output")) {
            outputPath = "./src/main/resources/resultSWJ";
        } else {
            outputPath = parameters.get("output") + joinOrder + "_" + w1Size + "_" + w1Slide + "_" + w2Size + "_" + w2Slide;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        System.out.println(joinOrder + "with Windows w1 (" + w1Size + ";" + w1Slide + ") and w2 (" + w2Size + ";" + w2Slide + ").");
        System.out.println("filter is " + filter);

        DataStream<Tuple10<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> resultStream;

        if (joinOrder.equals("ACB") || joinOrder.equals("CAB")) {

            DataStream<Tuple4<Integer, Integer, Long, Long>> streamA = env.addSource(new ArtificialSourceFunctionT4(throughputA, runtime, freqA, numberOfKeys))
                    .setParallelism(parallelism)
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000)); // as we consider Minutes

            DataStream<Tuple4<Integer, Integer, Long, Long>> streamC = env.addSource(new ArtificialSourceFunctionT4(throughputC, runtime, freqC, numberOfKeys))
                    .setParallelism(parallelism)
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000));

            streamA.flatMap(new ThroughputLogger<Tuple4<Integer, Integer, Long, Long>>(ArtificialSourceFunctionT4.RECORD_SIZE_IN_BYTE, throughputA));
            streamC.flatMap(new ThroughputLogger<Tuple4<Integer, Integer, Long, Long>>(ArtificialSourceFunctionT4.RECORD_SIZE_IN_BYTE, throughputC));

            if (filter) {
                streamA = streamA.filter(new UDFs.filterPosIntT4());
                streamC = streamC.filter(new UDFs.filterPosIntT4());
            }

            DataStream<Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long>> streamAC;
            if (joinOrder.equals("CAB")) {
                streamAC = new SWJ_LA_T4(streamC, streamA, w2Size, w2Slide).run();
            } else {
                streamAC = new SWJ_AR_T4(streamA, streamC, w2Size, w2Slide).run();
            }

            streamAC.assignTimestampsAndWatermarks(new UDFs.ExtractTimestampAR_T7(60000, timePropagation));

            DataStream<Tuple4<Integer, Integer, Long, Long>> streamB = env.addSource(new ArtificialSourceFunctionT4(throughputB, runtime, freqB, numberOfKeys))
                    .setParallelism(parallelism)
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000));

            streamB.flatMap(new ThroughputLogger<Tuple4<Integer, Integer, Long, Long>>(ArtificialSourceFunctionT4.RECORD_SIZE_IN_BYTE, throughputB));

            if (filter) {
                streamB = streamB.filter(new UDFs.filterPosIntT4());
            }

            resultStream = new SWJ_AB_T7(streamAC, streamB, w1Size, w1Slide).run();
        } else if (joinOrder.equals("ABC") || joinOrder.equals("BAC")) {

            DataStream<Tuple4<Integer, Integer, Long, Long>> streamA = env.addSource(new ArtificialSourceFunctionT4(throughputA, runtime, freqA, numberOfKeys))
                    .setParallelism(parallelism)
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000)); // as we consider Minutes

            DataStream<Tuple4<Integer, Integer, Long, Long>> streamB = env.addSource(new ArtificialSourceFunctionT4(throughputB, runtime, freqB, numberOfKeys))
                    .setParallelism(parallelism)
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000));

            streamA.flatMap(new ThroughputLogger<Tuple4<Integer, Integer, Long, Long>>(ArtificialSourceFunctionT4.RECORD_SIZE_IN_BYTE, throughputA));
            streamB.flatMap(new ThroughputLogger<Tuple4<Integer, Integer, Long, Long>>(ArtificialSourceFunctionT4.RECORD_SIZE_IN_BYTE, throughputB));

            if (filter) {
                streamA = streamA.filter(new UDFs.filterPosIntT4());
                streamB = streamB.filter(new UDFs.filterPosIntT4());
            }

            DataStream<Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long>> streamAB;

            if (joinOrder.equals("BAC")) {
                streamAB = new SWJ_LA_T4(streamB, streamA, w1Size, w1Slide).run();
            } else {
                streamAB = new SWJ_AR_T4(streamA, streamB, w1Size, w1Slide).run();

            }
            streamAB.assignTimestampsAndWatermarks(new UDFs.ExtractTimestampAR_T7(60000, timePropagation));

            DataStream<Tuple4<Integer, Integer, Long, Long>> streamC = env.addSource(new ArtificialSourceFunctionT4(throughputC, runtime, freqC, numberOfKeys))
                    .setParallelism(parallelism)
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000));

            streamC.flatMap(new ThroughputLogger<Tuple4<Integer, Integer, Long, Long>>(ArtificialSourceFunctionT4.RECORD_SIZE_IN_BYTE, throughputC));
            if (filter) {
                streamC = streamC.filter(new UDFs.filterPosIntT4());
            }

            resultStream = new SWJ_AC_T7(streamAB, streamC, w2Size, w2Slide).run();

        } else if ((joinOrder.equals("BCA") || joinOrder.equals("CBA")) && w1Slide >= w1Size && w2Slide >= w2Size) {

            DataStream<Tuple4<Integer, Integer, Long, Long>> streamB = env.addSource(new ArtificialSourceFunctionT4(throughputB, runtime, freqB, numberOfKeys))
                    .setParallelism(parallelism)
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000));

            DataStream<Tuple4<Integer, Integer, Long, Long>> streamC = env.addSource(new ArtificialSourceFunctionT4(throughputC, runtime, freqC, numberOfKeys))
                    .setParallelism(parallelism)
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000));

            streamC.flatMap(new ThroughputLogger<Tuple4<Integer, Integer, Long, Long>>(ArtificialSourceFunctionT4.RECORD_SIZE_IN_BYTE, throughputC));
            streamB.flatMap(new ThroughputLogger<Tuple4<Integer, Integer, Long, Long>>(ArtificialSourceFunctionT4.RECORD_SIZE_IN_BYTE, throughputB));

            if (filter) {
                streamC = streamC.filter(new UDFs.filterPosIntT4());
                streamB = streamB.filter(new UDFs.filterPosIntT4());
            }
            DataStream<Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long>> streamBC;
            if (w1Size > w2Size && joinOrder.equals("BCA")) {
                System.out.println(joinOrder + "with larger Windows w1 (" + w1Size + ";" + w1Slide + ") and smaller w2 (" + w2Size + ";" + w2Slide + ").");
                streamBC = new SWJ_AR_T4(streamB, streamC, w1Size, w1Slide).run();
                timePropagation = "C";
            } else if (w1Size <= w2Size && joinOrder.equals("BCA")) {
                System.out.println(joinOrder + "with Windows smaller w1 (" + w1Size + ";" + w1Slide + ") and larger w2 (" + w2Size + ";" + w2Slide + ").");
                streamBC = new SWJ_AR_T4(streamB, streamC, w2Size, w2Slide).run();
                timePropagation = "B";
            } else if (w1Size > w2Size && joinOrder.equals("CBA")) {
                System.out.println(joinOrder + "with larger Windows w1 (" + w1Size + ";" + w1Slide + ") and smaller w2 (" + w2Size + ";" + w2Slide + ").");
                timePropagation = "C";
                streamBC = new SWJ_LA_T4(streamC, streamB, w1Size, w1Slide).run();
            } else {
                System.out.println(joinOrder + "with Windows smaller w1 (" + w1Size + ";" + w1Slide + ") and larger w2 (" + w2Size + ";" + w2Slide + ").");
                streamBC = new SWJ_LA_T4(streamC, streamB, w2Size, w2Slide).run();
                timePropagation = "B";
            }

            streamBC.assignTimestampsAndWatermarks(new UDFs.ExtractTimestampBR_T7(60000, timePropagation));

            DataStream<Tuple4<Integer, Integer, Long, Long>> streamA = env.addSource(new ArtificialSourceFunctionT4(throughputA, runtime, freqA, numberOfKeys))
                    .setParallelism(parallelism)
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000)); // as we consider Minutes

            streamA.flatMap(new ThroughputLogger<Tuple4<Integer, Integer, Long, Long>>(ArtificialSourceFunctionT4.RECORD_SIZE_IN_BYTE, throughputA));

            if (filter) {
                streamA = streamA.filter(new UDFs.filterPosIntT4());
            }

            if (w1Size > w2Size) {
                resultStream = new SWJ_BA_T7(streamBC, streamA, w2Size, w2Slide).run();
            } else {
                resultStream = new SWJ_BA_T7(streamBC, streamA, w1Size, w1Slide).run();
            }
        } else {
            System.out.println("Something went wrong and we do ABC");
            resultStream = env.fromElements(
                    new Tuple10<>(1, 2, (2 * 60000L), 1, 2, (2 * 60000L), 1, 2, (2 * 60000L), System.currentTimeMillis()));
        }

        resultStream.flatMap(new LatencyLoggerT10())
                //.print();
                .writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE);

        //System.out.println(env.getExecutionPlan());
        JobExecutionResult executionResult = env.execute("My Flink Job");
        System.out.println("The job took " + executionResult.getNetRuntime(TimeUnit.MILLISECONDS) + "ms to execute");
    }

}
