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
 * This class runs the combinations of a two-way Interval Join Query [[A X B]^w1 x C]^w2
 */

public class IVJClusterT4 {
    public static void main(String[] args) throws Exception {

        final ParameterTool parameters = ParameterTool.fromArgs(args);
        // Checking input parameters
        // the number of keys, should be equals or more as parallelism
        int numberOfKeys = parameters.getInt("keys", 16);
        // we except minutes
        int w1lB = -parameters.getInt("w1lB", 10);
        int w1uB = parameters.getInt("w1uB", 20);
        int w2lB = -parameters.getInt("w2lB", 10);
        int w2uB = parameters.getInt("w2uB", 20);
        long throughput = parameters.getLong("tput", 1000);
        int freqA = parameters.getInt("freqA", 1); // equals 1 tuples per event time minute that is how to play with selective, note that event time is just an artificial time
        int freqB = parameters.getInt("freqB", 1);
        int freqC = parameters.getInt("freqC", 1);
        int parallelism = parameters.getInt("para", 5);
        int runtime = parameters.getInt("run", 25); // the time the source runs
        boolean filter = parameters.getBoolean("filter", false); // a positive Integer filter in the non-key attribute of the tuple to disrupt uniform distribution
        String joinOrder = parameters.get("order", "BAC");
        // 1 * 20 = 20 tuples per window per stream
        // 20*20 for window one * 20 for third window
        int maxFreq = max(max(freqA, freqB), freqC);
        long throughputA = (long) (throughput * ((double) (freqA) / (maxFreq)));
        long throughputB = (long) (throughput * ((double) (freqB) / (maxFreq)));
        long throughputC = (long) (throughput * ((double) (freqC) / (maxFreq)));

        String timePropagation = parameters.get("tProp", "A"); // the stream name for time propagation

        String outputPath;
        if (!parameters.has("output")) {
            outputPath = "./src/main/resources/resultIVJ";
        } else {
            outputPath = parameters.get("output") + joinOrder + "_" + w1lB + "_" + w1uB;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        System.out.println(joinOrder + "with Windows w1 (" + w1lB + ";" + w1uB + ") and w2 (" + w2lB + ";" + w2uB + ").");

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
                streamAC = new IVJ_BA_T4(streamC, streamA, -w2uB, -w2lB).run().assignTimestampsAndWatermarks(new UDFs.ExtractTimestampAR_T7(60000, timePropagation));
            } else {
                streamAC = new IVJ_AB_T4(streamA, streamC, w2lB, w2uB).run().assignTimestampsAndWatermarks(new UDFs.ExtractTimestampAR_T7(60000, timePropagation));
            }

            DataStream<Tuple4<Integer, Integer, Long, Long>> streamB = env.addSource(new ArtificialSourceFunctionT4(throughputB, runtime, freqB, numberOfKeys))
                    .setParallelism(parallelism)
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000));

            streamB.flatMap(new ThroughputLogger<Tuple4<Integer, Integer, Long, Long>>(ArtificialSourceFunctionT4.RECORD_SIZE_IN_BYTE, throughputB));

            resultStream = new IVJ_AB_T7(streamAC, streamB, w1lB, w1uB).run();
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
                streamAB = new  IVJ_BA_T4(streamB, streamA, -w1uB, -w1lB).run().assignTimestampsAndWatermarks(new UDFs.ExtractTimestampAR_T7(60000, timePropagation));
            } else {
                streamAB = new IVJ_AB_T4(streamA, streamB, w1lB, w1uB).run().assignTimestampsAndWatermarks(new UDFs.ExtractTimestampAR_T7(60000, timePropagation));
            }

            DataStream<Tuple4<Integer, Integer, Long, Long>> streamC = env.addSource(new ArtificialSourceFunctionT4(throughputC, runtime, freqC, numberOfKeys))
                    .setParallelism(parallelism)
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000));

            streamC.flatMap(new ThroughputLogger<Tuple4<Integer, Integer, Long, Long>>(ArtificialSourceFunction.RECORD_SIZE_IN_BYTE, throughputC));

            if (filter) {
                streamC = streamC.filter(new UDFs.filterPosIntT4());
            }

            resultStream = new IVJ_AC_T7(streamAB, streamC, w2lB, w2uB).run();
        } else {
            System.out.println("Something went wrong");
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
