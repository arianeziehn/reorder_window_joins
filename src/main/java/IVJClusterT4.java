import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import util.*;

import java.util.concurrent.TimeUnit;

import static java.lang.Math.max;

/**
 * This class runs the combinations of a two way Interval Join Query [[A X B]^w1 x C]^w2
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
        long throughput = parameters.getLong("tput", 1000); //for me at least rather heavy already
        int freqA = parameters.getInt("freqA", 1); // 1 tuples per minute that is how to play with selective
        int freqB = parameters.getInt("freqB", 1);
        int freqC = parameters.getInt("freqC", 1);
        int parallelism = parameters.getInt("para", 5);
        int runtime = parameters.getInt("run", 25);
        boolean filter = parameters.getBoolean("filter", false);
        String joinOrder = parameters.get("order", "BAC");
        // 1 * 20 = 20 tuples per window per stream
        // 20*20 for window one * 20 for third window
        int maxFreq = max(max(freqA, freqB), freqC);
        long throughputA = (long) (throughput * ((double) (freqA) / (maxFreq)));
        long throughputB = (long) (throughput * ((double) (freqB) / (maxFreq)));
        long throughputC = (long) (throughput * ((double) (freqC) / (maxFreq)));
        String timePropagation = parameters.get("tProp", "A");

        String outputPath;
        if (!parameters.has("output")) {
            outputPath = "./src/main/resources/resultIVJ";
        } else {
            outputPath = parameters.get("output") + joinOrder;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple4<Integer, Integer, Long, Long>> streamA = env.addSource(new ArtificialSourceFunctionT4(throughputA, runtime, freqA, numberOfKeys))
                .setParallelism(parallelism)
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000)); // as we consider Minutes

        DataStream<Tuple4<Integer, Integer, Long, Long>> streamB = env.addSource(new ArtificialSourceFunctionT4(throughputB, runtime, freqB, numberOfKeys))
                .setParallelism(parallelism)
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000));

        DataStream<Tuple4<Integer, Integer, Long, Long>> streamC = env.addSource(new ArtificialSourceFunctionT4(throughputC, runtime, freqC, numberOfKeys))
                .setParallelism(parallelism)
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000));

        streamA.flatMap(new ThroughputLogger<Tuple4<Integer, Integer, Long, Long>>(ArtificialSourceFunction.RECORD_SIZE_IN_BYTE, throughputA));
        streamB.flatMap(new ThroughputLogger<Tuple4<Integer, Integer, Long, Long>>(ArtificialSourceFunction.RECORD_SIZE_IN_BYTE, throughputB));
        streamC.flatMap(new ThroughputLogger<Tuple4<Integer, Integer, Long, Long>>(ArtificialSourceFunction.RECORD_SIZE_IN_BYTE, throughputC));

        if (filter) {
            streamA = streamA.filter(new UDFs.filterPosIntT4());
            streamB = streamB.filter(new UDFs.filterPosIntT4());
            streamC = streamC.filter(new UDFs.filterPosIntT4());
        }

        DataStream<Tuple10<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> resultStream;

        // join A B
        if (joinOrder.equals("ABC")) { // that is the original query
            System.out.println(joinOrder + "with Windows w1 (" + w1lB + ";" + w1uB + ") and w2 (" + w2lB + ";" + w2uB + ").");
            resultStream = new IVJ_ab_ABC_T4(streamA, streamB, streamC, w1lB, w1uB, w2lB, w2uB, timePropagation).run();
        } else {
            boolean w1_equal = Math.abs(w1lB) == (Math.abs(w1uB));
            boolean w2_equal = Math.abs(w2lB) == (Math.abs(w2uB));
            if (joinOrder.equals("BAC")) { // AC is in correct order, thus we do not care about differences in bound, BA is swoped thus we swope and negate boundaries
                if (w1_equal) {
                    System.out.println(joinOrder + "with Windows w1 (" + w1lB + ";" + w1uB + ") and w2 (" + w2lB + ";" + w2uB + ").");
                    resultStream = new IVJ_ab_BAC_T4(streamA, streamB, streamC, w1lB, w1uB, w2lB, w2uB, timePropagation).run();
                } else {
                    System.out.println(joinOrder + " with Windows w1 (" + -w1uB + ";" + -w1lB + ") and w2 (" + w2lB + ";" + w2uB + ").");
                    resultStream = new IVJ_ab_BAC_T4(streamA, streamB, streamC, -w1uB, -w1lB, w2lB, w2uB, timePropagation).run();
                }
            } else if (joinOrder.equals("ACB")) { // all in order we do not need to worry
                System.out.println(joinOrder + "with Windows w1 (" + w1lB + ";" + w1uB + ") and w2 (" + w2lB + ";" + w2uB + ").");
                resultStream = new IVJ_ac_ACB_T4(streamA, streamB, streamC, w1lB, w1uB, w2lB, w2uB, timePropagation).run();
            } else if (joinOrder.equals("CAB")) { // AB in order, AC is swoped thus, swope and negate CA
                if (w2_equal) {
                    System.out.println(joinOrder + "with Windows w1 (" + w1lB + ";" + w1uB + ") and w2 (" + w2lB + ";" + w2uB + ").");
                    resultStream = new IVJ_ac_CAB_T4(streamA, streamB, streamC, w1lB, w1uB, w2lB, w2uB, timePropagation).run();
                } else {
                    System.out.println(joinOrder + "with Windows w1 (" + w1lB + ";" + w1uB + ") and w2 (" + -w2uB + ";" + -w2lB + ").");
                    resultStream = new IVJ_ac_CAB_T4(streamA, streamB, streamC, w1lB, w1uB, -w2uB, -w2lB, timePropagation).run();
                }
            } else {
                System.out.println("Something went wrong and we do ABC");
                resultStream = new IVJ_ab_ABC_T4(streamA, streamB, streamC, w1lB, w1uB, w2lB, w2uB, timePropagation).run();
            }
        }

        resultStream.flatMap(new LatencyLoggerT10())
                //.print();
                .writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE);

        //System.out.println(env.getExecutionPlan());
        JobExecutionResult executionResult = env.execute("My Flink Job");
        System.out.println("The job took " + executionResult.getNetRuntime(TimeUnit.MILLISECONDS) + "ms to execute");
    }

}
