import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import util.ArtificalSourceFunction;
import util.ThroughputLogger;
import util.UDFs;

import java.util.concurrent.TimeUnit;

import static java.lang.Math.max;

/**
 * This class runs the combinations of a two way Interval Join Query [[A X B]^w1 x C]^w2
 */

public class IVJCluster {
    public static void main(String[] args) throws Exception {

        final ParameterTool parameters = ParameterTool.fromArgs(args);
        // Checking input parameters
        // the number of keys, should be equals or more as parallelism
        Integer numberOfKeys = parameters.getInt("keys", 16);
        // we except minutes
        Integer w1lB = -parameters.getInt("w1lB", 20);
        Integer w1uB = parameters.getInt("w1uB", 10);
        Integer w2lB = -parameters.getInt("w2lB", 20);
        Integer w2uB = parameters.getInt("w2uB", 10);
        long throughput = parameters.getLong("tput", 1000); //for me at least rather heavy already
        int freqA = parameters.getInt("freqA", 1); // 1 tuples per minute that is how to play with selective
        int freqB = parameters.getInt("freqB", 1);
        int freqC = parameters.getInt("freqC", 1);
        int parallelism = parameters.getInt("para", 5);
        int runtime = parameters.getInt("run", 25);
        String joinOrder = parameters.get("order", "ABC");
        // 1 * 20 = 20 tuples per window per stream
        // 20*20 for window one * 20 for third window
        String timePropagation = parameters.get("tProp", "A");

        String outputPath;
        if (!parameters.has("output")) {
            outputPath = "./src/main/resources/resultIVJ";
        } else {
            outputPath = parameters.get("output") + "IVJ";
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple3<Integer, Integer, Long>> streamA = env.addSource(new ArtificalSourceFunction(throughput, runtime, freqA, numberOfKeys))
                .setParallelism(parallelism)
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(60000)); // as we consider Minutes

        DataStream<Tuple3<Integer, Integer, Long>> streamB = env.addSource(new ArtificalSourceFunction(throughput, runtime, freqB, numberOfKeys))
                .setParallelism(parallelism)
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(60000));

        DataStream<Tuple3<Integer, Integer, Long>> streamC = env.addSource(new ArtificalSourceFunction(throughput, runtime, freqC, numberOfKeys))
                .setParallelism(parallelism)
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(60000));

        streamA.flatMap(new ThroughputLogger<Tuple3<Integer, Integer, Long>>(ArtificalSourceFunction.RECORD_SIZE_IN_BYTE, throughput));
        streamB.flatMap(new ThroughputLogger<Tuple3<Integer, Integer, Long>>(ArtificalSourceFunction.RECORD_SIZE_IN_BYTE, throughput));
        streamC.flatMap(new ThroughputLogger<Tuple3<Integer, Integer, Long>>(ArtificalSourceFunction.RECORD_SIZE_IN_BYTE, throughput));

        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> resultStream;

        // join A B
        if (joinOrder.equals("ABC")) { // that is the original query
            System.out.println(joinOrder + "with Windows w1 (" + w1lB + ";" +w1uB + ") and w2 (" + w2lB + ";" +w2uB + ").");
            resultStream = new IVJ_ab_ABC(streamA, streamB, streamC, w1lB, w1uB, w2lB, w2uB, timePropagation).run();
        }
        else {
            boolean w1_equal = Math.abs(w1lB) == (Math.abs(w1uB));
            boolean w2_equal = Math.abs(w2lB) == (Math.abs(w2uB));
            if (joinOrder.equals("BAC")) { // AC is in correct order, thus we do not care about differences in bound, BA is swoped thus we swope and negate boundaries
                if (w1_equal) {
                    System.out.println(joinOrder + "with Windows w1 (" + w1lB + ";" + w1uB + ") and w2 (" + w2lB + ";" + w2uB + ").");
                    resultStream = new IVJ_ab_BAC(streamA, streamB, streamC, w1lB, w1uB, w2lB, w2uB, timePropagation).run();
                } else {
                    System.out.println(joinOrder + "with Windows w1 (" + -w1uB + ";" + -w1lB + ") and w2 (" + w2lB + ";" + w2uB + ").");
                    resultStream = new IVJ_ab_BAC(streamA, streamB, streamC, -w1uB, -w1lB, w2lB, w2uB, timePropagation).run();
                }
            }
            else if (joinOrder.equals("ACB")) { // all in order we do not need to worry
                    System.out.println(joinOrder + "with Windows w1 (" + w1lB + ";" +w1uB + ") and w2 (" + w2lB + ";" +w2uB + ").");
                    resultStream = new IVJ_ac_ACB(streamA, streamB, streamC, w1lB, w1uB, w2lB, w2uB, timePropagation).run();
            }
            else if (joinOrder.equals("CAB")) { // AB in order, CA scwoped wit swope and negate CA
                if (w2_equal) {
                    System.out.println(joinOrder + "with Windows w1 (" + w1lB + ";" +w1uB + ") and w2 (" + w2lB + ";" +w2uB + ").");
                    resultStream = new IVJ_ac_CAB(streamA, streamB, streamC, w1lB, w1uB, w2lB, w2uB, timePropagation).run();
                } else {
                    System.out.println(joinOrder + "with Windows w1 (" + w1lB + ";" + w1uB + ") and w2 (" + w2lB + ";" + w2uB + ").");
                    resultStream = new IVJ_ac_CAB(streamA, streamB, streamC, w1lB, w1uB, -w2uB, -w2lB, timePropagation).run();
                }
            }
            else  {
                System.out.println("Something went wrong and we do ABC");
                resultStream = new IVJ_ab_ABC(streamA, streamB, streamC, w1lB, w1uB, w2lB, w2uB, timePropagation).run();
            }
        }

        resultStream//.print();
                .writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE);

        //System.out.println(env.getExecutionPlan());
        JobExecutionResult executionResult = env.execute("My Flink Job");
        System.out.println("The job took " + executionResult.getNetRuntime(TimeUnit.MILLISECONDS) + "ms to execute");
    }

}
