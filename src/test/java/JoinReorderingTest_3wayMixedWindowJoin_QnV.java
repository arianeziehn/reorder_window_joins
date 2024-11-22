import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple12;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Before;
import org.junit.Test;
import util.Tuple3ParallelSourceFunction;
import util.UDFs;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * In this class we test a three-way Window Join with mixed windows, i.e., [[[A X B]^w1_A x C]^w2_B x D]^w3,
 * where w1 is a SWJ creating overlapping windows, w2 is an IntervalJoin and, w3 is a non-overlapping SWJ
 * Using our window assignment algorithms, we get the following window assignments:
 * streams A, B, C, and D;
 * windows: w_1, w_2, W_3,
 * timestamp: W_2 : A, W_3 : B
 *
 * window_assignment:
 * A:B : w_1
 * A:C : w_2
 * B:C : w_1,W_2
 * B:D : W_3
 * A:D : W_1,W_2,W_3
 * C:D:  W_1,W_2,W_3
 */

public class JoinReorderingTest_3wayMixedWindowJoin_QnV {

    private StreamExecutionEnvironment env;
    private DataStream<Tuple3<Integer, Integer, Long>> streamA;
    private DataStream<Tuple3<Integer, Integer, Long>> streamB;
    private DataStream<Tuple3<Integer, Integer, Long>> streamC;
    private DataStream<Tuple3<Integer, Integer, Long>> streamD;
    private int w1Size;
    private int w1Slide;
    private int w2Size;
    private int w2Slide;
    private int w3Size;
    private int w3Slide;
    private String timePropagation1;
    private String timePropagation2;

    @Before
    public void setup() {
        // Initialize the StreamExecutionEnvironment
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(2);

        String file = "./src/main/resources/QnV_R2000070.csv";
        String filePM = "./src/main/resources/luftdaten_11245.csv";
        // the number of keys, should be equals or more as parallelism
        Integer para = 2;
        Integer numberOfKeys = 5;
        Integer velFilter = 90;
        Integer quaFilter = 80;
        Integer pm10Filter = 20;
        Integer pm2Filter = 20;
        // we except minutes
        long throughput = 1000; // we do it very slowly to be sure system grap it all

        // Create test streams A, B, C with some example data
        streamA = env.addSource(new Tuple3ParallelSourceFunction(file, numberOfKeys, ",", throughput, "V"))
                .setParallelism(para)
                .filter(t -> t.f1 > velFilter )
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(60000));

        streamB = env.addSource(new Tuple3ParallelSourceFunction(file, numberOfKeys, ",", throughput, "Q"))
                .setParallelism(para)
                .filter(t -> t.f1 > quaFilter)
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(60000));

        streamC = env.addSource(new Tuple3ParallelSourceFunction(filePM, numberOfKeys, ";", throughput, "PM10"))
                .setParallelism(para)
                .filter(t -> t.f1 > pm10Filter)
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(180000));

        streamD = env.addSource(new Tuple3ParallelSourceFunction(filePM, numberOfKeys, ";", throughput, "PM2"))
                .setParallelism(para)
                .filter(t -> t.f1 > pm2Filter)
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(180000));
    }


    @Test
    public void testMixedWindows() throws Exception {
        w1Size = 10;
        w1Slide = 5;
        w2Size = -3;
        w2Slide = 5;
        w3Size = 10;
        w3Slide = 10;
        timePropagation1 = "A";
        timePropagation2 = "B";
        String testCase = "Mix";
        // Execute each join operation
        //default case ABCD - [[[A X B]^w1_A x C]^w2_B x D]^w3
        DataStream<Tuple12<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABCD =
                new Mixed_WJ_ABCD(streamA, streamB, streamC, streamD, w1Size, w1Slide, w2Size, w2Slide, w3Size, w3Slide, timePropagation1, timePropagation2).run();
        //join order ABDC - [[[A X B]^w1_B x D]^w3_A x D]^w2,
        DataStream<Tuple12<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABDC =
                new Mixed_WJ_ABDC(streamA, streamB, streamC, streamD, w1Size, w1Slide, w2Size, w2Slide, w3Size, w3Slide, timePropagation1, timePropagation2).run();
        //join order ADCB - [[[A X D]^w1_A x C]^w2_D x B]^w3
        DataStream<Tuple12<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamADCB =
                new Mixed_WJ_ADCB(streamA, streamB, streamC, streamD, w1Size, w1Slide, w2Size, w2Slide, w3Size, w3Slide, timePropagation1, timePropagation2).run();
        // join order B → A → D → C - [[[B X A]^w1_B x D]^w3_A x C]^w2
        DataStream<Tuple12<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBADC =
                new Mixed_WJ_BADC(streamA, streamB, streamC, streamD, w1Size, w1Slide, w2Size, w2Slide, w3Size, w3Slide, timePropagation1, timePropagation2).run();
        // join order B → D → C → A not possible as no valid window assignment for BD -> C
        // join order B → D → A → C - [[[B X D]^w3_B x A]^w1_A x C]^w2
        DataStream<Tuple12<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBDAC =
                new Mixed_WJ_BDAC(streamA, streamB, streamC, streamD, w1Size, w1Slide, w2Size, w2Slide, w3Size, w3Slide, timePropagation1, timePropagation2).run();
        // join order A → C → B → D - [[[A X C]^w2_A x B]^w1_B x D]^w3
        DataStream<Tuple12<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamACBD =
                new Mixed_WJ_ACBD(streamA, streamB, streamC, streamD, w1Size, w1Slide, w2Size, w2Slide, w3Size, w3Slide, timePropagation1, timePropagation2).run();
        // join order A → C → D → B - [[[A X C]^w2_A x D]^w3_B x B]^w1
        DataStream<Tuple12<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamACDB =
                new Mixed_WJ_ACDB(streamA, streamB, streamC, streamD, w1Size, w1Slide, w2Size, w2Slide, w3Size, w3Slide, timePropagation1, timePropagation2).run();

        String outputPath = "./src/main/resources/result_mixed_3_way_";
        // Collect the results into lists
        streamABCD
                .writeAsText(outputPath + "ABCD_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABDC
                .writeAsText(outputPath + "ABDC_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamADCB
                .writeAsText(outputPath + "ADCB_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBADC
                .writeAsText(outputPath + "BADC_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBDAC
                .writeAsText(outputPath + "BDAC_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamACBD
                .writeAsText(outputPath + "ACBD_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamACDB
                .writeAsText(outputPath + "ACDB_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABCD = envBatch.readTextFile(outputPath + "ABCD_"+testCase+".csv").distinct().collect();
        List<String> resultABDC = envBatch.readTextFile(outputPath + "ABDC_"+testCase+".csv").distinct().collect();
        List<String> resultADCB = envBatch.readTextFile(outputPath + "ADCB_"+testCase+".csv").distinct().collect();
        List<String> resultBADC = envBatch.readTextFile(outputPath + "BADC_"+testCase+".csv").distinct().collect();
        List<String> resultBDAC = envBatch.readTextFile(outputPath + "BDAC_"+testCase+".csv").distinct().collect();
        List<String> resultACBD = envBatch.readTextFile(outputPath + "ACBD_"+testCase+".csv").distinct().collect();
        List<String> resultACDB = envBatch.readTextFile(outputPath + "ACDB_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertEquals(resultABCD.size(), resultABDC.size());
        assertEquals(resultABCD, resultABDC);
        assertNotEquals(resultABCD, resultADCB); // A x D has no explicit window assignment, and the list of possible window assignments does not contain multiple non-overlapping Sliding Window Joins
        assertEquals(resultABCD.size(), resultBADC.size());
        assertEquals(resultABCD, resultBADC);
        assertEquals(resultABCD.size(), resultBDAC.size());
        assertEquals(resultABCD, resultBDAC);
        assertEquals(resultABCD.size(), resultACBD.size());
        assertEquals(resultABCD, resultACBD);
        assertNotEquals(resultABCD, resultACDB); // AC x D has no explicit window assignment, and the list of possible window assignments does not contain multiple non-overlapping Sliding Window Joins
    }

}
