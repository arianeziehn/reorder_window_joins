import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple12;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Before;
import org.junit.Test;
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
public class JoinReorderingTest_3wayMixedWindowJoin {

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

        // Create test streams A, B, C with some example data
        streamA = env.fromElements(
                new Tuple3<>(1, 2, (1*60000L)),
                new Tuple3<>(1, 3, (3*60000L)),
                new Tuple3<>(1, 2, (5*60000L)),
                new Tuple3<>(1, 3, (7*60000L)),
                new Tuple3<>(1, 2, (9*60000L)),
                new Tuple3<>(1, 3, (11*60000L)),
                new Tuple3<>(1, 2, (13*60000L)),
                new Tuple3<>(1, 3, (15*60000L)),
                new Tuple3<>(1, 2, (17*60000L)),
                new Tuple3<>(1, 3, (19*60000L)),
                new Tuple3<>(1, 2, (21*60000L)),
                new Tuple3<>(1, 3, (23*60000L)),
                new Tuple3<>(1, 2, (25*60000L)),
                new Tuple3<>(1, 3, (27*60000L)),
                new Tuple3<>(1, 2, (29*60000L)),
                new Tuple3<>(1, 3, (31*60000L)),
                new Tuple3<>(1, 2, (33*60000L)),
                new Tuple3<>(1, 3, (35*60000L)),
                new Tuple3<>(1, 2, (37*60000L)),
                new Tuple3<>(1, 3, (39*60000L)),
                new Tuple3<>(1, 2, (41*60000L)),
                new Tuple3<>(1, 3, (43*60000L)),
                new Tuple3<>(1, 2, (45*60000L)),
                new Tuple3<>(1, 3, (47*60000L)),
                new Tuple3<>(1, 2, (49*60000L)),
                new Tuple3<>(1, 3, (51*60000L)),
                new Tuple3<>(1, 2, (53*60000L)),
                new Tuple3<>(1, 3, (55*60000L)),
                new Tuple3<>(1, 2, (57*60000L)),
                new Tuple3<>(1, 3, (59*60000L)),
                new Tuple3<>(1, 2, (61*60000L)),
                new Tuple3<>(1, 3, (63*60000L)),
                new Tuple3<>(1, 2, (65*60000L)),
                new Tuple3<>(1, 3, (67*60000L)),
                new Tuple3<>(1, 2, (69*60000L)),
                new Tuple3<>(1, 3, (71*60000L)),
                new Tuple3<>(1, 2, (73*60000L)),
                new Tuple3<>(1, 3, (75*60000L)),
                new Tuple3<>(1, 2, (77*60000L)),
                new Tuple3<>(1, 3, (79*60000L))
        ).assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(1000));

        streamB = env.fromElements(
                new Tuple3<>(1, 2, (2*60000L)),
                new Tuple3<>(1, 3, (4*60000L)),
                new Tuple3<>(1, 2, (6*60000L)),
                new Tuple3<>(1, 3, (8*60000L)),
                new Tuple3<>(1, 2, (10*60000L)),
                new Tuple3<>(1, 3, (12*60000L)),
                new Tuple3<>(1, 2, (14*60000L)),
                new Tuple3<>(1, 3, (16*60000L)),
                new Tuple3<>(1, 2, (18*60000L)),
                new Tuple3<>(1, 3, (20*60000L)),
                new Tuple3<>(1, 2, (22*60000L)),
                new Tuple3<>(1, 3, (24*60000L)),
                new Tuple3<>(1, 2, (26*60000L)),
                new Tuple3<>(1, 3, (28*60000L)),
                new Tuple3<>(1, 2, (30*60000L)),
                new Tuple3<>(1, 3, (32*60000L)),
                new Tuple3<>(1, 2, (34*60000L)),
                new Tuple3<>(1, 3, (36*60000L)),
                new Tuple3<>(1, 2, (38*60000L)),
                new Tuple3<>(1, 3, (40*60000L)),
                new Tuple3<>(1, 2, (42*60000L)),
                new Tuple3<>(1, 3, (44*60000L)),
                new Tuple3<>(1, 2, (46*60000L)),
                new Tuple3<>(1, 3, (48*60000L)),
                new Tuple3<>(1, 3, (50*60000L)),
                new Tuple3<>(1, 2, (52*60000L)),
                new Tuple3<>(1, 3, (54*60000L)),
                new Tuple3<>(1, 2, (56*60000L)),
                new Tuple3<>(1, 3, (58*60000L)),
                new Tuple3<>(1, 2, (60*60000L)),
                new Tuple3<>(1, 3, (62*60000L)),
                new Tuple3<>(1, 2, (64*60000L)),
                new Tuple3<>(1, 3, (66*60000L)),
                new Tuple3<>(1, 2, (68*60000L)),
                new Tuple3<>(1, 3, (70*60000L)),
                new Tuple3<>(1, 2, (72*60000L)),
                new Tuple3<>(1, 3, (74*60000L)),
                new Tuple3<>(1, 2, (76*60000L)),
                new Tuple3<>(1, 3, (78*60000L)),
                new Tuple3<>(1, 2, (80*60000L)),
                new Tuple3<>(1, 3, (82*60000L))
        ).assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(6000));

        streamC = env.fromElements(
                new Tuple3<>(1, 2, (1*60000L)),
                new Tuple3<>(1, 3, (3*60000L)),
                new Tuple3<>(1, 2, (5*60000L)),
                new Tuple3<>(1, 3, (7*60000L)),
                new Tuple3<>(1, 2, (9*60000L)),
                new Tuple3<>(1, 3, (11*60000L)),
                new Tuple3<>(1, 2, (13*60000L)),
                new Tuple3<>(1, 3, (15*60000L)),
                new Tuple3<>(1, 2, (17*60000L)),
                new Tuple3<>(1, 3, (19*60000L)),
                new Tuple3<>(1, 2, (21*60000L)),
                new Tuple3<>(1, 3, (23*60000L)),
                new Tuple3<>(1, 2, (25*60000L)),
                new Tuple3<>(1, 3, (27*60000L)),
                new Tuple3<>(1, 2, (29*60000L)),
                new Tuple3<>(1, 3, (31*60000L)),
                new Tuple3<>(1, 2, (33*60000L)),
                new Tuple3<>(1, 3, (35*60000L)),
                new Tuple3<>(1, 2, (37*60000L)),
                new Tuple3<>(1, 3, (39*60000L)),
                new Tuple3<>(1, 2, (41*60000L)),
                new Tuple3<>(1, 3, (43*60000L)),
                new Tuple3<>(1, 2, (45*60000L)),
                new Tuple3<>(1, 3, (47*60000L)),
                new Tuple3<>(1, 2, (49*60000L)),
                new Tuple3<>(1, 3, (51*60000L)),
                new Tuple3<>(1, 2, (53*60000L)),
                new Tuple3<>(1, 3, (55*60000L)),
                new Tuple3<>(1, 2, (57*60000L)),
                new Tuple3<>(1, 3, (59*60000L)),
                new Tuple3<>(1, 2, (61*60000L)),
                new Tuple3<>(1, 3, (63*60000L)),
                new Tuple3<>(1, 2, (65*60000L)),
                new Tuple3<>(1, 3, (67*60000L)),
                new Tuple3<>(1, 2, (69*60000L)),
                new Tuple3<>(1, 3, (71*60000L)),
                new Tuple3<>(1, 2, (73*60000L)),
                new Tuple3<>(1, 3, (75*60000L)),
                new Tuple3<>(1, 2, (77*60000L)),
                new Tuple3<>(1, 3, (79*60000L))
        ).assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(6000));

        streamD = env.fromElements(
                new Tuple3<>(1, 2, (2*60000L)),
                new Tuple3<>(1, 3, (4*60000L)),
                new Tuple3<>(1, 2, (6*60000L)),
                new Tuple3<>(1, 3, (8*60000L)),
                new Tuple3<>(1, 2, (10*60000L)),
                new Tuple3<>(1, 3, (12*60000L)),
                new Tuple3<>(1, 2, (14*60000L)),
                new Tuple3<>(1, 3, (16*60000L)),
                new Tuple3<>(1, 2, (18*60000L)),
                new Tuple3<>(1, 3, (20*60000L)),
                new Tuple3<>(1, 2, (22*60000L)),
                new Tuple3<>(1, 3, (24*60000L)),
                new Tuple3<>(1, 2, (26*60000L)),
                new Tuple3<>(1, 3, (28*60000L)),
                new Tuple3<>(1, 2, (30*60000L)),
                new Tuple3<>(1, 3, (32*60000L)),
                new Tuple3<>(1, 2, (34*60000L)),
                new Tuple3<>(1, 3, (36*60000L)),
                new Tuple3<>(1, 2, (38*60000L)),
                new Tuple3<>(1, 3, (40*60000L)),
                new Tuple3<>(1, 2, (42*60000L)),
                new Tuple3<>(1, 3, (44*60000L)),
                new Tuple3<>(1, 2, (46*60000L)),
                new Tuple3<>(1, 3, (48*60000L)),
                new Tuple3<>(1, 3, (50*60000L)),
                new Tuple3<>(1, 2, (52*60000L)),
                new Tuple3<>(1, 3, (54*60000L)),
                new Tuple3<>(1, 2, (56*60000L)),
                new Tuple3<>(1, 3, (58*60000L)),
                new Tuple3<>(1, 2, (60*60000L)),
                new Tuple3<>(1, 3, (62*60000L)),
                new Tuple3<>(1, 2, (64*60000L)),
                new Tuple3<>(1, 3, (66*60000L)),
                new Tuple3<>(1, 2, (68*60000L)),
                new Tuple3<>(1, 3, (70*60000L)),
                new Tuple3<>(1, 2, (72*60000L)),
                new Tuple3<>(1, 3, (74*60000L)),
                new Tuple3<>(1, 2, (76*60000L)),
                new Tuple3<>(1, 3, (78*60000L)),
                new Tuple3<>(1, 2, (80*60000L)),
                new Tuple3<>(1, 3, (82*60000L))
        ).assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(6000));
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
