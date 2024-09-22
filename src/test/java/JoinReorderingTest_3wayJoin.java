import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple12;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple9;
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

public class JoinReorderingTest_3wayJoin {

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
        ).assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(1000));

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
        ).assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(1000));

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
        ).assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(1000));
    }

    @Test
    //Case A1: W1=W2, s < l
    public void testABCD() throws Exception {
        w1Size = 10;
        w1Slide = 5;
        w2Size = -10;
        w2Slide = 5;
        w3Size = 10;
        w3Slide = 10;
        timePropagation1 = "A";
        timePropagation2 = "B";
        String testCase = "Mix";
        // Execute each join operation
        //default case
        DataStream<Tuple12<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABCD =
                new Mixed_WJ_ABCD(streamA, streamB, streamC, streamD, w1Size, w1Slide, w2Size, w2Slide, w3Size, w3Slide, timePropagation1, timePropagation2).run();

        DataStream<Tuple12<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABDC =
                new Mixed_WJ_ABDC(streamA, streamB, streamC, streamD, w1Size, w1Slide, w2Size, w2Slide, w3Size, w3Slide, timePropagation1, timePropagation2).run();


        String outputPath = "./src/main/resources/result_mixed_3_way_";
        // Collect the results into lists
        streamABCD
                .writeAsText(outputPath + "ABCD_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABDC
                .writeAsText(outputPath + "ABDC_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABCD = envBatch.readTextFile(outputPath + "ABCD_"+testCase+".csv").distinct().collect();
        List<String> resultABDC = envBatch.readTextFile(outputPath + "ABDC_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertEquals(resultABCD.size(), resultABDC.size());
        assertEquals(resultABCD, resultABDC);
        /**assertEquals(resultABC, resultBAC);
        assertEquals(resultABC.size(), resultACB.size());
        assertEquals(resultABC, resultACB);
        assertEquals(resultABC.size(), resultCAB.size());
        assertEquals(resultABC, resultCAB);
        assertNotEquals(resultABC, resultBCA);
        assertNotEquals(resultABC, resultCBA);*/
    }

}
