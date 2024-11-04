import org.apache.flink.api.java.ExecutionEnvironment;
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

/**
 * This class contains test cases covering the seven identified cases for associativity, i.e., Case A1-A4 for sliding window joins,
 * Case A5 for Session Window Joins, and the Cases A6 and A7 for the IntervalJoin.
 * The test considers a single key.
 * In particular, it evaluates if a window join query [[A x B]^W1 X C]^W2 = [[B x C]^W2 X A]^W1
 */
public class AssociativeTest {

    private StreamExecutionEnvironment env;
    private DataStream<Tuple3<Integer, Integer, Long>> streamA;
    private DataStream<Tuple3<Integer, Integer, Long>> streamB;
    private DataStream<Tuple3<Integer, Integer, Long>> streamC;
    private int w1Size;
    private int w1Slide;
    private int w2Size;
    private int w2Slide;
    private String timePropagation;

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
    }

    @Test
    //Case A1: W1 = W2, W2.s < W2.l
    public void CaseA1() throws Exception {
        w1Size = 10;
        w1Slide = 5;
        w2Size = 10;
        w2Slide = 5;
        String testCase = "A1_";
        // Execute each join operation
        // first we use a for time propagation in q_1 = ABC_a and q_2 = BCA_a
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // second we use b for time propagation in q_1 = ABC_b and q_2 = BCA_b
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A1: W1 = W2, W2.s < W2.l
    public void CaseA1_outer_edge_case() throws Exception {
        w1Size = 10;
        w1Slide = 9;
        w2Size = 10;
        w2Slide = 9;

        String testCase = "A1_l10_s9";
        // Execute each join operation
        // first we use a for time propagation in q_1 = ABC_a and q_2 = BCA_a
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // second we use b for time propagation in q_1 = ABC_b and q_2 = BCA_b
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A1: W1 = W2, W2.s < W2.l
    public void CaseA1_outer_edge_case_2() throws Exception {
        w1Size = 20;
        w1Slide = 15;
        w2Size = 20;
        w2Slide = 15;
        timePropagation = "A";
        String testCase = "A1_l20_s15";
        // Execute each join operation
        // first we use a for time propagation in q_1 = ABC_a and q_2 = BCA_a
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // second we use b for time propagation in q_1 = ABC_b and q_2 = BCA_b
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A2: W1=W2, W2.s >= W2.l
    public void CaseA2() throws Exception {
        // Set up the testing environment
        w1Size = 10;
        w1Slide = 15;
        w2Size = 10;
        w2Slide = 15;

        String testCase = "A2_l10_s15";
        // Execute each join operation
        // first we use a for time propagation in q_1 = ABC_a and q_2 = BCA_a
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // second we use b for time propagation in q_1 = ABC_b and q_2 = BCA_b
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertEquals(resultABC_a.size(), resultBCA_a.size());
        assertEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A2 W1=W2, W2.s >= W2.l
    public void CaseA2_TWJ() throws Exception {
        // Set up the testing environment
        w1Size = 10;
        w1Slide = 10;
        w2Size = 10;
        w2Slide = 10;
        timePropagation = "A";
        String testCase = "A2_l_s10";
        // Execute each join operation
        // first we use a for time propagation in q_1 = ABC_a and q_2 = BCA_a
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // second we use b for time propagation in q_1 = ABC_b and q_2 = BCA_b
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertEquals(resultABC_a.size(), resultBCA_a.size());
        assertEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A2: W1=W2, W2.s >= W2.l
    public void CaseA2_slide_no_divisor() throws Exception {
        // Set up the testing environment
        w1Size = 10;
        w1Slide = 13;
        w2Size = 10;
        w2Slide = 13;
        timePropagation = "A";
        String testCase = "A2_slide_no_divisor";
        // Execute each join operation
        // first we use a for time propagation in q_1 = ABC_a and q_2 = BCA_a
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // second we use b for time propagation in q_1 = ABC_b and q_2 = BCA_b
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertEquals(resultABC_a.size(), resultBCA_a.size());
        assertEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }


    @Test
    //Case A3: W1 != W2, W2.s < W2.l and W1 is non overlapping
    public void CaseA3_w1_gt_w2() throws Exception {
        // Set up the testing environment
        w1Size = 30;
        w1Slide = 15;
        w2Size = 15;
        w2Slide = 5;

        String testCase = "A3_w1_gt_w2";
        // Execute each join operation
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // Execute each join operation
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A3 W1 != W2, W2.s < W2.l and W1 is non overlapping
    public void CaseA3_w1_lt_w2() throws Exception {
        // Set up the testing environment
        w1Size = 10;
        w1Slide = 5;
        w2Size = 15;
        w2Slide = 5;

        String testCase = "A3_w1_lt_w2";
        // Execute each join operation
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // Execute each join operation
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A3 W1 != W2, W2.s < W2.l and W1.l >= W2.l
    public void Case_MixedWindow_w1_non_overlapping_w2_overlapping() throws Exception {
        // Set up the testing environment
        w1Size = 20;
        w1Slide = 30;
        w2Size = 15;
        w2Slide = 5;

        String testCase = "A3_A4";
        // Execute each join operation
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // Execute each join operation
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }
    @Test
    //Case A4 W1 != W2, W2.s >= W2.l and W1.l >= W2.l
    public void CaseA4_slide_eq_w1_gt_w2() throws Exception {
        // Set up the testing environment
        w1Size = 20;
        w1Slide = 30;
        w2Size = 15;
        w2Slide = 30;

        String testCase = "A4_w1_gt_w2";
        // Execute each join operation with timestamp propagation of A
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // Execute each join operation with timestamp propagation of B
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A4 W1 != W2, W2.s >= W2.l and W1.l >= W2.l
    public void CaseA4_slide_eq_w1_lt_w2() throws Exception {
        // Set up the testing environment
        w1Size = 10;
        w1Slide = 15;
        w2Size = 15;
        w2Slide = 20;

        String testCase = "A4_w1_lt_w2";
        // Execute each join operation with timestamp propagation of A
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // Execute each join operation with timestamp propagation of B
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A4 W1 != W2, W2.s >= W2.l and W1.l >= W2.l
    public void CaseA4_slides_eq_w1_lt_w2_s1_gt_s2() throws Exception {
        // Set up the testing environment
        w1Size = 10;
        w1Slide = 20;
        w2Size = 15;
        w2Slide = 15;

        String testCase = "A4_w1_lt_w2_s1_gt_s2";
        // Execute each join operation with timestamp propagation of A
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // Execute each join operation with timestamp propagation of B
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A4 W1 != W2, W2.s >= W2.l and W1.l >= W2.l
    public void CaseA4_2TWJs_w1_gt_w2() throws Exception {
        // Set up the testing environment
        w1Size = 20;
        w1Slide = 20;
        w2Size = 15;
        w2Slide = 15;

        String testCase = "A4_2TWJs";
        // Execute each join operation with timestamp propagation of A
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // Execute each join operation with timestamp propagation of B
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A4 W1 != W2, W2.s >= W2.l and W1.l >= W2.l
    public void CaseA4_slides_neq_w1_gt_w2() throws Exception {
        // Set up the testing environment
        w1Size = 20;
        w1Slide = 25;
        w2Size = 15;
        w2Slide = 15;
        timePropagation = "A";
        String testCase = "A4_slides_neq_w1_gt_w2";
        // Execute each join operation with timestamp propagation of A
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // Execute each join operation with timestamp propagation of B
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A4 W1 != W2, W2.s >= W2.l and W1.l >= W2.l
    public void CaseA4_2TWs_w1_lt_w2() throws Exception {
        // Set up the testing environment
        w1Size = 10;
        w1Slide = 10;
        w2Size = 25;
        w2Slide = 25;

        String testCase = "A4_2TWs_w1_lt_w2";
        // Execute each join operation with timestamp propagation of A
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // Execute each join operation with timestamp propagation of B
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A4 W1 != W2, W2.s >= W2.l and W1.l >= W2.l
    public void CaseA4_slides_neq_w1_lt_w2_s1_lt_s2() throws Exception {
        // Set up the testing environment
        w1Size = 10;
        w1Slide = 15;
        w2Size = 25;
        w2Slide = 35;

        String testCase = "A4_w1_lt_w2_s1_lt_s2";
        // Execute each join operation with timestamp propagation of A
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // Execute each join operation with timestamp propagation of B
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A4 W1 != W2, W2.s >= W2.l and W1.l >= W2.l
    public void CaseA4_slide_neq_w1_lt_w2_s1_gt_s2() throws Exception {
        // Set up the testing environment
        w1Size = 10;
        w1Slide = 45;
        w2Size = 25;
        w2Slide = 25;

        String testCase = "A4_w1_lt_w2_s1_gt_s2";
        // Execute each join operation with timestamp propagation of A
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // Execute each join operation with timestamp propagation of B
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A4 W1 != W2, W2.s >= W2.l and W1.l >= W2.l
    public void CaseA4_slide_neq_w1_eq_w2() throws Exception {
        // Set up the testing environment
        w1Size = 20;
        w1Slide = 20;
        w2Size = 20;
        w2Slide = 30;

        String testCase = "A4_w1_eq_w2";
        // Execute each join operation with timestamp propagation of A
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // Execute each join operation with timestamp propagation of B
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A5: Session Windows
    public void CaseA5() throws Exception {
        streamA = env.fromElements(
                new Tuple3<>(1, 2, (5*60000L)),
                new Tuple3<>(1, 3, (14*60000L))
        ).assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(1000));

        streamB = env.fromElements(
                new Tuple3<>(1, 2, (2*60000L)),
                new Tuple3<>(1, 3, (5*60000L)),
                new Tuple3<>(1, 2, (9*60000L)),
                new Tuple3<>(1, 3, (14*60000L)),
                new Tuple3<>(1, 2, (18*60000L)),
                new Tuple3<>(1, 3, (26*60000L)),
                new Tuple3<>(1, 2, (40*60000L)),
                new Tuple3<>(1, 3, (47*60000L)),
                new Tuple3<>(1, 2, (49*60000L)),
                new Tuple3<>(1, 3, (57*60000L))
        ).assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(1000));

        streamC = env.fromElements(
                new Tuple3<>(1, 2, (8*60000L)),
                new Tuple3<>(1, 3, (10*60000L)),
                new Tuple3<>(1, 2, (13*60000L)),
                new Tuple3<>(1, 3, (15*60000L)),
                new Tuple3<>(1, 2, (18*60000L)),
                new Tuple3<>(1, 3, (20*60000L)),
                new Tuple3<>(1, 3, (23*60000L)),
                new Tuple3<>(1, 3, (25*60000L)),
                new Tuple3<>(1, 2, (28*60000L)),
                new Tuple3<>(1, 3, (30*60000L)),
                new Tuple3<>(1, 2, (32*60000L)),
                new Tuple3<>(1, 3, (35*60000L)),
                new Tuple3<>(1, 2, (37*60000L)),
                new Tuple3<>(1, 3, (42*60000L)),
                new Tuple3<>(1, 3, (47*60000L)),
                new Tuple3<>(1, 2, (53*60000L)),
                new Tuple3<>(1, 3, (55*60000L))
        ).assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(1000));

        w1Size = 5;
        w2Size = 5;

        String testCase = "A5_";
        // Execute each join operation
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SeWJ_ABC(streamA, streamB, streamC, w1Size, w2Size, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SeWJ_BCA(streamA, streamB, streamC, w1Size, w2Size, timePropagation).run();

        // Execute each join operation with timestamp propagation of B
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SeWJ_ABC(streamA, streamB, streamC, w1Size, w2Size, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SeWJ_BCA(streamA, streamB, streamC, w1Size, w2Size, timePropagation).run();

        String outputPath = "./src/main/resources/result_SeWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the result
        assertNotEquals(resultABC_a,resultBCA_a);
        assertNotEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A6 IVJ lB = uB
    public void CaseA6_lB_eq_uB() throws Exception {
        // Set up the testing environment
        w1Size = -10; // lB W1
        w1Slide = 10; // uB W1
        w2Size = -10; // lB W1
        w2Slide = 10; // uB W1

        String testCase = "A6_lB_eq_uB";
        // Execute each join operation with timestamp propagation of A
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new IVJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new IVJ_BCA(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // Execute each join operation with timestamp propagation of B
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new IVJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new IVJ_BCA(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_IVJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A6 IVJ lB = uB
    public void CaseA6_w1_neq_w2() throws Exception {
        // Set up the testing environment
        w1Size = -6; // lB W1
        w1Slide = 6; // uB W1
        w2Size = -10; // lB W1
        w2Slide = 10; // uB W1

        String testCase = "A6_lB_eq_uB_w1_eq_w2";
        // Execute each join operation with timestamp propagation of A
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new IVJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new IVJ_BCA(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // Execute each join operation with timestamp propagation of B
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new IVJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new IVJ_BCA(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_IVJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A6 IVJ lB = uB
    public void CaseA7() throws Exception {
        // Set up the testing environment
        w1Size = -5; // lB W1
        w1Slide = 0; // uB W1
        w2Size = -7; // lB W1
        w2Slide = 7; // uB W1


        String testCase = "A7_lB_eq_uB_w1_neq_w2";
        // Execute each join operation with timestamp propagation of A
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new IVJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new IVJ_BCA(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // Execute each join operation with timestamp propagation of B
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new IVJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new IVJ_BCA(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_IVJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertNotEquals(resultABC_b,resultBCA_b);
    }
}
