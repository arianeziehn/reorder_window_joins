package util;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;


public class ArtificialSourceFunction extends RichParallelSourceFunction<Tuple3<Integer, Integer, Long>> {
    private volatile boolean isRunning = true;
    public static final int RECORD_SIZE_IN_BYTE = 16;
    private final long throughput;
    boolean manipulateIngestionRate;
    private int numberOfKeys;
    private long startTime;
    private final int runtime;
    double freq; // tuples per minute


    public ArtificialSourceFunction(long throughput, int runtime, double freq, int numberOfKeys) {
        this.numberOfKeys = numberOfKeys;
        this.runtime = runtime;
        this.throughput = throughput;
        this.freq = freq;
        this.manipulateIngestionRate = throughput != 0;
    }

    /***
     * Run method of the source, creates a stream of Tuple3()
     * runs for the period of runtime, e.g., 20 Minutes
     * assign uniform distributed event time stamps
     * @param sourceContext
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Tuple3<Integer, Integer, Long>> sourceContext) throws Exception {
        long start = System.currentTimeMillis();
        this.startTime = start;
        boolean run = true;
        long tupleCounter = 0;
        long millisSinceEpoch = 0L;

        Random r = new Random();
        while (run) {
            long now = System.currentTimeMillis();
            Integer value = r.nextInt();
            long eventTime = millisSinceEpoch;
            int key = 0;

            int maxPara = this.getRuntimeContext().getNumberOfParallelSubtasks();
            if (this.numberOfKeys == 0) {
                this.numberOfKeys = this.getRuntimeContext().getNumberOfParallelSubtasks();
            }
            if (this.numberOfKeys >= maxPara) {
                for (int i = 0; i < (this.numberOfKeys / maxPara); i++) {
                    key = this.getRuntimeContext().getIndexOfThisSubtask() + (maxPara * i);
                    Tuple3<Integer, Integer, Long> event = new Tuple3<>(key, value, eventTime);
                    sourceContext.collect(event); // output tuple
                    tupleCounter++; // increase tuple counter
                }
            } else {
                run = false;
                //TODO
            }

            millisSinceEpoch += ((60000L * 60) / freq); //increase event time by 1 minute/ freq

            // check if the tuple counts equals the defined throughput (per second)
            if (tupleCounter >= throughput && manipulateIngestionRate) {
                // if tuples were creates faster then in 1 second, wait
                if (((1000 - (now - start)) > 0) && ((now - this.startTime) < this.runtime * 60000L)) {
                    Thread.sleep(1000 - (now - start));
                    // in case runtime is reached stop producing tuples
                }
                // reset parameters
                tupleCounter = 0;
                start = System.currentTimeMillis();
            }
            if ((now - this.startTime) >= this.runtime * 60000L) {
                cancel();
                run = false;
                break;
            }
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}
