/**
 * (c) dataartisans
 * https://data-artisans.com/blog/robust-stream-processing-flink-walkthrough
 *
 */

package util;

import java.util.Date;

public class KeyedDataPointNASDAQ<Double> extends DataPoint<Double> {

    private String key;
    private int vol;
    private Double high;

    private Double close;
    private Double low;

    public KeyedDataPointNASDAQ(){
        super();
        this.key = null;
    }

    public KeyedDataPointNASDAQ(String key, long timeStampMs, Double value1, Double high, Double low, Double close, int vol) {
        super(timeStampMs, value1);
        this.key = key;
        this.high = high;
        this.low = low;
        this.close = close;
        this.vol = vol;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public int getVol() {
        return vol;
    }

    public void setVol(int vol) {
        this.vol = vol;
    }

    public Double getHigh() {
        return high;
    }

    public void setHigh(Double high) {
        this.high = high;
    }

    public Double getLow() {
        return low;
    }

    public void setLow(Double low) {
        this.low = low;
    }

    public Double getClose() {
        return close;
    }

    public void setClose(Double close) {
        this.close = close;
    }


    @Override
    public String toString() {
        Date date = new Date(getTimeStampMs());
        return date + "," + getKey() + "," + getValue() + ", with daily volumne" + getVol();
        // short in tm
        // return getTimeStampMs() + "," + getKey() + "," + getValue();
    }



}