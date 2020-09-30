package ru.mail.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class CountMeanVarWritable implements Writable {
    private final DoubleWritable meanWritable;
    private final IntWritable countWritable;
    private final DoubleWritable varWritable;


    public CountMeanVarWritable(int count, double mean, double var) {
        this.countWritable = new IntWritable(count);
        this.meanWritable = new DoubleWritable(mean);
        this.varWritable = new DoubleWritable(var);
    }

    public CountMeanVarWritable() {
        this.countWritable = new IntWritable();
        this.meanWritable = new DoubleWritable();
        this.varWritable = new DoubleWritable();
    }

    public int count() {
        return countWritable.get();
    }

    public double mean() {
        return meanWritable.get();
    }

    public double var() {
        return varWritable.get();
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append(this.countWritable.get());
        str.append("\t");
        str.append(this.meanWritable.get());
        str.append("\t");
        str.append(this.varWritable.get());
        return str.toString();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        countWritable.readFields(in);
        meanWritable.readFields(in);
        varWritable.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        countWritable.write(out);
        meanWritable.write(out);
        varWritable.write(out);
    }
}