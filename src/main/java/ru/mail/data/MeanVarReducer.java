package ru.mail.data;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MeanVarReducer extends Reducer<Text, CountMeanVarWritable, Text, CountMeanVarWritable> {
    @Override
    public void reduce(Text key, Iterable<CountMeanVarWritable> values, Context context) throws IOException, InterruptedException {
        int currentCount = 0;
        double currentMean = 0;
        double currentVar = 0;

        for (CountMeanVarWritable tuple : values) {
            int newCount = tuple.count();
            double newMean = tuple.mean();
            double newVar = tuple.var();

            currentVar = (newVar * newCount + currentCount * currentVar) / (currentCount + newCount) +
                    newCount * currentCount * Math.pow((currentMean - newMean) / (currentCount + newCount), 2);

            currentMean = (newMean * newCount + currentMean * currentCount) / (currentCount + newCount);

            currentCount += newCount;
        }

        CountMeanVarWritable tuple = new CountMeanVarWritable(currentCount, currentMean, currentVar);
        context.write(key, tuple);
    }
}
