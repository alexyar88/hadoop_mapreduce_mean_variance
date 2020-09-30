package ru.mail.data;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MeanVarReducer extends Reducer<Text, CountMeanVarWritable, Text, CountMeanVarWritable> {
    @Override
    public void reduce(Text key, Iterable<CountMeanVarWritable> values, Context context) throws IOException, InterruptedException {
        double mean = 0;
        int count = 0;
        double var = 0;

        for (CountMeanVarWritable tuple : values) {
            double priceMean = tuple.mean();
            int priceCount = tuple.count();
            double priceVar = tuple.var();

            var = (priceVar * priceCount + count * var) / (count + priceCount) + priceCount * count * Math.pow((mean - priceMean) / (count + priceCount), 2);
            mean = (priceMean * priceCount + mean * count) / (count + priceCount);

            count += priceCount;
        }

        CountMeanVarWritable tuple = new CountMeanVarWritable(count, mean, var);
        context.write(key, tuple);
    }
}
