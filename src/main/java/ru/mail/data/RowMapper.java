package ru.mail.data;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Pattern;

public class RowMapper extends Mapper<Object, Text, Text, CountMeanVarWritable> {

    static int FIELD_POSITION = 9;
    static int FIELDS_COUNT = 16;

    static IntWritable one = new IntWritable(1);
    static Pattern splitPattern = Pattern.compile(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
    static Text countMeanVarKey = new Text("count_mean_var");

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = splitPattern.split(value.toString());

        // Если у нас достаточно полей, чтобы найти нужное поле с конца,
        // то пробуем это сделать.
        // Дикий костыль, на самом деле :)
        if (fields.length > (FIELDS_COUNT - FIELD_POSITION)) {
            try {
                // Ищем поле price с конца
                double price = Double.parseDouble(fields[fields.length - (FIELDS_COUNT - FIELD_POSITION)]);
                CountMeanVarWritable tuple = new CountMeanVarWritable(1, price, 0);
                context.write(countMeanVarKey, tuple);
            } catch (Exception e) {
                System.out.println("Mapper exception:");
                System.out.println(e.getMessage());
            }

        }
    }
}
