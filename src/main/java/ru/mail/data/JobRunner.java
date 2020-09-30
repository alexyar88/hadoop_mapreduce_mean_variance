package ru.mail.data;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class JobRunner {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();
        String input_file = files[0];
        String output_file = files[1];

        Path input = new Path(input_file);
        Path output = new Path(output_file);

        Job job = Job.getInstance(conf, "MeanVarMapReduce");
        job.setJarByClass(JobRunner.class);

        job.setMapperClass(RowMapper.class);
        job.setCombinerClass(MeanVarReducer.class);
        job.setReducerClass(MeanVarReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(CountMeanVarWritable.class);
        job.setMapOutputValueClass(CountMeanVarWritable.class);

        // Чтобы предыдущие результаты не удалять вручную каждый раз
        FileSystem.get(conf).delete(output, true);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        boolean status = job.waitForCompletion(true);

        if (status) {
            System.exit(0);
        } else {
            System.exit(1);
        }

    }

}
