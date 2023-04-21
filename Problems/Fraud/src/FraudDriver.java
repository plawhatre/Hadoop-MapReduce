package src;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;

public class FraudDriver{
    public static void main(String[] args) throws Exception{
        Configuration c = new Configuration();

        Job j = Job.getInstance(c, "WordCount");
        j.setJarByClass(FraudDriver.class);
        j.setMapperClass(FraudMapper.class);
        j.setReducerClass(FraudReducer.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(FraudWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);

        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true)?0:1);
    }
}