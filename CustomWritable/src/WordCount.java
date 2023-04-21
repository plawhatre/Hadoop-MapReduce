package src;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount{
    public static void main(String[] args) throws Exception{
        Configuration c = new Configuration();

        Job j = Job.getInstance(c, "WordCount");
        j.setJarByClass(WordCount.class);
        j.setMapperClass(WordCountMapper.class);
        j.setReducerClass(WordCountReducer.class);

        j.setMapOutputKeyClass(WCWritable.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(WCWritable.class);
        j.setOutputValueClass(IntWritable.class);

        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // output.getFileSystem(j.getConfiguration()).delete(output, true);
        System.exit(j.waitForCompletion(true)?0:1);
    }
}
