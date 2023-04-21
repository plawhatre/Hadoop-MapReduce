package src;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

    public static void main(String[] args) throws Exception {
        Configuration c = new Configuration();

        Job j = Job.getInstance(c, "WordCount");
        j.setJarByClass(WordCount.class);

        ChainMapper.addMapper(j, WCMapper1.class, LongWritable.class, Text.class, Text.class, IntWritable.class, c);

        ChainMapper.addMapper(j, WCMapper2.class, Text.class, IntWritable.class, Text.class, IntWritable.class, c);

        ChainReducer.setReducer(j, WCReducer.class, Text.class, IntWritable.class, Text.class, IntWritable.class, c);

        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path(files[0]);
        Path output = new Path(files[1],"j1");
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        output.getFileSystem(j.getConfiguration()).delete(output, true);

        j.waitForCompletion(true);

        // MR2
        Configuration c2 = new Configuration();
        Job j2 = Job.getInstance(c2, "fWordCount");

        j2.setJarByClass(WordCount.class);
        j2.setMapperClass(FinalMapper.class);
        j2.setReducerClass(FinalReducer.class);
        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(IntWritable.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(j2, new Path(files[1],"j1"));
        FileOutputFormat.setOutputPath(j2, new Path(files[1],"j2"));

        System.exit(j2.waitForCompletion(true) ? 0 : 1);
    }
}
