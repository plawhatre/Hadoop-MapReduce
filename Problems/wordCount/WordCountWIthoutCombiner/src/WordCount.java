package src;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount
{
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
        {
            String line = value.toString();
            String[] words = line.split(",");

            for (String word: words)
            {
                Text OutputKey = new Text(word.toUpperCase().trim());
                IntWritable OutputValue = new IntWritable(1);

                con.write(OutputKey, OutputValue);
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException
        {
          int sum = 0;
          
          for (IntWritable value: values)
          {
              sum += value.get();
          }

          con.write(word, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration c = new Configuration();

        Job j = Job.getInstance(c, "WordCount");
        j.setJarByClass(WordCount.class);
        j.setMapperClass(WordCountMapper.class);
        j.setReducerClass(WordCountReducer.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true)?0:1);
    }
}
