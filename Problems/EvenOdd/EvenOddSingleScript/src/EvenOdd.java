package src;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;


public class EvenOdd
{
    public static class EvenOddMapper extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        public void map(LongWritable key, Text values, Context con) throws IOException, InterruptedException
        {
            String line = values.toString();
            String[] numbers = line.split(",");
            
            for (String number: numbers)
            {
                int new_number = Integer.parseInt(number);
                

                if (new_number % 2 == 0)
                {
                    Text OutputKey = new Text("Even");
                    IntWritable OutputValue =  new IntWritable(new_number);
                    con.write(OutputKey, OutputValue);
                } else
                {
                    Text OutputKey = new Text("Odd");
                    IntWritable OutputValue =  new IntWritable(new_number);
                    con.write(OutputKey, OutputValue);
                }
                

            }
        }
    }

    public static class EvenOddReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        public void reduce(Text key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException
        {
            int sum = 0;

            for (IntWritable value: values)
            {
                sum += value.get();
            }
            con.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "even odd");
        job.setJarByClass(EvenOdd.class);
        job.setMapperClass(EvenOddMapper.class);
        job.setReducerClass(EvenOddReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true)?0:1);
    }
}


