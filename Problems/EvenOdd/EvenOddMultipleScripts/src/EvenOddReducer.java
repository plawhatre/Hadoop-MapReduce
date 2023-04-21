package src;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class EvenOddReducer extends Reducer<Text, IntWritable, Text, IntWritable>
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