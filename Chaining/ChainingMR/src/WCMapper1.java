package src;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(",");

        for (String word : words) {
            Text OutputKey = new Text(word.toUpperCase().trim());
            IntWritable OutputValue = new IntWritable(1);

            con.write(OutputKey, OutputValue);
        }
    }
}
