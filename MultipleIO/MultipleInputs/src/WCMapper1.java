package src;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
  
    public void map(LongWritable key, Text vals, Context con) throws IOException, InterruptedException {
        String line = vals.toString().trim();
        String[] words = line.split(" ");

        for(String word : words) {
            con.write(new Text(word), new IntWritable(1));
        }
    } 
}
