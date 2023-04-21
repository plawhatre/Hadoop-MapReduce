package src;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper2 extends Mapper<Text, Text, Text, IntWritable> {
  public void map(Text key, Text vals, Context con) throws IOException, InterruptedException {
    con.write(vals, new IntWritable(1));
  }
}