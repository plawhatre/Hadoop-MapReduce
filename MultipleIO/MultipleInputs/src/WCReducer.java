package src;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
  public void reduce(Text key, Iterable<IntWritable> vals, Context con) throws IOException, InterruptedException {
    int count = 0;
    for (IntWritable val : vals)
      count += val.get(); 
    con.write(key, new IntWritable(count));
  }
}
