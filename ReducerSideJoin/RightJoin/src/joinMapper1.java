package src;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

public class joinMapper1 extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text val, Context con) throws IOException, InterruptedException  {
        // 
        String line = val.toString();
        String[] words = line.split(",");

        Text id =  new Text(words[5]);
        Text data = new Text("Emp,"+words[0]+" "+words[1]+" "+words[2]+" "+words[3]+" "+words[4]);

        con.write(id, data);
    }
}