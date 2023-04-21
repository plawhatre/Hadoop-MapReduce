package src;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class FraudMapper extends Mapper<LongWritable, Text, Text, FraudWritable>{
    public void map(LongWritable key, Text values, Context con) throws IOException, InterruptedException{
        
        String line = values.toString();
        String[] features = line.split(",");

        Text outputKey = new Text();
        FraudWritable outputValues = new FraudWritable();

        outputKey.set(features[0]);
        outputValues.set(features[1], features[5], features[6], features[7]);
        
        con.write(outputKey, outputValues);
    }
}