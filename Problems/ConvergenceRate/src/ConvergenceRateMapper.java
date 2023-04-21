package src;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;



public class ConvergenceRateMapper extends Mapper<LongWritable, Text, Text, Text>{
    public void map(LongWritable key, Text values, Context con) throws IOException, InterruptedException{
        String line = values.toString();
        String[] features = line.split(",");

        Text OutputKey = new Text(features[3]);
        Text OutputValue = new Text(features[2]+","+ features[4]+","+ features[5]);

        con.write(OutputKey, OutputValue);
    }
}

