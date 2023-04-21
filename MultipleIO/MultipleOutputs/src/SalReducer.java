package src;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class SalReducer extends Reducer<Text, Text, Text, Text> {
    private MultipleOutputs<Text, Text> out;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        out = new MultipleOutputs<>(context);
    }

    public void reduce(Text key, Iterable<Text> vals, Context con) throws IOException, InterruptedException {
        String name = "" ;
        String dept = "" ;
        float totalSal = 0.0f;

        for (Text val : vals){
            String[] words = val.toString().split(",");
            name = words[0];
            dept = words[1];
            totalSal += totalSal + Float.parseFloat(words[2]); 
        }

        if (dept.equalsIgnoreCase("hr")) {
            out.write(key, new Text(name+","+totalSal), "HR");
        }

        if (dept.equalsIgnoreCase("accounts")) {
            out.write(key, new Text(name+","+totalSal), "Accounts");
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException{
        out.close();
    }
}
