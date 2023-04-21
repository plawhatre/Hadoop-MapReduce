package src;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import org.apache.hadoop.fs.FileSystem;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.util.HashMap;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;


public class HikeMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private HashMap<String, Double> percentageHikePerDesig = new HashMap<>();

    @Override
    public void setup(Context context) throws IOException, InterruptedException {

        URI[] cacheFile = context.getCacheFiles(); 

        if (cacheFile!=null && cacheFile.length > 0) {
                // 
                try{
                    //
                    String line = ""; 
                    FileSystem fs = FileSystem.get(context.getConfiguration());
                    Path getFilePath = new Path(cacheFile[0].toString());
                    
                    BufferedReader myReader = new BufferedReader(
                        new InputStreamReader(fs.open(getFilePath)));
                                        
                    while((line = myReader.readLine())!= null) {
                        String[] words = line.split(",");
                        percentageHikePerDesig.put(words[0].toString(), 
                                               Double.parseDouble(words[1].toString()));
                    }

                    myReader.close();
                } 
                catch (FileNotFoundException e) {
                    System.out.println("An error occured");
                }
            }
        // super.setup(context);            
    }

    public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
        // 
        String line = values.toString();
        String[] words = line.split(",");

        String designation = words[2];
        Double n = 1.0;

        if(designation.equalsIgnoreCase("manager")) {
            n = percentageHikePerDesig.get("MGR");   
        } else if (designation.equalsIgnoreCase("developer")) {
            n = percentageHikePerDesig.get("DLP");
        } else {
            n = percentageHikePerDesig.get("HHR");
        }

        double hike = (n/100) * Double.parseDouble(words[3]);
        context.write(new Text(designation), new DoubleWritable(hike));
    }
}