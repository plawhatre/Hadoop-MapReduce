package src;

import java.io.IOException;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;



public class ConvergenceRateReducer extends Reducer<Text, Text, Text, Text>{
    public void reduce(Text key, Iterable<Text> values, Context con) throws IOException, InterruptedException{
        
        HashMap<String, String> cityData = new HashMap<String, String>();

        Iterator<Text> itr = values.iterator();

        while(itr.hasNext()){
            String features = itr.next().toString();
            String[] words = features.split(",");
            String location = words[0].trim();

            int clicks = Integer.parseInt(words[1].trim());
            int conversion = Integer.parseInt(words[2].trim());
            Double successRate = new Double(100*(conversion)/(1.0*clicks));

            if (cityData.containsKey(location)){
                String records = cityData.get(location).toString();
                String[] vals = records.split(",");

                Double totalSR = Double.parseDouble(vals[0]) + successRate ;
                Integer totalCt = Integer.parseInt(vals[1]) + 1 ;

                cityData.put(location, totalSR+","+totalCt);
            } else {
                cityData.put(location, successRate+",1");
            }
        }

        for(Map.Entry<String, String> entry : cityData.entrySet()){
            String[] record = entry.getValue().split(",");
            Double avgSuccessRate = Double.parseDouble(record[0])/(Integer.parseInt(record[1]));
            con.write(key, new Text(entry.getKey()+","+avgSuccessRate));
        }

    }
}

