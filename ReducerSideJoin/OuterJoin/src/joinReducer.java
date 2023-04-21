package src;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class joinReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> vals, Context con) throws IOException, InterruptedException {

        List<String> emp = new ArrayList<>();
        String dept = "";

        Iterator<Text> itr = vals.iterator();

        while (itr.hasNext()) {
            String[] words = itr.next().toString().split(",");

            if (words[0].equalsIgnoreCase("emp")) {
                emp.add(words[1]);
            } else {
                dept = words[1];
            }
        }
        /*
        Inner: condition 1 

        Left: condition 1,2
        
        Right: condition 1,3
        
        Full Outer: condition 1,2,3
        */ 

        // Condition 1: Inner Join
        if (!emp.isEmpty() && !dept.isEmpty()) {
            for (String record : emp){
                con.write(key, new Text(record + " " + dept));
            }
        }

        // Condition 2: Left Outer Join
        if (!emp.isEmpty() && dept.isEmpty()) {
            for (String record : emp){
                con.write(key, new Text(record + " " + "NULL NULL"));
            }
        }

        // Condition 3: Right Outer Join
        if (emp.isEmpty() && !dept.isEmpty()) {
            con.write(key, new Text("NULL NULL NULL NULL" + " " + dept));
        }
    }
}
