package src;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.WritableComparable;

public class WCWritable implements WritableComparable<WCWritable> {

    private String word;

    // Constructor 1
    public WCWritable() {
        set("");
    }

    // Constructor 2
    public WCWritable(String word) {
        set(word);
    }

    // setter
    public void set(String word) {
        this.word = word;
    }

    // getter
    public String getWord() {
        return this.word;
    }

    // how to write values to this class
    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, this.word);
    }

    // how to read values to this class
    @Override
    public void readFields(DataInput in) throws IOException {
        this.word = WritableUtils.readString(in);
    }
    
    @Override
    public int compareTo(WCWritable o) {
        return this.word.compareTo(o.getWord());
    }

    public String toString() {
        return this.word;
    }

}
