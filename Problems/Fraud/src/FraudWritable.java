package src;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class FraudWritable implements WritableComparable<FraudWritable>{

    private String name; 
    private String rxDate; 
    private boolean returned;
    private String retDate;

    // Constructor 1
    public FraudWritable() {
        set("", "", "no", "");
    }

    // Constructor 2
    public FraudWritable(String name, String rxDate, String returned, String retDate) {
        set(name, rxDate, returned, retDate);
    }

    // setter
    public void set(String name, String rxDate, String returned, String retDate) {
        this.name = name;
        this.rxDate = rxDate;

        if (returned.equalsIgnoreCase("no")) {
            this.returned = false;
        } else {
            this.returned = true;
        }

        this.retDate = retDate;
    }

    // getter
    public String getName() {
        return this.name;
    }

    public String getReceivedDate() {
        return this.rxDate;
    }

    public boolean getReturned() {
        return this.returned;
    }

    public String getReturnDate() {
        return this.retDate;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.name = WritableUtils.readString(in);
        this.rxDate = WritableUtils.readString(in);
        this.returned = in.readBoolean();
        this.retDate = WritableUtils.readString(in);
        
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, this.name);
        WritableUtils.writeString(out, this.rxDate);
        out.writeBoolean(this.returned);
        WritableUtils.writeString(out, this.retDate);
    }

    @Override
    public int compareTo(FraudWritable o) {
        String b1 = Boolean.toString(this.returned);
        String b2 = Boolean.toString(o.getReturned());

        int cmp1 = this.name.compareTo(o.getName());
        int cmp2 = this.rxDate.compareTo(o.getReceivedDate());
        int cmp3 = b1.compareTo(b2);
        int cmp4 = this.retDate.compareTo(o.getReturnDate());

        if (cmp1 != 0) {
            return cmp1;
        } else  if (cmp2 != 0) {
            return cmp2;
        } else if (cmp3 != 0) {
            return cmp3;
        } 
        return cmp4;
    }

    @Override
    public String toString() {
        return name + "," + retDate + "," + returned + "," + rxDate;
    }
}