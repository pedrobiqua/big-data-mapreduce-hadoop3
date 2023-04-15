package tde_1.Hard.Exercise7;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class Exercise7KeyWritable implements WritableComparable<Exercise7KeyWritable> {

    private String flow;
    private String commName;

    public Exercise7KeyWritable() {
    }

    public Exercise7KeyWritable(String flow, String commName) {
        this.flow = flow;
        this.commName = commName;
    }

    public String getFlow() {
        return this.flow;
    }

    public void setFlow(String flow) {
        this.flow = flow;
    }

    public String getCommName() {
        return this.commName;
    }

    public void setCommName(String commName) {
        this.commName = commName;
    }

    @Override
    public int compareTo(Exercise7KeyWritable o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(flow);
        dataOutput.writeUTF(commName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        flow = dataInput.readUTF();
        commName = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof Exercise7KeyWritable)) {
            return false;
        }
        Exercise7KeyWritable exercise7KeyWritable = (Exercise7KeyWritable) o;
        return Objects.equals(flow, exercise7KeyWritable.flow)
                && Objects.equals(commName, exercise7KeyWritable.commName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(flow, commName);
    }

}
