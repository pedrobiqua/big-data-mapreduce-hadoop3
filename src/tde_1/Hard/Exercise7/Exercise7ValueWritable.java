package tde_1.Hard.Exercise7;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class Exercise7ValueWritable implements WritableComparable<Exercise7ValueWritable> {
    private String nameComm;
    private String flow;
    private float qtdValue;


    public Exercise7ValueWritable() {
    }

    public Exercise7ValueWritable(String nameComm, String flow, float qtdValue) {
        this.nameComm = nameComm;
        this.qtdValue = qtdValue;
        this.flow = flow;
    }

    public String getNameComm() {
        return nameComm;
    }

    public void setNameComm(String nameComm) {
        this.nameComm = nameComm;
    }

    public float getQtdValue() {
        return qtdValue;
    }

    public void setQtdValue(float qtdValue) {
        this.qtdValue = qtdValue;
    }

    public String getFlow() {
        return flow;
    }

    public void setFlow(String flow) {
        this.flow = flow;
    }

    @Override
    public int compareTo(Exercise7ValueWritable o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(nameComm);
        dataOutput.writeUTF(flow);
        dataOutput.writeFloat(qtdValue);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        nameComm = dataInput.readUTF();
        flow = dataInput.readUTF();
        qtdValue = dataInput.readFloat();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Exercise7ValueWritable that = (Exercise7ValueWritable) o;
        return Float.compare(that.qtdValue, qtdValue) == 0 && Objects.equals(nameComm, that.nameComm) && Objects.equals(flow, that.flow);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nameComm, flow, qtdValue);
    }
}