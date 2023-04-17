package tde_1.Medium.Exercise5;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class Exercise5KeyWritable implements WritableComparable<Exercise5KeyWritable> {

    private String unityType;
    private String year;

    public Exercise5KeyWritable() {
    }

    public Exercise5KeyWritable(String unityType, String year) {
        this.unityType = unityType;
        this.year = year;

    }

    public String getUnityType() {
        return unityType;
    }

    public void setUnityType(String unityType) {
        this.unityType = unityType;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    @Override
    public int compareTo(Exercise5KeyWritable o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(unityType);
        dataOutput.writeUTF(year);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        unityType = dataInput.readUTF();
        year = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Exercise5KeyWritable that = (Exercise5KeyWritable) o;
        return Objects.equals(unityType, that.unityType) && Objects.equals(year, that.year);
    }

    @Override
    public int hashCode() {
        return Objects.hash(unityType, year);
    }
}
