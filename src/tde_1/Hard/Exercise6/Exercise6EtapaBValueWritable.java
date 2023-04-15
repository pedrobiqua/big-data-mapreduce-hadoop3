package tde_1.Hard.Exercise6;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class Exercise6EtapaBValueWritable implements WritableComparable<Exercise6EtapaBValueWritable> {

    private float priceComm;
    private String country;

    public Exercise6EtapaBValueWritable() {}

    public Exercise6EtapaBValueWritable(float priceComm, String country) {
        this.priceComm = priceComm;
        this.country = country;
    }

    public float getPriceComm() {
        return priceComm;
    }

    public void setPriceComm(float priceComm) {
        this.priceComm = priceComm;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    @Override
    public int compareTo(Exercise6EtapaBValueWritable o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(priceComm);
        dataOutput.writeUTF(country);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        priceComm = dataInput.readFloat();
        country = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Exercise6EtapaBValueWritable that = (Exercise6EtapaBValueWritable) o;
        return Float.compare(that.priceComm, priceComm) == 0 && Objects.equals(country, that.country);
    }

    @Override
    public int hashCode() {
        return Objects.hash(priceComm, country);
    }
}