package tde_1.Hard.Exercise6;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
public class Exercise6EtapaAValueWritable implements WritableComparable<Exercise6EtapaAValueWritable> {

    private float priceComm;
    private int qtd;

    private float max;

    public Exercise6EtapaAValueWritable() {}

    public Exercise6EtapaAValueWritable(float priceComm, int qtd) {
        this.priceComm = priceComm;
        this.qtd = qtd;
    }

    public Exercise6EtapaAValueWritable(float priceComm, int qtd, float max) {
        this.priceComm = priceComm;
        this.qtd = qtd;
        this.max = max;
    }

    public float getPriceComm() {
        return priceComm;
    }

    public void setPriceComm(float priceComm) {
        this.priceComm = priceComm;
    }

    public int getQtd() {
        return qtd;
    }

    public void setQtd(int qtd) {
        this.qtd = qtd;
    }

    public float getMax() {
        return max;
    }

    public void setMax(float max) {
        this.max = max;
    }

    @Override
    public int compareTo(Exercise6EtapaAValueWritable o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(priceComm);
        dataOutput.writeInt(qtd);
        dataOutput.writeFloat(max);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        priceComm = dataInput.readFloat();
        qtd = dataInput.readInt();
        max = dataInput.readFloat();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Exercise6EtapaAValueWritable that = (Exercise6EtapaAValueWritable) o;
        return Float.compare(that.priceComm, priceComm) == 0 && qtd == that.qtd && Float.compare(that.max, max) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(priceComm, qtd, max);
    }
}