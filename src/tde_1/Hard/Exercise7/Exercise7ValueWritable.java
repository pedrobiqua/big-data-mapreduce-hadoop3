package tde_1.Hard.Exercise7;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class Exercise7ValueWritable implements WritableComparable<Exercise7ValueWritable> {
    private float price;
    private int qtd;
    private float max;
    private float min;

    public Exercise7ValueWritable() {
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
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

    public float getMin() {
        return min;
    }

    public void setMin(float min) {
        this.min = min;
    }

    @Override
    public int compareTo(Exercise7ValueWritable o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(price);
        dataOutput.writeInt(qtd);
        dataOutput.writeFloat(max);
        dataOutput.writeFloat(min);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        price = dataInput.readFloat();
        qtd = dataInput.readInt();
        max = dataInput.readFloat();
        min = dataInput.readFloat();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Exercise7ValueWritable that = (Exercise7ValueWritable) o;
        return Float.compare(that.price, price) == 0 && qtd == that.qtd && Float.compare(that.max, max) == 0
                && Float.compare(that.min, min) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(price, qtd, max, min);
    }
}