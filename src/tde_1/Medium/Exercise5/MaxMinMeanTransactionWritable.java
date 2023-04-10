package tde_1.Medium.Exercise5;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
public class MaxMinMeanTransactionWritable implements WritableComparable<MaxMinMeanTransactionWritable> {
    private float price;
    private int qtd;

    public MaxMinMeanTransactionWritable(){}

    public MaxMinMeanTransactionWritable(float price, int qtd) {
        this.price = price;
        this.qtd = qtd;
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

    @Override
    public int compareTo(MaxMinMeanTransactionWritable o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(price);
        dataOutput.writeInt(qtd);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        price = dataInput.readFloat();
        qtd = dataInput.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MaxMinMeanTransactionWritable that = (MaxMinMeanTransactionWritable) o;
        return Float.compare(that.price, price) == 0 && qtd == that.qtd;
    }

    @Override
    public int hashCode() {
        return Objects.hash(price, qtd);
    }
}