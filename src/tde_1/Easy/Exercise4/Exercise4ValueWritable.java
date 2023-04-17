package tde_1.Easy.Exercise4;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class Exercise4ValueWritable
        implements WritableComparable<Exercise4ValueWritable> {
    // Atributos privados
    private float priceComm;
    private int qntd;

    // Construtor vazio
    public Exercise4ValueWritable() {
    }

    public Exercise4ValueWritable(float priceComm, int qntd) {
        this.priceComm = priceComm;
        this.qntd = qntd;
    }

    // gets e sets de todos os atributos

    public float getPriceComm() {
        return priceComm;
    }

    public void setPriceComm(float priceComm) {
        this.priceComm = priceComm;
    }

    public int getQntd() {
        return this.qntd;
    }

    public void setQntd(int qntd) {
        this.qntd = qntd;
    }

    @Override
    public int compareTo(Exercise4ValueWritable o) {
        // manter essa implemementacao independentemente da classe e dos atributos
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // escrevendo em uma ordem desejada
        dataOutput.writeFloat(priceComm);
        dataOutput.writeInt(qntd);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // lendo na mesma ordem com que os dados foram escritos anteriormente
        priceComm = dataInput.readFloat();
        qntd = dataInput.readInt();

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Exercise4ValueWritable that = (Exercise4ValueWritable) o;
        return Float.compare(that.priceComm, priceComm) == 0 && qntd == that.qntd;
    }

    @Override
    public int hashCode() {
        return Objects.hash(priceComm, qntd);
    }
}