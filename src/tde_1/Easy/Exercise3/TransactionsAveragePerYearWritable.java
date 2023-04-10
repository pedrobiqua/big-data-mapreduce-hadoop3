package tde_1.Easy.Exercise3;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
public class TransactionsAveragePerYearWritable
        implements WritableComparable<TransactionsAveragePerYearWritable> {
    // Atributos privados
    private float commValue;
    private int year;

    // Construtor vazio
    public TransactionsAveragePerYearWritable() {
    }
    public TransactionsAveragePerYearWritable(float commValue, int year) {
        this.commValue = commValue;
        this.year = year;
    }
    // gets e sets de todos os atributos
    public float getcommValue() {
        return commValue;
    }
    public void setcommValue(float commValue) {
        this.commValue = commValue;
    }
    public int getQtd() {
        return year;
    }
    public void setYear(int year) {
        this.year = year;
    }
    @Override
    public int compareTo(TransactionsAveragePerYearWritable o) {
        // manter essa implemementacao independentemente da classe e dos atributos
        return Integer.compare(o.hashCode(), this.hashCode());
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // escrevendo em uma ordem desejada
        dataOutput.writeFloat(commValue);
        dataOutput.writeInt(year);
    }
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // lendo na mesma ordem com que os dados foram escritos anteriormente
        commValue = dataInput.readFloat();
        year = dataInput.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransactionsAveragePerYearWritable that = (TransactionsAveragePerYearWritable) o;
        return Float.compare(that.commValue, commValue) == 0 && year == that.year;
    }

    @Override
    public int hashCode() {
        return Objects.hash(commValue, year);
    }
}