package tde_1.Easy.Exercise2;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
public class TransactionsFlowAndYearTempWritable
        implements WritableComparable<TransactionsFlowAndYearTempWritable> {
    // Atributos privados
    private String flow;
    private int year;

    // Construtor vazio
    public TransactionsFlowAndYearTempWritable() {
    }
    public TransactionsFlowAndYearTempWritable(String flow, int year) {
        this.flow = flow;
        this.year = year;
    }
    // gets e sets de todos os atributos
    public String getFlow() {
        return flow;
    }
    public void setFlow(String flow) {
        this.flow = flow;
    }
    public int getQtd() {
        return year;
    }
    public void setYear(int year) {
        this.year = year;
    }
    @Override
    public int compareTo(TransactionsFlowAndYearTempWritable o) {
    // manter essa implemementacao independentemente da classe e dos atributos
        return Integer.compare(o.hashCode(), this.hashCode());
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
    // escrevendo em uma ordem desejada
        dataOutput.writeUTF(flow);
        dataOutput.writeInt(year);
    }
    @Override
    public void readFields(DataInput dataInput) throws IOException {
    // lendo na mesma ordem com que os dados foram escritos anteriormente
        flow = dataInput.readUTF();
        year = dataInput.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransactionsFlowAndYearTempWritable that = (TransactionsFlowAndYearTempWritable) o;
        return year == that.year && Objects.equals(flow, that.flow);
    }

    @Override
    public int hashCode() {
        return Objects.hash(flow, year);
    }
}