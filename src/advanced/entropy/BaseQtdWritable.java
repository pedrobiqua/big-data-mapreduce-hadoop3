package advanced.entropy;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import java.util.regex.Pattern;

public class BaseQtdWritable
        implements WritableComparable<BaseQtdWritable> {


    // Atributos privados
    // Construtor vazio
    // Gets e Sets
    // equals e hashcode

    private String caracter;
    private long contagem;

    public BaseQtdWritable() {
    }

    public BaseQtdWritable(String caracter, long contagem) {
        this.caracter = caracter;
        this.contagem = contagem;
    }

    public String getCaracter() {
        return caracter;
    }

    public void setCaracter(String caracter) {
        this.caracter = caracter;
    }

    public long getContagem() {
        return contagem;
    }

    public void setContagem(long contagem) {
        this.contagem = contagem;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BaseQtdWritable that = (BaseQtdWritable) o;
        return contagem == that.contagem && Objects.equals(caracter, that.caracter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(caracter, contagem);
    }

    @Override
    public int compareTo(BaseQtdWritable o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(caracter);
        dataOutput.writeLong(contagem);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        caracter = dataInput.readUTF();
        contagem = dataInput.readLong();
    }
}