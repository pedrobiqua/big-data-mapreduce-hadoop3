package advanced.customwritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.checkerframework.checker.units.qual.Temperature;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
public class FireAvgTempWritable
        implements WritableComparable<FireAvgTempWritable> {
    // Atributos privados
    private float somaTemperaturas;
    private int qtd;
    // Construtor vazio
    public FireAvgTempWritable() {
    }
    public FireAvgTempWritable(float somaTemperaturas, int qtd) {
        this.somaTemperaturas = somaTemperaturas;
        this.qtd = qtd;
    }
    // gets e sets de todos os atributos
    public float getSomaTemperaturas() {
        return somaTemperaturas;
    }
    public void setSomaTemperaturas(float somaTemperaturas) {
        this.somaTemperaturas = somaTemperaturas;
    }
    public int getQtd() {
        return qtd;
    }
    public void setQtd(int qtd) {
        this.qtd = qtd;
    }
    @Override
    public int compareTo(FireAvgTempWritable o) {
// manter essa implemementacao independentemente da classe e dos atributos
        return Integer.compare(o.hashCode(), this.hashCode());
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
// escrevendo em uma ordem desejada
        dataOutput.writeFloat(somaTemperaturas);
        dataOutput.writeInt(qtd);
    }
    @Override
    public void readFields(DataInput dataInput) throws IOException {
// lendo na mesma ordem com que os dados foram escritos anteriormente
        somaTemperaturas = dataInput.readFloat();
        qtd = dataInput.readInt();
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FireAvgTempWritable that = (FireAvgTempWritable) o;
        return Float.compare(that.somaTemperaturas, somaTemperaturas) == 0 && qtd
                == that.qtd;
    }
    @Override
    public int hashCode() {
        return Objects.hash(somaTemperaturas, qtd);
    }
}