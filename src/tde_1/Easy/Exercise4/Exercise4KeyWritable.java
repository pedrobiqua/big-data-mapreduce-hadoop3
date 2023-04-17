package tde_1.Easy.Exercise4;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class Exercise4KeyWritable
        implements WritableComparable<Exercise4KeyWritable> {
    // Atributos privados
    private String flow;
    private int year;
    private String unitType;
    private String category;
    private String country;

    // Construtor vazio
    public Exercise4KeyWritable() {
    }

    public Exercise4KeyWritable(String country, String flow, int year, String unitType,
                                String category) {
        this.country = country;
        this.flow = flow;
        this.year = year;
        this.unitType = unitType;
        this.category = category;
    }

    // gets e sets de todos os atributos

    public String getFlow() {
        return this.flow;
    }

    public String getCountry() {
        return this.country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public void setFlow(String flow) {
        this.flow = flow;
    }

    public int getYear() {
        return this.year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public String getUnitType() {
        return this.unitType;
    }

    public void setUnitType(String unitType) {
        this.unitType = unitType;
    }

    public String getCategory() {
        return this.category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    @Override
    public int compareTo(Exercise4KeyWritable o) {
        // manter essa implemementacao independentemente da classe e dos atributos
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // escrevendo em uma ordem desejada
        dataOutput.writeUTF(country);
        dataOutput.writeUTF(flow);
        dataOutput.writeInt(year);
        dataOutput.writeUTF(unitType);
        dataOutput.writeUTF(category);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // lendo na mesma ordem com que os dados foram escritos anteriormente
        country = dataInput.readUTF();
        flow = dataInput.readUTF();
        year = dataInput.readInt();
        unitType = dataInput.readUTF();
        category = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Exercise4KeyWritable that = (Exercise4KeyWritable) o;
        return year == that.year && Objects.equals(flow, that.flow);
    }

    @Override
    public int hashCode() {
        return Objects.hash(flow, year);
    }
}