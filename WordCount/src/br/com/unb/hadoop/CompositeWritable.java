package br.com.unb.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CompositeWritable implements Writable {
    int count = 0;
    double valor = 0d;

    public CompositeWritable() {}

    public CompositeWritable(int val1, double val2) {
        this.count = val1;
        this.valor = val2;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        count = in.readInt();
        valor = in.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(count);
        out.writeDouble(valor);
    }

//    public void merge(CompositeWritable other) {
//        this.count += other.count;
//        this.valor += other.valor;
//    }

    @Override
    public String toString() {
        return this.count + "\t" + this.valor;
    }
}
