package br.com.unb.hadoop;
 
import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class BeneficiosPagosEstadoReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> 
 {
 
    public void reduce(Text text, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        BigDecimal sum = new BigDecimal(0);
        for (DoubleWritable value : values) {
        	sum = sum.add(BigDecimal.valueOf(value.get()));
        }
        context.write(text, new DoubleWritable(sum.doubleValue()));
    }
}