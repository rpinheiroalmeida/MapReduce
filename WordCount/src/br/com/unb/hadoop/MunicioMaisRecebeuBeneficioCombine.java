package br.com.unb.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


//The method setCombinerClass(Class<? extends Reducer>) in the type Job is not applicable for the arguments (Class<MunicioMaisRecebeuBeneficioCombine>)
public class MunicioMaisRecebeuBeneficioCombine extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text,IntWritable>  {
	
	//extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		int count = 0; // should be an int, but anyway...

        while (values.hasNext()) {
            ++count;
        }

        output.collect(new Text("count"), new IntWritable(count));
		
	}
}
