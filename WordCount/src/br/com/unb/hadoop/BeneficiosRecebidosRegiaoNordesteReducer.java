package br.com.unb.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class BeneficiosRecebidosRegiaoNordesteReducer extends Reducer<Text, CompositeWritable, Text, DoubleWritable> {

//	int max =0;
//	Text maxWord = new Text();
	
//	double maxSum = 0d;
	
	public void reduce(Text text, Iterable<CompositeWritable> values, Context context)
			throws IOException, InterruptedException {
		
		int count = 0;
		double sum = 0d;
		for (CompositeWritable value : values) {
			count += value.count;
			sum += value.valor;
		}
		context.write(text, new DoubleWritable(sum/count));
//		if(count > max)
//        {
//            max = count;
//            maxSum = sum;
//            maxWord.set(text);
//        }
//		context.write(text, new IntWritable(sum));
	}

//	@Override
//	protected void cleanup(Context context) throws IOException, InterruptedException {
//		context.write(maxWord, new CompositeWritable(max, maxSum));
//	}
}








//public class MunicipioMaisRecebeuBeneficiosReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
//
//	int max =0;
//	Text maxWord = new Text();
//	
//	public void reduce(Text text, Iterable<IntWritable> values, Context context)
//			throws IOException, InterruptedException {
//		int sum = 0;
//		for (IntWritable value : values) {
//			sum += value.get();
//		}
//		if(sum > max)
//        {
//            max = sum;
//            maxWord.set(text);
//        }
////		context.write(text, new IntWritable(sum));
//	}
//
//	@Override
//	protected void cleanup(Context context) throws IOException, InterruptedException {
//		context.write(maxWord, new IntWritable(max));
//	}
//}




