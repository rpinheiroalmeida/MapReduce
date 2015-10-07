package br.com.unb.hadoop;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BeneficiosPagosEstadoMapper extends Mapper<Object, Text, Text, DoubleWritable> {

	private Text word = new Text();
	private static int STATE_POSITION = 0;
	private static int VALUE_POSITION = 10;

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] csv = value.toString().split("\\t");

		word.set(csv[STATE_POSITION]);
		
		if (!csv[VALUE_POSITION].equalsIgnoreCase("Valor Parcela")) {
			DecimalFormat formatterUK = new DecimalFormat( "#,##0.0#" );
	        Double valueFormated;
			try {
				valueFormated = formatterUK.parse( csv[VALUE_POSITION] ).doubleValue();
				context.write(word, new DoubleWritable(valueFormated));
			} catch (ParseException e) {
				e.printStackTrace();
			}
	        
		}

	}
}