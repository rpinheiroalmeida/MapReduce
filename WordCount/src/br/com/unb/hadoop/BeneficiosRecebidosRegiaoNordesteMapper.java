package br.com.unb.hadoop;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BeneficiosRecebidosRegiaoNordesteMapper extends Mapper<Object, Text, Text, CompositeWritable> {

	private Text word = new Text();
	private final DecimalFormat formatterUK = new DecimalFormat( "#,##0.0#" );
	private static int STATE_POSITION = 0;
	private static int VALUE_POSITION = 10;
	private static final List<String> STATE_NE = Arrays.asList("MA", "PI", "CE", "RN", "PB", "PE", "SE", "AL","BA");

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] csv = value.toString().split("\\t");
//		String[] csv = value.toString().split(";");

		String state = csv[STATE_POSITION];
		
		
		CompositeWritable compositeWritable = new CompositeWritable();
		
		word.set(String.format("%s", csv[STATE_POSITION]));
		
		if (!csv[VALUE_POSITION].equalsIgnoreCase("Valor Parcela")) {
			if (STATE_NE.contains(state)) {
				compositeWritable.count = 1;
				compositeWritable.valor = transformValor(csv[VALUE_POSITION]);
				context.write(word, compositeWritable);
			}
		}

	}
	
	private Double transformValor(String valor) {
        Double valueFormated = Double.valueOf(0);
		try {
			valueFormated = formatterUK.parse( valor ).doubleValue();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return valueFormated;
	}
}