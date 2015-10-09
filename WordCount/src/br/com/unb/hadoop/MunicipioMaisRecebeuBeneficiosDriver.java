package br.com.unb.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MunicipioMaisRecebeuBeneficiosDriver extends Configured implements Tool  {

	@Override
	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputDir = new Path(args[1]);

		// Create configuration
		Configuration conf = new Configuration(true);

		// Create job
		Job job = new Job(conf, "MunicipioMaisRecebeuBeneficiosSPDriver");
		job.setJarByClass(MunicipioMaisRecebeuBeneficiosDriver.class);

		// Setup MapReduce
		job.setMapperClass(MunicioMaisRecebeuBeneficioMapper.class);
//		job.setCombinerClass(MunicioMaisRecebeuBeneficioCombine.class);
		job.setReducerClass(MunicipioMaisRecebeuBeneficiosReducer.class);
		job.setNumReduceTasks(1);

		// Specify key / value
		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(IntWritable.class);
		job.setOutputValueClass(CompositeWritable.class);

		// Input
		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job, outputDir);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

		// Execute job
		int code = job.waitForCompletion(true) ? 0 : 1;
		System.exit(code);
		return code;
	}

	public static void main(String[] args) throws Exception {
		MunicipioMaisRecebeuBeneficiosDriver driver = new MunicipioMaisRecebeuBeneficiosDriver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}

}
