import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
  /**
   * MapReduce program that uses the hadoop framework to fetch ngrams from a dataset.
   * @author Adithya Balasubramanian
   *
   */
public class WordCount {
	/**
  	 * method that takes the unit and fetches the list of ngrams from the input file.
  	 * @param n unit of gram
   	 * @param str input line from the file
   	 * @return list of ngrams in the input string
   	 */
	public static List<String> ngrams(int n, String str) {
		List<String> ngrams = new ArrayList<String>();
		str = str.replaceAll("[^a-zA-Z]", " ").toLowerCase().trim();
		str = str.replaceAll(" {2,}", " ");
		String[] words = str.split(" ");
		for (int i = 0; i < words.length - n + 1; i++)
			ngrams.add(concat(words, i, i + n));
		return ngrams;
	}
	/**
  	 * method that concatenates the words to form a phrase depending on the unit of n-gram
  	 * @param words
  	 * @param start
  	 * @param end
  	 * @return phrase that forms the ngram
  	 */
	public static String concat(String[] words, int start, int end) {
		StringBuilder sb = new StringBuilder();
		for (int i = start; i < end; i++)
			sb.append((i > start ? " " : "") + words[i]);
		return sb.toString();
	}

	public static void main(String[] args) {
		try{
		Configuration conf = new Configuration();
	    Job job = new Job(conf);
	    job.setJarByClass(WordCount.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	/**
  	 * Mapper class that transforms input text into ngrams text and IntWritable i.e. the value 1
  	 * @author Adithya Balasubramanian
  	 *
  	 */
	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			for (int n = 1; n <= 5; n++) {
				for (String ngram : ngrams(n, value.toString())) {
					word.set(ngram);
					context.write(word, one);
				}
			}
		}
	}
	/**
  	 * Reducer class that reduces by summing occurences of ngrams over key to give the number of occurences
  	 * @author Adithya Balasubramanian
  	 *
  	 */
	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
}
