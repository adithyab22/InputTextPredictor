import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
  * Language model for input text predictor
  * @author Adithya Balasubramanian
  *    *
      */

public class LanguageModel {
	public static void main(String[] args) {
		try {
			Configuration conf = HBaseConfiguration.create();
			Job job = new Job(conf, "languageModel");
			job.setJarByClass(LanguageModel.class);
			TableMapReduceUtil.initTableReducerJob("languageModel", IntSumReducer.class,job);
			job.setNumReduceTasks(10);
			job.setMapperClass(TokenizerMapper.class);
			job.setReducerClass(IntSumReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			// get the command line parameters:
			conf.set("Occurence", args[2]);
			conf.set("N", args[3]);
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

public static class TokenizerMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		private Text text1 = new Text();
		private Text text2 = new Text();
		private int t = 2;

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] wordArray = line.split("\t");
			String phrase = wordArray[0];
			int occurence = Integer.parseInt(wordArray[1]);
			// ignore the phrases that fall under the category <= t
			if (occurence < t) {
				return;
			}
			String[] phraseArray = phrase.split(" ");
			// position of last word in the sequence
			int endIndex = phrase.lastIndexOf(" ");
			String prefixLine = line.substring(0, endIndex).trim();
			String word = phrase.substring(endIndex).trim();
			word += " " + occurence;
			text1.set(prefixLine);
			text2.set(word);
			context.write(text1, text2);
		}
	}
	public static class IntSumReducer extends
			TableReducer<Text, Text, ImmutableBytesWritable> {
		// get the number of words that display in the website.
		private int n = 5;
		private Configuration config = HBaseConfiguration.create();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String word = "";
			int occurence = 0;
			int sum = 0;
			int size = 0;
			String wordnext = "";
			double wordcount = 0;
			double probability = 0;
			// map to sort
			Map<String, Integer> wordList = new HashMap<String, Integer>();
			for (Text value : values) {
				String value_string = value.toString();
				String count = "";
				int endIndex = value_string.lastIndexOf(" ");
				word = value_string.substring(0, endIndex);
				count = value_string.substring(endIndex).trim();
				occurence = Integer.parseInt(count);
				wordList.put(word, occurence);
				sum += occurence;
			}
			// sort the result
			List<Map.Entry<String, Integer>> sortedList = new ArrayList<Map.Entry<String, Integer>>(
					wordList.entrySet());
			Collections.sort(sortedList,
					new Comparator<Map.Entry<String, Integer>>() {
						public int compare(Map.Entry<String, Integer> occurence1, Map.Entry<String, Integer> occurence2) {
							int result = (Integer) occurence2.getValue() - (Integer) occurence1.getValue();
							return result;
						}
					});

			// store top n words with highest probabilities
			if (sortedList.size() > n) {
				size = n;
			} else {
				size = sortedList.size();
			}

			Put put = new Put(Bytes.toBytes(key.toString()));
			int i = 0;
			while (i < size) {
				wordnext = sortedList.get(i).getKey();
				wordcount = sortedList.get(i).getValue();
				probability = wordcount * 100.0 / sum;
				String storeword = wordnext + "|" + wordcount;
				put.add(Bytes.toBytes("Probability"), Bytes.toBytes(wordnext),
						Bytes.toBytes("" + probability));
				i++;
			}
			if (!put.isEmpty()) {
				context.write(new ImmutableBytesWritable(key.getBytes()), put);
			}
		}
	}
}
