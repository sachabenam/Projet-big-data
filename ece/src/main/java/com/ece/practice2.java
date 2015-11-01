import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class practice2 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

public void map(Object key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context
			) throws IOException, InterruptedException { //we take off the first line
	if (!value.toString().equals("first_name;city;age;bank_balance;number_of_friends"))
	{ String[] result = value.toString().split(";");
	//We group by age
	if(Integer.parseInt(result[1])<5)
	{
		word.result[1]="0-5 ans";
	}
	if(5<Integer.parseInt(result[1]) && Integer.parseInt(result[1])<13)
	{
		word.result[1]="6-12 ans";
	}
	if(12<Integer.parseInt(result[1]) && Integer.parseInt(result[1])<18)
	{
		word.result[1]="12-17 ans";
	}
	if(17<Integer.parseInt(result[1]) && Integer.parseInt(result[1])<26)
	{
		word.result[1]="18-25 ans";
	}
	if(25<Integer.parseInt(result[1]) && Integer.parseInt(result[1])<36)
	{
		word.result[1]="26-35 ans";
	}
	if(35<Integer.parseInt(result[1]) && Integer.parseInt(result[1])<46)
	{
		word.result[1]="36-45 ans";
	}
	if(45<Integer.parseInt(result[1]) && Integer.parseInt(result[1])<61)
	{
		word.result[1]="46-60 ans";
	}
	if(60<Integer.parseInt(result[1]) )
	{
		word.result[1]="plus de 60 ans";
	}
	context.write(word, new IntWritable(Integer.parseInt(result[4]))); 
	} 
	}

  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      int total=0;
      int moyenne;
      // WE sum the number of friends group by ages and count the number of persons to get the average of friends by scale of ages
      for (IntWritable val : values) {
        sum += val.get();
        total+=1;
      }
      // WE test the number total to not get an error and not divide by 0
      if(total==0){
    	  result.set(total);
      }
      else{
    	  // we calculate the average if the number total is not 0
    	  moyenne=sum/total;
    	  result.set(moyenne);
      }
      
      context.write(key, result);
    }
  }
// this the main
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}



