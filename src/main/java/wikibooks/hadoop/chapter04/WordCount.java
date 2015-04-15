package wikibooks.hadoop.chapter04;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;

public class WordCount {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
    if (args.length != 2) {
      System.err.println("Usage: WordCount <input> <output>");
      System.exit(2);
    }
    Job job = new Job(conf, "WordCount");

    job.setJarByClass(WordCount.class);
    job.setMapperClass(WordCountMapper.class);
    job.setReducerClass(WordCountReducer.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

//    FileInputFormat.addInputPath(job, new Path(args[0]));
      /*
    FileInputFormat.addInputPath(job, new Path("input.txt"));
      FileInputFormat.addInputPath(job, new Path("input2.txt"));
      FileInputFormat.addInputPath(job, new Path("input3.txt"));
      */
/*
      FileSystem fs = FileSystem.get(conf);
      FileStatus[] fileStatuses = fs.listStatus(new Path("/user/hadoop/builddata"));

      for (FileStatus f : fileStatuses){
          File[] fileses = FileUtil.listFiles(new File(f.getPath().toString()));
          for (File f1 : fileses){
              System.out.println(f1.getPath());
          }
      }
      System.exit(0);
*/
      FileInputFormat.addInputPaths(job, args[0]);
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);
  }
}