package tp1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/** Classe Driver : configure et lance le job.  Usage : hadoop jar wordcount.jar tp1.WordCount <entree> <sortie> */
public class WordCount {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] ourArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (ourArgs.length != 2) {
            System.err.println("Usage: WordCount <entree HDFS> <sortie HDFS>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "WordCount");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(ourArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(ourArgs[1])); // ne doit pas exister

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
