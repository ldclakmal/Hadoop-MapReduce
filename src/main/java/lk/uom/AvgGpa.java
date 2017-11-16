package lk.uom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Chanaka Lakmal
 * @date 11/15/2017
 * @since 1.0
 */
public class AvgGpa {

    public static class GpaMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] gpaRecord = value.toString().split(",");
            DoubleWritable doubleWritable = new DoubleWritable();
            Text index = new Text();
            index.set(gpaRecord[0]);
            doubleWritable.set(Double.parseDouble(gpaRecord[2]));
            context.write(index, doubleWritable);
        }
    }

    public static class GpaReducer extends Reducer<Text, DoubleWritable, Text, Text> {

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (DoubleWritable val : values) {
                count++;
                sum += val.get();
            }
            Double avg = sum / count;
            String out = String.valueOf(avg);
            context.write(key, new Text(out));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "AvgGpa");
        job.setJarByClass(AvgGpa.class);
        job.setMapperClass(GpaMapper.class);
        job.setReducerClass(GpaReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

