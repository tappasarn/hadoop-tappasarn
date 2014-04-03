
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: vaio
 * Date: 3/27/14
 * Time: 8:53 PM
 * To change this template use File | Settings | File Templates.
 */
public class WholeFileInputFormat extends FileInputFormat<IntWritable, Text> {
    @Override
    protected boolean isSplitable(FileSystem fs, Path filename) {
        return false;
    }

    @Override
    public RecordReader<IntWritable, Text> getRecordReader(InputSplit inputSplit, JobConf entries, Reporter reporter) throws IOException {
        WholeFileRecordReader reader = new WholeFileRecordReader(inputSplit, entries);
        return reader;
    }
}