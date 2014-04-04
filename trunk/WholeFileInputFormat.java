
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.net.NetworkTopology;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 * User: vaio
 * Date: 3/27/14
 * Time: 8:53 PM
 * To change this template use File | Settings | File Templates.
 */
public class WholeFileInputFormat extends FileInputFormat<IntWritable, Text>  {
    private long minSplitSize = 1;
    private static final double SPLIT_SLOP = 1.1;
    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {

        FileStatus[] files = listStatus(job);

        // Save the number of input files for metrics/loadgen
        job.setLong(NUM_INPUT_FILES, files.length);
        long totalSize = 0;                           // compute total size
        for (FileStatus file: files) {                // check we have valid files
            if (file.isDirectory()) {
                throw new IOException("Not a file: "+ file.getPath());
            }
            totalSize += file.getLen();
        }

        long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);
        long minSize = Math.max(job.getLong(org.apache.hadoop.mapreduce.lib.input.
                FileInputFormat.SPLIT_MINSIZE, 1), minSplitSize);

        // generate splits
        ArrayList<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
        NetworkTopology clusterMap = new NetworkTopology();
        for (FileStatus file: files) {
            Path path = file.getPath();
            long length = file.getLen();
            if (length != 0) {
                FileSystem fs = path.getFileSystem(job);
                BlockLocation[] blkLocations;
                if (file instanceof LocatedFileStatus) {
                    blkLocations = ((LocatedFileStatus) file).getBlockLocations();
                } else {
                    blkLocations = fs.getFileBlockLocations(file, 0, length);
                }
                if (isSplitable(fs, path)) {
                    long blockSize = file.getBlockSize();
                    long splitSize = 524288;//computeSplitSize(goalSize, minSize, blockSize);

                    long bytesRemaining = length;
                    while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
                        String[] splitHosts = getSplitHosts(blkLocations,
                                length-bytesRemaining, splitSize, clusterMap);
                        splits.add(makeSplit(path, length-bytesRemaining, splitSize,
                                splitHosts));
                        bytesRemaining -= splitSize;
                    }

                    if (bytesRemaining != 0) {
                        String[] splitHosts = getSplitHosts(blkLocations, length
                                - bytesRemaining, bytesRemaining, clusterMap);
                        splits.add(makeSplit(path, length - bytesRemaining, bytesRemaining,
                                splitHosts));
                    }
                } else {
                    String[] splitHosts = getSplitHosts(blkLocations,0,length,clusterMap);
                    splits.add(makeSplit(path, 0, length, splitHosts));
                }
            } else {
                //Create empty hosts array for zero length files
                splits.add(makeSplit(path, 0, length, new String[0]));
            }
        }
        //LOG.debug("Total # of splits: " + splits.size());
        return splits.toArray(new FileSplit[splits.size()]);
    }


    @Override
    public RecordReader<IntWritable, Text> getRecordReader(InputSplit inputSplit, JobConf entries, Reporter reporter) throws IOException {
        WholeFileRecordReader reader = new WholeFileRecordReader(inputSplit, entries);
        return reader;
    }
}