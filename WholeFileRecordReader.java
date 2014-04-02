import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import java.io.*;

/**
 * Created with IntelliJ IDEA.
 * User: vaio
 * Date: 3/27/14
 * Time: 8:55 PM
 * To change this template use File | Settings | File Templates.
 */
public class WholeFileRecordReader implements RecordReader<IntWritable, Text> {
    private String writtenFileName="writtenFileName";
    private FileSplit fileSplit;
    private Configuration conf;
    private IntWritable startingoffset = new IntWritable();
    private Text value = new Text();
    private int processed = 0;
    int fileLength=0;
    FSDataInputStream in;
    RandomAccessFile inRanAccessFile;
    public WholeFileRecordReader(InputSplit inputSplit, JobConf entries) throws IOException {
        fileSplit=(FileSplit)inputSplit;
        byte[] fileTemp = new byte[4096];
        conf=(Configuration)entries;
        Path file = fileSplit.getPath();
        FileSystem fs = null;
        FileOutputStream fileOutputStream =new FileOutputStream(writtenFileName);
        try {
            fs = file.getFileSystem(conf);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        in = null;
        try {
            in = fs.open(file);

        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        try {
            while((fileLength=in.read(fileTemp,0,fileTemp.length))!=-1){
                fileOutputStream.write(fileTemp,0,fileLength);
                fileOutputStream.flush();

            }
            fileOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
       in.close();
       fileOutputStream.close();
       inRanAccessFile = new RandomAccessFile(new File(writtenFileName), "r");

    }

    @Override
    public boolean next(IntWritable nullWritable, Text bytesWritable) throws IOException {
        if (processed<=(fileSplit.getLength()-100)) {
            inRanAccessFile.seek(processed);
            //System.out.println(processed);

            byte[] contents = new byte[100];
            inRanAccessFile.read(contents, 0, 100);
            startingoffset.set(processed);
            String contentString = new String(contents);
            value.set(contentString);
            processed = processed+1;
            return true;
        }
        else{
            IOUtils.closeStream(inRanAccessFile);
            return false;}
    }

        /*if (!processed) {
            byte[] contents;
            contents = new byte[(int) fileSplit.getLength()];
            Path file = fileSplit.getPath();
            FileSystem fs = file.getFileSystem(conf);
            FSDataInputStream in;
            in = null;
            try {
                in = fs.open(file);

                IOUtils.readFully(in, contents, 0, contents.length);
                String contentString = new String(contents);
                value.set(contentString);
               // System.out.print("next"+contentString);
            } finally {
                IOUtils.closeStream(in);
            }
            processed = true;
            return true;
        }
        return false;
    }                  */

    @Override
    public IntWritable createKey() {
        return startingoffset;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Text createValue() {
        return value;
    }

    @Override
    public long getPos() throws IOException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void close() throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public float getProgress() throws IOException {
        return 0;
    }
}