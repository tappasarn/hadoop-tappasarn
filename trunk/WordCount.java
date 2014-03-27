import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class WordCount {
    public static class valueFormat implements Writable{
        public int offset;
        public String fileName;

        public valueFormat(int offset, String fileName){
            this.offset=offset;
            this.fileName=fileName;

        }
        public valueFormat(){
            this.offset=0;
            this.fileName=null;
        }
        public String getFileName() {
            return fileName;
        }

        public long getOffset() {
            return offset;
        }
        @Override
        public String toString() {
            return "("+this.fileName + " " + this.offset+")";
        }
        @Override
        public void write(DataOutput dataOutput) throws IOException {
            //To change body of implemented methods use File | Settings | File Templates.

            String offsetStr = String.valueOf(offset);
            for(int j=0; j<offsetStr.length(); j++) {
                dataOutput.write( offsetStr.charAt(j) );
            }

            dataOutput.writeChar(',');

            for(int j=0; j<fileName.length(); j++) {
                dataOutput.write(fileName.charAt(j));
            }

            /*
            //System.out.println("write"+offset+fileName);
            for(int i=0;i<offset.toString().length();j++){

                System.out.print(offset.toString().charAt(i));
                dataOutput.writeChar(offset.toString().charAt(i));
                if(i==(offset.toString().length()-1)){
                    dataOutput.writeChar(',');
                    System.out.print(',');
                }
            }
            for(int j=0;j<fileName.length();j++){
                System.out.print(fileName.charAt(j));
                dataOutput.writeChar(fileName.charAt(j));
            }
            //dataOutput.writeLong(offset);
            //dataOutput.writeChars(fileName);
            */
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            String line = dataInput.readLine();
            System.out.println("ReadField, in line: " + line); // debug
            String[] rawIns = line.split(",");
            System.out.println("value:" + rawIns[1] + "-- offset" + rawIns[0]);
            this.offset = Integer.parseInt(rawIns[0]);
            this.fileName = rawIns[1];
        }
    }

    public static class WholeFileInputFormat extends FileInputFormat<NullWritable, Text> {
        @Override
        protected boolean isSplitable(FileSystem fs, Path filename) {
            return false;
        }

        @Override
        public RecordReader<NullWritable, Text> getRecordReader(InputSplit inputSplit, JobConf entries, Reporter reporter) throws IOException {
            WholeFileRecordReader reader = new WholeFileRecordReader(inputSplit, entries);
            return reader;
        }
    }

    public static class WholeFileRecordReader implements RecordReader<NullWritable, Text> {

        private FileSplit fileSplit;
        private Configuration conf;
        private Text value = new Text();
        private boolean processed = false;
        public WholeFileRecordReader(InputSplit inputSplit, JobConf entries){
            fileSplit=(FileSplit)inputSplit;
            conf=(Configuration)entries;
        }

        @Override
        public boolean next(NullWritable nullWritable, Text bytesWritable) throws IOException {
            if (!processed) {
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
                    System.out.print("next"+contentString);
                } finally {
                    IOUtils.closeStream(in);
                }
                processed = true;
                return true;
            }
            return false;
        }

        @Override
        public NullWritable createKey() {
            return NullWritable.get();  //To change body of implemented methods use File | Settings | File Templates.
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
            return processed ? 1.0f : 0.0f;
        }
    }

    public static class Map extends MapReduceBase implements Mapper<NullWritable, Text, Text, valueFormat > {
        private File stringListFile;
        private Text keyOut = new Text();
        valueFormat valueOut = new valueFormat();
        // private IntWritable valueOut = new IntWritable(1);
        private String currentFile;

        @Override
        public void configure(JobConf job) {
            stringListFile = new File("./stringlist.txt");
        }

        @Override
        public void map(NullWritable key, Text value, OutputCollector<Text, valueFormat> output, Reporter reporter) throws IOException {


            // Debug
            System.out.println(key + ":" + value +":" + value.getLength());

            // Get name
            currentFile = ((FileSplit)reporter.getInputSplit()).getPath().getName();

            BufferedReader linereader = new BufferedReader(new InputStreamReader(new FileInputStream(stringListFile)));
            String line;
            String region = value.toString();
            while((line = linereader.readLine()) != null) {
                Pattern p = Pattern.compile(line);
                Matcher m = p.matcher(region);
                while ( !m.hitEnd() ) {
                    if (m.find() ) {
                        keyOut.set(line);
                        valueOut.offset=m.end()-line.length();
                        valueOut.fileName=currentFile;
                        System.out.println("map"+" "+keyOut+" "+valueOut.fileName+" "+valueOut.offset);
                        output.collect(keyOut, valueOut);

                    }
                }

            }

        }
    }
    public static class Reduce extends MapReduceBase implements Reducer<Text, valueFormat, Text, Text> {
        public void reduce(Text key, Iterator<valueFormat> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            StringBuilder sb =new StringBuilder();
            Text finalput = new Text();
            while (values.hasNext()) {
                //curValue = values.next();
                sb.append('(');
                sb.append(values.next().getOffset());
                sb.append(',');
                sb.append(values.next().getFileName());
                sb.append(") ");

                // if sb.length more than limit , collect once
                if(sb.length() > 1048576) {
                    // Debug
                    System.out.println("Reduce Output: " + key + "<>" + sb.toString());

                    finalput.set(sb.toString());
                    output.collect(key, finalput);
                    sb = new StringBuilder();
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName("wordcount");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(valueFormat.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(WholeFileInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        DistributedCache.addCacheFile(new URI("./stringlist.txt"), conf);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}