import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: vaio
 * Date: 3/27/14
 * Time: 8:56 PM
 * To change this template use File | Settings | File Templates.
 */
public class Map extends MapReduceBase implements Mapper<NullWritable, Text, Text, valueFormat > {
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