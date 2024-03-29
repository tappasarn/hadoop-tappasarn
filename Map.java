import org.apache.hadoop.io.IntWritable;
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
public class Map extends MapReduceBase implements Mapper<IntWritable, Text, Text, Text > {
    private File stringListFile;
    private Text keyOut = new Text();
    Text valueOut = new Text();
    // private IntWritable valueOut = new IntWritable(1);
    private String currentFile;

    @Override
    public void configure(JobConf job) {
        stringListFile = new File("./query.dat");
    }

    @Override
    public void map(IntWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {


        // Debug
        //    System.out.println(key + ":" + value +":" + value.getLength());

        // Get name
        currentFile = ((FileSplit)reporter.getInputSplit()).getPath().getName();

        BufferedReader linereader = new BufferedReader(new InputStreamReader(new FileInputStream(stringListFile)));
        String line;
        int addingoffset = key.get();
        String region = value.toString().substring(0,16777216);
        String region1 = value.toString().substring(16777215).trim();//end of region-1
        while((line = linereader.readLine()) != null) {
            Pattern p = Pattern.compile(line.charAt(0)+"(?="+line.substring(1)+")");
            //Pattern p = Pattern.compile("\\b"+line);
            Matcher m = p.matcher(region);
            m.reset();
            while ( !m.hitEnd() ) {
                if (m.find() ) {
                    keyOut.set(currentFile+","+line);
                    //keyOut.set(line);
                    valueOut.set(String.valueOf(addingoffset+m.start()));//m.end()-line.length();
                    //valueOut.fileName=currentFile;
                    //System.out.println("map"+" "+keyOut+" "+valueOut.fileName+" "+valueOut.offset);
                    output.collect(keyOut, valueOut);
                }
                //-------------------------------------------------------------------


            }
            //}
            if(region1.length()!=1){ //ถ้าเท่ากับ1อาจมีการนับซ้ำ
                Pattern p1 = Pattern.compile("\\b"+line);
                //Pattern p = Pattern.compile("\\b"+line);
                Matcher m1 = p1.matcher(region1);
                m.reset();
                //while ( !m.hitEnd() ) {
                if (m1.find() ) {

                    keyOut.set(currentFile+","+line);
                    valueOut.set(String.valueOf(addingoffset+m1.start()));
                    //valueOut.offset=addingoffset+m1.start();//m1.end()-line.length();
                    //valueOut.fileName=currentFile;
                    //System.out.println("map"+" "+keyOut+" "+valueOut.fileName+" "+valueOut.offset);
                    output.collect(keyOut, valueOut);

                }    }

        }
    }    }