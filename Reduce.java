import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: vaio
 * Date: 3/27/14
 * Time: 8:56 PM
 * To change this template use File | Settings | File Templates.
 */
public class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        StringBuilder sb =new StringBuilder();
        Text finalput = new Text();
        //valueFormat values2 =new valueFormat();
        int separatorCount=0;
        while (values.hasNext()) {
            //curValue = values.next();
            //values2=values.next();
            if(separatorCount!=0)sb.append(',');
            sb.append(values.next());
            separatorCount=1;

            //sb.append(',');
            //sb.append(values2.getFileName());
            //sb.append(") ");

            // if sb.length more than limit , collect once
            if(sb.length() > 1048576) {
                // Debug
                // System.out.println("Reduce Output: " + key + "<>" + sb.toString());

                finalput.set(sb.toString());
                output.collect(key, finalput);
                sb = new StringBuilder();
            }

        }
        finalput.set(sb.toString());
        output.collect(key, finalput);
    }
}