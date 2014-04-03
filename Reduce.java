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
public class Reduce extends MapReduceBase implements Reducer<Text, valueFormat, Text, Text> {
    public void reduce(Text key, Iterator<valueFormat> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        StringBuilder sb =new StringBuilder();
        Text finalput = new Text();
        valueFormat values2 =new valueFormat();
        Text key2 =new Text();
        String tempKey2;
        while (values.hasNext()) {
            //curValue = values.next();
            values2=values.next();
            sb.append(',');
            sb.append(values2.getStringOffset());

            //sb.append(',');
            //sb.append(values2.getFileName());
            //sb.append(") ");

            // if sb.length more than limit , collect once
            if(sb.length() > 1048576) {
                // Debug
               // System.out.println("Reduce Output: " + key + "<>" + sb.toString());
                tempKey2=values2.getFileName()+","+key;
                key2.set(tempKey2);
                finalput.set(sb.toString());
                output.collect(key2, finalput);
                sb = new StringBuilder();
            }

        }
        tempKey2=values2.getFileName()+","+key;
        key2.set(tempKey2);
        finalput.set(sb.toString());
        output.collect(key2, finalput);
    }
}