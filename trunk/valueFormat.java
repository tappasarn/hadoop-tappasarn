import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: vaio
 * Date: 3/27/14
 * Time: 8:50 PM
 * To change this template use File | Settings | File Templates.
 */
public class valueFormat implements Writable {
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
        System.out.println("write"+offset+fileName);
        dataOutput.writeChars(String.valueOf(offset));
        //String offsetStr = String.valueOf(offset);
        //for(int j=0; j<offsetStr.length(); j++) {
          //  dataOutput.write( offsetStr.charAt(j) );
        //}

        dataOutput.writeChar(',');

        dataOutput.writeChars(fileName);


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
        //int keep=0;
        System.out.println("ReadField, in line: " + line); // debug
        String[] rawIns = line.split(",");
        System.out.println("value:" + rawIns[1] + "-- offset" + rawIns[0]);
        //Integer.parseInt(rawIns[0]);
        offset = Integer.parseInt(rawIns[0]);
        fileName = rawIns[1];
        System.out.println("readField"+offset+fileName+"END!!");

        }


    }

