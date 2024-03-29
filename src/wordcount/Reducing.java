package wordcount;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Reducing extends Reducer<Text,IntWritable,Text,IntWritable>{
    public void reduce(Text key,Iterable<IntWritable>  values,Context context)throws IOException,InterruptedException{
        int total=0;
        for(IntWritable value: values){
            total++;
        }
        context.write(key,new IntWritable(total));
    }

}
