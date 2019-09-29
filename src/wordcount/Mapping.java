package wordcount;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapping extends Mapper<Object,Text,Text,IntWritable>{

    public void map(Object key,Text value, Context context) throws IOException, InterruptedException{
        Text word=new Text();
        IntWritable one = new IntWritable(1);

        String[] words = value.toString().split(" ");//split es que hace una separacion
        for(String string: words){
            string=string.replaceAll("[$?¡¿):#=(*/,;.^+%&!''\"--]","");
            string=string.toLowerCase();
       

                if(valER(string)==true){
                    word.set("er");
                    context.write(word, one);
                }
                else if(valTION(string)==true){
                    word.set("tion");
                    context.write(word, one);
                }
                else if(valEST(string)==true){
                    word.set("est");
                    context.write(word, one);
                }
            
            //word es lo que contiente key y one es el equivalente a 1 y va contando de 1 en 1.
        }
      }
    public static  boolean valER(String cad){
        String cadena=".*er$";
        return Pattern.matches(cadena, cad.toLowerCase());
    }
    public static boolean valTION(String cad){
        String cadena=".*tion$";
        return Pattern.matches(cadena, cad.toLowerCase());
    }

    public static boolean valEST (String cad){
        String cadena=".*est$";
        return Pattern.matches(cadena, cad.toLowerCase());
    }
}
