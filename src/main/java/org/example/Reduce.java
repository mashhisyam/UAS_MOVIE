package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class Reduce extends Reducer<Text , Text,Text , Text>{
    public void reduce(Text key , Iterable<Text> values , Context context) throws IOException, InterruptedException {

//        System.out.println("--------------");
//        System.out.println(key.toString());
//        System.out.println("--------------");
        int count = 0;
        int comedy = 0;
        for (Text val : values){

            //Untuk mendapatkan data yang bergenre comedy
            String[] company = val.toString().split("/");
            for(String temppp : company){
                if(temppp.contains("\"")){
                    String[] yo = temppp.split("\"");
                    for (String haa : yo){
                        if(haa.trim().equalsIgnoreCase("Comedy")){
                            comedy++;
                        }
                    }
                }else{
                    if(temppp.trim().equalsIgnoreCase("Comedy")){
                        comedy++;
                    }
                }
            }
            count++;
        }
        // Inisiasi variable yang menampung data genre film dan tahun
        int last = Character.getNumericValue( key.charAt(1));
        int first = Character.getNumericValue( key.charAt(0));
        Text tahun = new Text();

        // Untuk mengambil dan  menampilkan data tahun dari dataset
        for (int i = 2008 ; i < 2021 ; i++){
            if (i== last){
                tahun.set("tahun "+last);
            }
        }
        // Menampilkan data yang sudah dicounting
        HashMap<String , String> value = new HashMap<String ,String>();
        value.put("TOTAL FILM " , String.valueOf(count));
        value.put("COMEDY FILM " , String.valueOf(comedy));
        context.write(tahun, new Text(value.toString()) );
    }
}