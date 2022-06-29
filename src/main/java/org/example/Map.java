package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map extends Mapper<Object,Text,Text, Text>{
    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String txt = value.toString();
        String[] tokens = txt.split(",");

        if(tokens[0].compareTo("year")!= 0) {
            context.write(new Text(tokens[2].trim()) , new Text(tokens[8].trim()));
        }

//        if(tokens[0].compareTo("THOR_DATA_VIET_ID") != 0){
//            context.write(new Text(tokens[6].trim()) , new Text(tokens[1].trim()));
//        }
//        System.out.println(name.compareTo("city"));
//        if(name.compareTo("City") != 0){
//            context.write(new Text(id) , new Text(name));
//        }

    }
}