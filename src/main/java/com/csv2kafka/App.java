package com.csv2kafka;

import au.com.bytecode.opencsv.CSVReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

/**
 *
 *
 */
public class App
{

    public static void main( String[] args )
    {
        System.out.println( "test csv reader and kafka writer" );
        Formatter formatter = new Formatter();
        Loader loader = new Loader();
        readFileToKafka("sample.log", formatter, loader);
    }

    public static void readFileToKafka(String filename, Formatter formatter, Loader loader)
    {
        CSVReader reader = null;
        Integer i = new Integer(0);

        HashMap input = new HashMap();
        loader.init();
        try {
            reader = new CSVReader(new FileReader(filename));
            String[] line;

            while((line = reader.readNext()) != null) {

                input.put("unixtime",line[0]);
                input.put("name",line[1]);
                input.put("value",line[2]);

                loader.load(formatter.format(input));

            }
        } catch(IOException oe) {
            oe.printStackTrace();
        }
    }
}
