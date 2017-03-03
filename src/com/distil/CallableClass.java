package com.distil;

import com.distil.bean.Result;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * This class Implements the callable Interface.
 * The Constructor takes the filename as the input which is then used by the call method to read the file. The call method returns Object of type Result.
 * simply displays "Hello World!" to the standard output.
 * @return: Result
 * @author  sgogineni
 * @version 1.0
 * @since   3/2/2017.
 */


class CallableClass implements Callable<Result> {
    private  String sFilename;
    private  Result rResult;
    public CallableClass(String sFilename) {
        this.sFilename = sFilename;
        this.rResult= new Result(sFilename, new ArrayList<String>());
    }
    public Result call() throws Exception {
        //System.out.println("Reading file:"+sFilename);
        List<String> listOfLines= new ArrayList<String>();
        InputStream in = Files.newInputStream(Paths.get(sFilename));
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        Thread.sleep(100);
        String line = reader.readLine();
        while(line != null){
            listOfLines.add(line);
            Thread.sleep(100);
            line = reader.readLine();
        }
        System.out.println("Finished Reading file:"+sFilename);
        this.rResult.setListOfLines(listOfLines);
        return this.rResult;
    }
}
