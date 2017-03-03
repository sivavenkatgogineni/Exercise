package com.distil.bean;

import java.util.List;

/**
 * This class Contains contains all the required fields that are required by the CallableClass class to return an result object.
 * @author  sgogineni
 * @version 1.0
 * @since   3/2/2017.
 */
public class Result {

    private String sName;
    private List<String> listOfLines;

    public String getsName() {
        return sName;
    }

    public void setsName(String sName) {
        this.sName = sName;
    }

    public List<String> getListOfLines() {
        return listOfLines;
    }

    public void setListOfLines(List<String> listOfLines) {
        this.listOfLines = listOfLines;
    }

    public Result(String sName, List<String> listOfLines) {
        this.sName = sName;
        this.listOfLines = listOfLines;
    }


}
