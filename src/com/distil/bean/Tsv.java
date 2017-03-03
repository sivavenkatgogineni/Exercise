package com.distil.bean;

import java.util.Date;

/**
 * This class Contains contains all the required fields of the TSV files.
 * @author  sgogineni
 * @version 1.0
 * @since   3/2/2017.
 */
public class Tsv
{
    private Date requestTime;
    private String domainName;

    public Tsv(Date requestTime, String domainName)
    {
        this.requestTime=requestTime;
        this.domainName=domainName;

    }
    public Date getRequestTime() {
        return requestTime;
    }
    public void setRequestTime(Date requestTime) {
        this.requestTime=requestTime;
    }
    public String getDomainName() {
        return domainName;
    }
    public void setDomainName(String domainName) {
        this.domainName=domainName;
    }

}