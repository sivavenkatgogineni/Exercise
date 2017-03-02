package com.distil;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * This class Contains contains all the members and methods that are required for a domain.
 * @author  sgogineni
 * @version 1.0
 * @since   3/2/2017.
 */
public class DomainRecord
{
    private String domainName;
    private List<Date> requestTimes = new ArrayList<Date>();

    public void addRequestTime(Date requestTime) {
        this.requestTimes.add(requestTime);
    }
    public void addAllRequestTime(List<Date> requestTimes) {
        this.requestTimes.addAll(requestTimes);
    }
    public DomainRecord( String domainName)
    {
        this.domainName=domainName;

    }
    public List<Date> getRequestTimes() {
        return requestTimes;
    }

    public String getDomainName() {
        return domainName;
    }
    public void setDomainName(String domainName) {
        this.domainName=domainName;
    }


}