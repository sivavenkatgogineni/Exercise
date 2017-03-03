package com.distil;

import com.distil.bean.DomainRecord;
import com.distil.bean.Result;
import com.distil.bean.Tsv;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

class DisplayMetrics {

    /**
     * This method performs the conversion of a List of Strings into a map, where the key is a domain name and value is a list of tsv objects.
     * @return mapDomainToListTsvs: Map containing a domain name and corresponding Tsv objects.
     */
    public static Map<String, List<Tsv>> convertListOfTsvIntoMapOfDomainNmaeToListOfTsv(List<String> listCsvLines)
    {
        Map<String, List<Tsv>> mapDomainToListTsvs = new HashMap<String, List<Tsv>>();
        // Mapping the tsv lines into a map, where the key is the domain name and the value is a list of Tsv objects. Each of the tsv objects represents a tsv line.
        for (String resultLine : listCsvLines) {
            String[] columns = resultLine.split("\t");
            String sDomain=columns[1];
            Date dRequestDateObject = new Date(Double.valueOf((Double.parseDouble(columns[0]) * 1000)).longValue());
            Tsv tsvRecord =new Tsv(dRequestDateObject,sDomain);
            List<Tsv> listOfCsvsForADomain= new ArrayList<Tsv>();
            if(mapDomainToListTsvs.containsKey(sDomain))
                listOfCsvsForADomain=mapDomainToListTsvs.get(sDomain);
            listOfCsvsForADomain.add(tsvRecord);
            mapDomainToListTsvs.put(sDomain, listOfCsvsForADomain);
        }
        return mapDomainToListTsvs;
    }
    /**
     * This method performs the conversion of a map into a list of DomainRecord objects
     * @return listOfDomainRecords: List containing Domain Record objects.
     */
    public static  List<DomainRecord> convertMapOfDomainNameToListIntoListOfDomainRecords(Map<String, List<Tsv>> mInput)
    {
        List<DomainRecord> listOfDomainRecords = new ArrayList<DomainRecord>();
       // Converting the above map into a list of DomainRecord objects where in each object represents a domain.
        for(Map.Entry<String, List<Tsv>> resultLinesPerDomainEntry : mInput.entrySet()){
            DomainRecord domainRecord=  new DomainRecord(resultLinesPerDomainEntry.getKey());
            for(Tsv tsv :resultLinesPerDomainEntry.getValue()) {
                domainRecord.addRequestTime(tsv.getRequestTime());
            }
            listOfDomainRecords.add(domainRecord);
        }
        return listOfDomainRecords;
    }
    /**
     * This method performs the necessary Aggregations as requested in the document per domain and displays it in the console.
     * @param listCsvLines: List of strings, where in each string represents a line read from the tsv file.
     */
   public static void doTheAggregates(List<String> listCsvLines)
   {
       //System.out.println("Finished Reading All files Concurrently using  threads!!! Hurray!!!");
       //System.out.println("Total Requests across all domains::" + listCsvLines.size());
       Map<String, List<Tsv>> mapDomainToListCsvs = new HashMap<String, List<Tsv>>();
       mapDomainToListCsvs = convertListOfTsvIntoMapOfDomainNmaeToListOfTsv(listCsvLines);
       // Iterating the list of domain objects and performing the aggregations for each of the domains
       for(DomainRecord domainRecord : convertMapOfDomainNameToListIntoListOfDomainRecords(mapDomainToListCsvs))
       {
           Integer iTotalNoOfRequests =  domainRecord.getRequestTimes().size();
           Map<String, Integer> mapOfDateHourKeyToNoOfRequests = new HashMap<String, Integer>();
           for (Date requestTime :  domainRecord.getRequestTimes()) {
               DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH");
               String sDateHourKey = dateFormat.format(requestTime);
               if (mapOfDateHourKeyToNoOfRequests.containsKey(sDateHourKey))
                   mapOfDateHourKeyToNoOfRequests.put(sDateHourKey, mapOfDateHourKeyToNoOfRequests.get(sDateHourKey) + 1);
               else
                   mapOfDateHourKeyToNoOfRequests.put(sDateHourKey, 1);
           }

           /** Metrics as requested**/
           System.out.println();
           System.out.println("Domain Name::"+domainRecord.getDomainName());
           System.out.println("Total Number of Requests for this domain :" + iTotalNoOfRequests);
           //System.out.println("Total No of Hours for domain::"+domainRecord.getDomainName()+" :::::" + mapOfDateHourKeyToNoOfRequests.size());
           System.out.println("Average requests per hour for this domain :"+((0.0+iTotalNoOfRequests) / mapOfDateHourKeyToNoOfRequests.size()));
           System.out.println("Maximum requests per hour for this domain :" + (Collections.max(mapOfDateHourKeyToNoOfRequests.values())));
           System.out.println();
       }
       System.out.println("Done. Thanks for the opportunity. I thoroughly Enjoyed It!!!");
   }

    /**
     * This method returns the list of file names that exist under the reesource folder.
     * @return : Returns the list of file names.
     */
    public static List<String> getFileNames()
    {
        String sUserDir=System.getProperty("user.dir");
        String sFilePath= sUserDir+File.separator+"resources"+File.separator;
        File sPath = new File(sFilePath);
        /**making sure that the directory exists else altering the path such that it works**/
        if (!sPath.isDirectory()) {
            sFilePath = sUserDir.substring(0, sUserDir.length() - 4) + File.separator + "resources" + File.separator;
        }
        //System.out.println("sUserDir::::::"+sUserDir);
        //System.out.println("sFilePath::::::"+sFilePath);
        List<String> listOfFilePaths = new ArrayList<String>();
        File[] files = new File(sFilePath).listFiles();
        for (File file : files)
            listOfFilePaths.add(file.getAbsolutePath());

        return listOfFilePaths;
    }

    public static void recieveSigTerm(final List<Future<Result>> finalSynchronizedListOfFutures)
    {

        /** shutdown hook is called wen a SIGTERM is recieved**/
        Runtime.getRuntime().addShutdownHook(
                new Thread()
                {
                    @Override
                    public void run()
                    {
                        List<String> localListCsvLines = new ArrayList<String>();

                        List<Future<Result>> localSynchronizedListOfFutures=finalSynchronizedListOfFutures;
                        System.out.println();
                        System.out.println(" Alert!!!!!!!!!!!!! Shutdown hook has been invoked!!!!!!!!!!");
                        System.out.println();

                        //System.out.println("synchronizedListOfFutures"+localSynchronizedListOfFutures);
                        try {
                            while (localSynchronizedListOfFutures.size() > 0) {
                                List<Future<Result>> toBeRemoved = new ArrayList<Future<Result>>();
                                /** Iterating over all the futures and recieving the response back**/
                                for (Future<Result> future : localSynchronizedListOfFutures) {
                                    if (future.isDone()) {
                                        localListCsvLines.addAll(future.get().getListOfLines());
                                        toBeRemoved.add(future);
                                    }
                                }
                                localSynchronizedListOfFutures.removeAll(toBeRemoved);
                            }
                            //System.out.println("localListCsvLines size"+localListCsvLines.size());
                        }
                        catch (ConcurrentModificationException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (ExecutionException e) {
                            e.printStackTrace();
                        }
                        catch(Exception e) {
                            e.printStackTrace();
                        }
                        doTheAggregates(localListCsvLines);

                    }
                });
    }

   public static void main(String[] args) throws InterruptedException, ExecutionException, IOException {
       /** 3 is the number of processes that run at any given time**/
       int iNumOfThreads = 3;
       ExecutorService esExecutor = Executors.newFixedThreadPool(iNumOfThreads);
       List<Future<Result>> synchronizedListOfFutures = Collections.synchronizedList(new ArrayList<Future<Result>>());
       for (String filename : getFileNames()) {
           Future<Result> future = esExecutor.submit(new CallableClass("" + filename));
           synchronizedListOfFutures.add(future);
       }
       esExecutor.shutdown();
       final List<Future<Result>> finalSynchronizedListOfFutures = synchronizedListOfFutures;
       recieveSigTerm(finalSynchronizedListOfFutures);
   }
}
