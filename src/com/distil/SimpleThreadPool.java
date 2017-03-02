package com.distil;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
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

class SimpleThreadPool {
    /**
     * This method performs the necessary Aggregations as requested in the document per domain and displays it in the console.
     * @param listCsvLines: List of strings, where in each string represents a line read from the tsv file.
     */
   public static void doTheAggregates(List<String> listCsvLines)
   {
       //System.out.println("Finished Reading All files Concurrently using  threads!!! Hurray!!!");
       //System.out.println("Total Requests across all domains::" + listCsvLines.size());
       Map<String, List<Tsv>> mapDomainToListCsvs = new HashMap<String, List<Tsv>>();
       List<DomainRecord> listOfDomainRecords = new ArrayList<DomainRecord>();

       // Mapping the tsv lines into a map, where the key is the domain name and the value is a list of Tsv objects. Each of the tsv objects represents a tsv line.
       for (String resultLine : listCsvLines) {
           String[] columns = resultLine.split("\t");
           String sDomain=columns[1];
           Date dRequestDateObject = new Date(Double.valueOf((Double.parseDouble(columns[0]) * 1000)).longValue());
           Tsv tsvRecord =new Tsv(dRequestDateObject,sDomain);
           List<Tsv> listOfCsvsForADomain= new ArrayList<Tsv>();
           if(mapDomainToListCsvs.containsKey(sDomain))
               listOfCsvsForADomain=mapDomainToListCsvs.get(sDomain);
           listOfCsvsForADomain.add(tsvRecord);
           mapDomainToListCsvs.put(sDomain, listOfCsvsForADomain);
       }
       // Converting the above map into a list of DomainRecord objects where in each object represents a domain.
       for(Map.Entry<String, List<Tsv>> resultLinesPerDomainEntry : mapDomainToListCsvs.entrySet()){
           DomainRecord domainRecord=  new DomainRecord(resultLinesPerDomainEntry.getKey());
           for(Tsv tsv :resultLinesPerDomainEntry.getValue()) {
               domainRecord.addRequestTime(tsv.getRequestTime());
           }
           listOfDomainRecords.add(domainRecord);
       }

       // Iterating the list of domain objects and performing the aggregations for each of the domains
       for(DomainRecord domainRecord : listOfDomainRecords)
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
           System.out.println("Domain Name::"+domainRecord.getDomainName());
           System.out.println("Total Number of Requests for this domain :" + iTotalNoOfRequests);
           //System.out.println("Total No of Hours for domain::"+domainRecord.getDomainName()+" :::::" + mapOfDateHourKeyToNoOfRequests.size());
           System.out.println("Average requests per hour for this domain :"+((0.0+iTotalNoOfRequests) / mapOfDateHourKeyToNoOfRequests.size()));
           System.out.println("Maximum requests per hour for this domain :" + (Collections.max(mapOfDateHourKeyToNoOfRequests.values())));
           System.out.println();
       }
       System.out.println("Done. Thanks for the opportunity. I thoroughly Enjoyed It!!!");
   }


   public static void main(String[] args) throws InterruptedException, ExecutionException, IOException {
       /** 3 is the number of processes that run at any given time**/
       int iNumOfThreads = 3;
       ExecutorService esExecutor = Executors.newFixedThreadPool(iNumOfThreads);
       List<Future<Result>> synchronizedListOfFutures = Collections.synchronizedList(new ArrayList<Future<Result>>());
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



       for (String filename : listOfFilePaths) {
           Future<Result> future = esExecutor.submit(new CallableClass("" + filename));
           synchronizedListOfFutures.add(future);
       }
       esExecutor.shutdown();
       final List<Future<Result>> finalSynchronizedListOfFutures = synchronizedListOfFutures;
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
}
