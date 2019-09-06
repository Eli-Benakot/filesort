
import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.lang.String.*;


/**
 * Created by ania on 9/3/2019.
 */
@SuppressWarnings("Since15")
public class FileSort {


    //list with some part of data from the file
    protected List<List<String>> partialFileList=new ArrayList<List<String>>();
    //list of temp sorted files
    protected  List<File> tempFilesList = new ArrayList();

    protected Queue<File> queue = new ConcurrentLinkedQueue<File>();
    private CountDownLatch countDownLatch=null;
    private File sortedFile=null;

    private static int numRecordsInMemory = 0;
    private static int fieldIndex = 0;

    static String inputCsv = "C:/fileSort/bigCSVFile.csv";
    static String outputCsv = "C:/fileSort/sortedFile.csv";

    private Comparator<List<String>> comparator=null;

    private static AtomicInteger fileNumber = new AtomicInteger(1);

    /**
     * @param args command line argument
     * @throws IOException generic IO exception
     */
    public static void main(final String[] args) throws IOException,InterruptedException,ExecutionException {

        for (int param = 0; param < args.length; ++param) {

            if ((args[param].equals("-x")) && args.length > param + 1) {
                param++;
                numRecordsInMemory = Integer.parseInt(args[param]);
                if (numRecordsInMemory < 0) {
                    throw new IllegalArgumentException("Number of records, that possible to store in memory - should be positive!");
                }
                System.out.println("-x " + numRecordsInMemory);
            } else if ((args[param].equals("-f"))) {
                param++;
                fieldIndex = Integer.parseInt(args[param]);
                if (fieldIndex < 0) {
                    throw new IllegalArgumentException("Field index - should be positive!");
                }
                System.out.println("-f " + fieldIndex);
            }
        }


        FileSort fileSort = new FileSort();
        fileSort.sortCSVFile();
    }


    private void sortCSVFile() throws IOException,InterruptedException,ExecutionException {
        comparator = createComparator(fieldIndex);
        //read CSV file, sort and save to temp files
        readCSVFileSortAndWriteToTempFiles(numRecordsInMemory);
        //merge the files from the tempFilesList
        mergeTempFiles();

        persistFileToDB();

    }

    private void persistFileToDB(){
        System.out.println(" file ready for to be persisted");
    }

    //read CSV file from constant path "C:/fileSort/bigCSVFile.csv"
    //reads in chunks (requested number of lines) , sorts the data in memory
    //writes to temporary sorted files to "C:/fileSort/temp_X.csv"
    private void readCSVFileSortAndWriteToTempFiles(int numRecordsInMemory) throws IOException {
        BufferedReader csvReader = null;

        File csvFile = new File(inputCsv);
        if (csvFile.isFile()) {
            try {
                csvReader = new BufferedReader(new FileReader(csvFile));

                long fileNumber=1;

                for (int i = 0; i <numRecordsInMemory; ++i) {

                    String row = csvReader.readLine();

                    if (row == null) { //end of file
                        if(partialFileList.size()>0) {
                            partialFileList.sort(comparator);
                            writeToTempFile(fileNumber);
                        }
                        break;
                    }

                        List<String> list = Arrays.asList(row.split(","));
                        partialFileList.add(list);

                    //we have required number of sorted rows, now need to save it into temp file
                    if (i == numRecordsInMemory-1) {
                        partialFileList.sort(comparator);
                        writeToTempFile(fileNumber);
                        i = -1;
                        fileNumber++;
                    }



                }

            } catch (FileNotFoundException fnf) {
                System.err.println("File not found" + fnf);
            } finally {
                if(csvReader!=null) {
                    csvReader.close();
                }
            }

        }
    }


    //flushes sorted collection's data into temp file
    private void writeToTempFile(long fileNumber) throws IOException{
      //  FileWriter writer=null;
        File tempFile = new File("C:/fileSort/temp_" + fileNumber + ".csv");
        PrintWriter pw = new PrintWriter(tempFile);

        try {
           // writer = new FileWriter(tempFile);

            for (List<String> row : partialFileList) {
                String rowCommaSeparated = join(",", row);
                pw.println(rowCommaSeparated);
            }
            //add sorted file to the list of temporary files
            tempFilesList.add(tempFile);
            //clear sorted collection before loading the next part of unsorted csv
            partialFileList.clear();
        }
        finally{
            pw.close();
        }

    }


    private static <T> Comparator<List<T>> createComparator(final Comparator<? super T> delegate, final int  index) {
        return new Comparator<List<T>>()
        {
            @Override
            public int compare(List<T> list0, List<T> list1)
            {
                    T element0 = list0.get(index);
                    T element1 = list1.get(index);
                    return delegate.compare(element0, element1);
            }
        };
    }

    private static <T extends Comparable<? super T>> Comparator<List<T>>  createComparator(int index) {
        return createComparator(Comparator.naturalOrder(), index);
    }


    private File merge2Files(File file1, File file2,String fileNumber) throws IOException{

        List<String> list1=null;
        List<String> list2=null;

        File mergedFile = new File("C:/fileSort/merged_" + fileNumber + ".csv");

        PrintWriter pw = new PrintWriter(mergedFile);


        BufferedReader br1 = new BufferedReader(new FileReader(file1));
        BufferedReader br2 = new BufferedReader(new FileReader(file2));

        String row1 = br1.readLine();
        String row2 = br2.readLine();
        System.out.println("Merging: "+ file1.getName() + " and " + file2.getName());

        while (row1 != null || row2 !=null)
        {
            if(row1 != null) {
                list1 = Arrays.asList(row1.split(","));

                if (row2 != null) {
                    list2 = Arrays.asList(row2.split(","));

                    int i = comparator.compare(list1, list2);

                    if (i < 0) {
                        pw.println(row1);
                        row1 = br1.readLine();
                    } else {
                        pw.println(row2);
                        row2 = br2.readLine();
                    }
                }else{
                    pw.println(row1);
                    row1 = br1.readLine();
                }
            }else{
                while(row2!=null){
                    pw.println(row2);
                    row2 = br2.readLine();
                }
            }

        }
        System.out.println("New file added to the queue: "+ mergedFile.getName());
        queue.add(mergedFile);
        pw.flush();

        // closing resources
        br1.close();
        br2.close();
        pw.close();
        file1.delete();
        file2.delete();
        return mergedFile;
    }


    private void mergeTempFiles() {

        queue.addAll(tempFilesList);
        int numOfIterations = queue.size() - 1;
        countDownLatch=new CountDownLatch(numOfIterations);
        int threadPoolSize=numOfIterations/2;
        if(threadPoolSize<1){
            threadPoolSize=1;
        }else if(threadPoolSize>10){//need to limit thread pool size, too many threads will not increase performance but even may crash application
            threadPoolSize=10;
        }

        ExecutorService executorService = Executors.newFixedThreadPool(threadPoolSize);

        while(countDownLatch.getCount()>0){

            Future<File> sortedFileFuture = executorService.submit(new Callable<File>() {

                public File call() {
                    File mergedFile=null;
                    try {
                         while (queue.size()>1){
                             countDownLatch.countDown();
                            mergedFile = merge2Files(queue.poll(), queue.poll(), Thread.currentThread().getName() + fileNumber.toString());
                            fileNumber.getAndIncrement();
                        }

                    } catch (IOException ioe) {
                        System.err.println("Error occured while merging the files: " + ioe);
                    }finally {

                    }
                    return mergedFile;
                }
            });
            while(true) {
                if (countDownLatch.getCount() == 0 ) {
                    try {
                        sortedFile = sortedFileFuture.get(2, TimeUnit.MINUTES);
                        sortedFile.renameTo(new File(outputCsv));
                        break;
                    }catch(Exception e){
                        System.err.println("Sort of the file failed with exception: " + e);
                    }
                }
            }
        }


        executorService.shutdown();
    }
 }
