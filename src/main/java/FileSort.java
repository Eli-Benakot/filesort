
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


import static java.lang.String.*;


/**
 * Created by ania on 9/3/2019.
 */
public class FileSort {


    //list with some part of data from the file
    private List<List<String>> partialFileList = new ArrayList<List<String>>();

    //list of temporary files
    private Queue<File> queue = new ConcurrentLinkedQueue<File>();
    private CountDownLatch countDownLatch = null;

    private static int numRecordsInMemory = 0;
    private static int fieldIndex = 0;

    private static String inputCsv = "C:/fileSort/bigCSVFile.csv";
    private static String outputCsv = "C:/fileSort/sortedFile.csv";

    private Comparator<List<String>> comparator = null;

    private static AtomicInteger fileNumber = new AtomicInteger(1);

    /**
     * @param args command line argument
     * @throws IOException generic IO exception
     */
    public static void main(final String[] args) throws IOException{

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


    private void sortCSVFile() throws IOException {
        comparator = createComparator(fieldIndex);
        //read CSV file, sort and save to temp files
        readCSVFileSortAndWriteToTempFiles(numRecordsInMemory);
        //merge the files from the tempFilesList
        if(queue.size()>1) {
            mergeTempFiles();
        }else if(queue.size()==1){
            queue.poll().renameTo(new File(outputCsv));
        }else{
            System.err.println("File was not sorted!");
            return;
        }
        //persist the sorted CSV file data to database
        persistFileToDB();
    }

    private void persistFileToDB() {
        System.out.println(" file ready to be stored in database. ");
        BufferedReader csvReader = null;
        String row;
        try {
            csvReader = new BufferedReader(new FileReader(outputCsv));
            System.out.println(" following rows will be persisted: ");
            while ((row = csvReader.readLine()) != null) {

                System.out.println(row);
                // DatabaseHelper.persist(row);
            }

        } catch (FileNotFoundException fnfe) {
            System.err.println("File not found: " + fnfe);
        } catch (IOException ioe) {
            ioe.printStackTrace();
//        }catch(ClassNotFoundException cnfe){
//            cnfe.printStackTrace();
//        }catch(SQLException se){
//            se.printStackTrace();
        } finally {
            if (csvReader != null) {
                try {
                    csvReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //read CSV file from constant path "C:/fileSort/bigCSVFile.csv"
    //reads in chunks (requested number of lines) , sorts the data in memory
    private void readCSVFileSortAndWriteToTempFiles(int numRecordsInMemory) throws IOException {
        BufferedReader csvReader = null;

        File csvFile = new File(inputCsv);
        if (csvFile.isFile()) {
            try {
                csvReader = new BufferedReader(new FileReader(csvFile));

                long fileNumber = 1;

                for (int i = 0; i < numRecordsInMemory; ++i) {

                    String row = csvReader.readLine();

                    if (row == null) { //end of file
                        if (partialFileList.size() > 0) {
                            partialFileList.sort(comparator);
                            writeToTempFile(fileNumber);
                        }
                        break;
                    }

                    List<String> list = Arrays.asList(row.split(","));
                    partialFileList.add(list);

                    //we have required number of sorted rows, now need to save it into temp file
                    if (i == numRecordsInMemory - 1) {
                        partialFileList.sort(comparator);
                        writeToTempFile(fileNumber);
                        i = -1;
                        fileNumber++;
                    }
                }

            } catch (FileNotFoundException fnf) {
                System.err.println("File not found" + fnf);
            } finally {
                if (csvReader != null) {
                    csvReader.close();
                }
            }

        }
    }


    //flushes sorted collection's data into temp file
    private void writeToTempFile(long fileNumber) throws IOException {
        File tempFile = new File("C:/fileSort/temp_" + fileNumber + ".csv");

        try (PrintWriter pw = new PrintWriter(tempFile)) {
            for (List<String> row : partialFileList) {
                String rowCommaSeparated = join(",", row);
                pw.println(rowCommaSeparated);
            }
            //add sorted file to the queue of temporary files
            queue.add(tempFile);
            //clear sorted collection before loading the next part of unsorted csv
            partialFileList.clear();
        }
    }


    private static <T> Comparator<List<T>> createComparator(final Comparator<? super T> delegate, final int index) {
        return (list0, list1) -> {
            T element0 = list0.get(index);
            T element1 = list1.get(index);
            return delegate.compare(element0, element1);
        };
    }

    private static <T extends Comparable<? super T>> Comparator<List<T>> createComparator(int index) {
        return createComparator(Comparator.naturalOrder(), index);
    }


    private File merge2Files(File file1, File file2, String fileNumber) throws IOException {

        List<String> list2;
        BufferedReader br1=new BufferedReader(new FileReader(file1));
        BufferedReader br2=new BufferedReader(new FileReader(file2));

        File mergedFile = new File("C:/fileSort/merged_" + fileNumber + ".csv");

        try (PrintWriter pw = new PrintWriter(mergedFile)) {

            String row1 = br1.readLine();
            String row2 = br2.readLine();
            System.out.println("Merging: " + file1.getName() + " with " + file2.getName());

            while (row1 != null || row2 != null) {
                if (row1 != null) {
                    List<String> list1 = Arrays.asList(row1.split(","));

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
                    } else {
                        pw.println(row1);
                        row1 = br1.readLine();
                    }
                } else {
                    while (row2 != null) {
                        pw.println(row2);
                        row2 = br2.readLine();
                    }
                }

            }
            System.out.println("New file added to the queue: " + mergedFile.getName());
            queue.add(mergedFile);
            pw.flush();
        }finally {
            // closing resources
            br1.close();
            br2.close();
            file1.delete();
            file2.delete();
        }
        return mergedFile;
    }


    private void mergeTempFiles() {

        int numOfIterations = queue.size() - 1;
        countDownLatch = new CountDownLatch(numOfIterations);
        int threadPoolSize = numOfIterations / 2;
        if (threadPoolSize < 1) {
            threadPoolSize = 1;
        } else if (threadPoolSize > 10) {//need to limit thread pool size, too many threads will not increase performance but even may crash application
            threadPoolSize = 10;
        }
        ExecutorService executorService = Executors.newFixedThreadPool(threadPoolSize);
        try {
            while (countDownLatch.getCount() > 0) {

                Future<File> sortedFileFuture = executorService.submit(new Callable<File>() {

                    public File call() {
                        File mergedFile = null;
                        try {
                            while (queue.size() > 1) {
                                countDownLatch.countDown();
                                mergedFile = merge2Files(queue.poll(), queue.poll(), Thread.currentThread().getName()+ "__" + fileNumber.toString());
                                fileNumber.getAndIncrement();
                            }

                        } catch (IOException ioe) {
                            System.err.println("Error occured while merging the files: " + ioe);
                        }
                        return mergedFile;
                    }
                });
                while (true) {
                    if (countDownLatch.getCount() == 0) {
                        try {
                            File sortedFile = sortedFileFuture.get(2, TimeUnit.MINUTES);
                            boolean isRenamed = sortedFile.renameTo(new File(outputCsv));
                            String result = isRenamed ? "succeedeed" : "failed";
                            System.out.println("Rename of the file: " + result);
                            break;
                        } catch (Exception e) {
                            System.err.println("Sort of the file failed with exception: " + e);
                        }
                    }
                }
            }
        } finally {
            executorService.shutdown();
        }
    }
}
