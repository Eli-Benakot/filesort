
import java.io.*;
import java.util.*;

import static java.lang.String.*;


/**
 * Created by ania on 9/3/2019.
 */
@SuppressWarnings("Since15")
public class FileSort {

    //sorted map of some part of the file
    protected static SortedMap<String, List<String>> sortedFilePart = new TreeMap<String, List<String>>();



    //list of temp sorted files
    protected static List<File> tempFilesList = new ArrayList();

    static int numRecordsInMemory = 0;
    static int fieldIndex = 0;

    /**
     * @param args command line argument
     * @throws IOException generic IO exception
     */
    public static void main(final String[] args) throws IOException {




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

        try {
            readCSVFileSortAndWriteToTempFiles(numRecordsInMemory, fieldIndex);
            //merge the files from the tempFilesList
            //write the final sorted file into database
        }finally{
           // sortedFilePart=null;
            //tempFilesList=null;
        }
    }


    //read CSV file from constant path "C:/fileSort/bigCSVFile.csv"
    //reads in chunks (requested number of lines) , sorts the data in memory
    //writes to temporary sorted files to "C:/fileSort/temp_X.csv"
    private static void readCSVFileSortAndWriteToTempFiles(int numOfLines, int fieldIndex) throws IOException {

        String pathToCsv = "C:/fileSort/bigCSVFile.csv";

        BufferedReader csvReader = null;

        File csvFile = new File(pathToCsv);
        if (csvFile.isFile()) {
            try {
                csvReader = new BufferedReader(new FileReader(pathToCsv));

                long fileNumber=1;

                for (int i = 0; i <numOfLines; ++i) {

                    String row = csvReader.readLine();

                    //add the row to sorted map
                    if(row!=null){
                        List<String> data = Arrays.asList(row.split("\\s*,\\s*"));

                        //rows will be sorted by required field and stored in a map
                        sortedFilePart.put(data.get(fieldIndex), data);
                    }

                    //we have required number of sorted rows, now need to save it into temp file
                    if (i == numOfLines-1 && row!=null) {

                        writeToTempFile(fileNumber);
                        i = 0;
                        fileNumber++;
                    }

                    else if (row == null) { //end of file
                        if(sortedFilePart.size()>0) {
                            writeToTempFile(fileNumber);
                        }
                        break;
                    }

                }

            } catch (FileNotFoundException fnf) {
                System.err.println("File not found" + fnf);
            } finally {
                csvReader.close();
            }

        }
    }


    //flushes sorted collection's data into temp file
    private static void writeToTempFile(long fileNumber) throws IOException{
        FileWriter writer=null;
        File tempFile = new File("C:/fileSort/temp_" + fileNumber + ".csv");
        try {
            writer = new FileWriter(tempFile);

            for (List<String> row : sortedFilePart.values()) {
                String rowCommaSeparated = join(",", row);
                writer.write(rowCommaSeparated+System.getProperty( "line.separator" ));
            }
            //add sorted file to the list of temporary files
            tempFilesList.add(tempFile);
            //clear sorted collection before loading the next part of unsorted csv
            sortedFilePart.clear();
        }
        finally{
            writer.close();
        }

    }





 }
