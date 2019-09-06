Read CSV file from constant path "C:/fileSort/bigCSVFile.csv"
Reads in chunks (requested number of lines) , sorts the data in memory
Writes to temporary sorted files to "C:/fileSort/temp_X.csv"
Merges the temp_X files to one sorted file
The sorted file will in the following path: "C:/fileSort/sortedFile.csv"
