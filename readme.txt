Inverted Index Generation:
The inverted index has been compiled to have 100 WET files which result in approximately, 4 million documents. 
The flow for generating the inverted index can be seen as the following:
Part 1:
The first part of the program as seen in the figure, generates the postings and URL Table. This is implemented in the function ‘Get_all_postings’. For each WET File, this function is called. It 
• generates sorted postings for each WET file
• populated the URL Table and writes it to file 
• populates SQLite database to include text of each URL.
The code does not include every URL encountered. The text is first stripped using Regular expressions. If the new text is at least 75% of the original text only then are the entries written to corresponding files.
Part 2:
The Second part of the program merges postings files to an intermediate postings files. This is implemented in the function ‘Merge_all_intermediate’. This function takes an array of files and merges them in a sorted way. The code at this moment has been implemented in such a way that there need to be at least 2 postings files.
If there are more than 4 postings files, the function merges the postings files into 4 intermediate files. Otherwise, the function merges the postings files into 2 intermediate files.
This is being done using the heapq library.
The merge is being done in parallel.
Part 3:
The third part of the program takes all the merge files, and generates an intermediate index. This is implemented in the function ‘Get_mid_index’. This function takes a merged postings file and generates an index for each file. 
The index generates in this case is in the format: word:docID,Freq, DocID,Freq…
This is also being done in parallel. So the files are generated at almost the same time.
The merged posting file is read line by line, there is a variable where the current word is being stores. For each word all the doc ids and freq ids are being stored.  When a different word is encountered, the list is written to file, and the current word variable updated.
Part 4:
The fourth part takes all the indexes generated and merges them. This assumes that the index files generated are in sorted order, which is true since all the postings originally generated are sorted. 
This is being done in the function named: ‘Merge_indexes’
This is being done using the heapq library.
Part 5:
The fifth and final part takes the merged index file and generates a lexicon and inverted index.
The file is read line by line, there is a variable where the current word is being stored.
For each word, two arrays are being stored, one for the document IDs and one for the Frequencies. 
Once a different word is encountered, the arrays are encoded, using the vbencode package, and written to the index file.
The word, start position, number of bytes for doc IDs and number of bytes for the frequencies is then stored in the lexicon file.
The vbencode package, takes each number, keeps on dividing it by 128 and added the modulo of that at the start of the string. This string is then passed into the struct library and encoded using 1 bit signed char. 
Time taken and size:
The whole code (WET files to inverted index) takes around 4 hours for 100 WET files.
The most time consuming process in all of this is the get all postings function which takes 60 to 75 seconds per WET file.
The files generated have the following sizes:
• Index.dat (which stores the inverted index) : 4.15gb
• Lexicon.dat : 511mb
• WebTextInfo.sqllite: 18.7 gb
• Webpage_infos.txt : 245mb
Packages needed:
Packages needed for running the webIndex.py:
In order to run this code, the following packages are needed:
• Vbcode:
By github user utahta 
Link: https://pypi.org/project/vbcode/
To encode the inverted index
• glob
To get the files list in a directory
• Collections
For counting the frequencies of word
• Heapq
For merging sorting files
• Contextlib
In merge files, the exitstack function closes all the opened files.
• Timeit
To time the execution
• Concurrent.futures
For parallel execution of some parts of the code.
• Sqlitedict
By RaRe Technologies
Github link: https://github.com/RaRe-Technologies/sqlitedict
For creating the database
Query Processing and execution:
The code for this part of the program is in queryproc.py file:
The code can be seen in the following flow:
Part 1:
The first part of the program as seen in the figure, gets the lexicon from the file. It reads the file line by line and writes it to a dictionary where the word is the key and value is an array containing start position, number of bytes for docIds, number of byters for Freq Ids
This is implemented in the function names load_lex. It requires the lexicon.dat file to be located in the same directory as the code.
Part 2:
The second part of the program as seen in the figure, gets the URL Table from the file. It reads the file line by line and writes it to a dictionary where the docId  is the key and value is an array containing URL and content length
This is implemented in the function names load_webInfo. It requires the webpage_infos.txt file to be located in the same directory as the code.
Part 3:
This part is implemented in the main function itself. It only asks for the user input where it first asks for the query and the choice for how it is to be implemented.
Part 4:
The code then takes the query, splits it into an array of words. For each term, it gets the positions from the lexicon and according to the position it gets the encoded docIDs and Frequencies.
The code then continues to decode the docIDs and Frequencies and stores them the dociD and FreqId dictionary respectively.
This is being implemented in the function named getTermIndex. The main function calls the gettermIndex for each term in parallel.
Part 5a:
If the user chose union which is (choice 1) the function get_union_results is called. This function first takes all the docIds, finds its unique values and stores them in an array. A heap is initialized. After that, it takes each docId, for each term it gets the nextGEQ and then sends them in the function named getBM25_union. The score and docId is then inserted in the heap. If the size of heap is greater than 10, it is sliced to include maximum of 10 values at any given point.
getBM25_union is called in parallel to speed up the process.
The function then returns the heap.
Part 5b:
If the user chooses intersection which is choice 2, the function get_common is called. 
The function, docIds dictionary and the sorted terms, according to the number of docIds in each term.
If the number of terms is greater than 1, it initializes an array for pointers and a heap.
Starting from the first docId in the smallest docIds list, it calls the nextGEQ for each term with the current docId. If the returned docId is equal to the current one, it goes on and check for the next term. 
The pointers for each term list are being updated as well. Since the lists are sorted, this ensures that the maximum number of docIds compared decreases with each step. 
If the docID exists in all the term lists, the function getBM25 is called. Which returns a score and the docID. The score and docID is then entered in the heap.
If at any term the returned docId is not equal to the current docID, the current DocID is updated to be the next one in the max DocIDs. 
This is repeated for all docIds in shortest Term list.
If there is only 1 term, the process is similar to union.
If the size of heap is greater than 10, it is sliced to include maximum of 10 values at any given point.
Part 6:
The returned heaps are then processed to get the URL from the URL Table, and get the text from the database.
Part 7:
The text is then processed to get a snippet, which basically returns the first substring which contains all the terms, if not all then some of them. 
At each word in text, the snippet is being updated from the first time the text was encountered till the last time there was a match. If there is only one match then simply the text from start till the first match is returned.
If the corresponding snippet is greater than 50 words, the snippet is sliced from start to 50 words.
Part 8:
The user is asked if it wants to search for another query, if so it goes back to part 3, otherwise it just exits the code.

Packages needed:
Packages needed for running the queryproc.py:
In order to run this code, the following packages are needed:
Vbcode:
To decode the inverted index
Heapq
For creating the heaps
Contextlib
In merge files, the exitstack function closes all the opened files.
Timeit
To time the execution
Concurrent.futures
For parallel execution of some parts of the code.
Sqlitedict
For reading the database
Numpy
For getting the unique elements in the array
Time taken:
The time taken for loading lexicon and URL table is 1 minute. It takes around 60 seconds to load the term indexes and then anywhere between 0 seconds to 15 minutes to get the query results. 
Queries like “Brooklyn metrotech” and “California wildfires” take less time whereas queries like “hello world” take a lot of time.
