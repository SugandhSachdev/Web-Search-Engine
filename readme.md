# A basic web search engine

## Inverted Index Generation:

Inverted index is compiled to have 100 WET files. Approximately 4 million documents are generated.
Follow the steps below to generate the inverted index key.

##### Part 1: 

The postings and URL Table are generated in the first part. Implementation is in the function `Get_all_postings`. This function is called for each WET File. The function 
- generates sorted postings for each WET file,
- populated the URL Table and writes it to file, and 
- populates SQLite database to include text of each URL.

The code does not include every URL encountered. The text is first stripped using Regular expressions. If the new text is at least 75% of the original text, only then are the entries written to corresponding files.

##### Part 2: 

The program merges postings files to an intermediate postings files in the second part. Implementation is in the function ‘Merge_all_intermediate’. This function takes an array of files and merges them in a sorted way. The code implementation requires a minimum of 2 posting files. If four postings files are present, the function merges them into 4 intermediate files. The function merges the postings files into 2 intermediate files if the postings files are less.
This is done using the heapq library.
Files are merged in parallel.

##### Part 3: 
The program uses the merged files to generate an intermediate index. Implementation is in the function `Get_mid_index`. The function generates an index for each file in merged postings.  
Format of the generated index: word:docID,Freq, DocID,Freq…
Files are generated at the same time in parallel.
All lines in merged posting are read for a variable that stores current word. Doc IDs and freq IDs are stored for each word. When a different word is encountered, the list is written to file, and the current word variable updated.

##### Part 4: 

Generated indexes are merged in the fourth part. This assumes that the index files generated are in sorted order, which is true as all the postings originally generated are sorted. 
The implementation in the function `Merge_indexes`
This is being done using the heapq library.

##### Part 5:  
The merged index file are used to generate a lexicon and inverted index in the final part.
All lines in merged posting are read for a variable that stores current word.
Two arrays are being stored for each word. The arrays are for document IDs and Frequencies. 
When a different word is encountered, the arrays are encoded using the vbencode package, and written to the index file.
The word, start position, number of bytes for doc IDs and number of bytes for the frequencies is then stored in the lexicon file.

The vbencode package continually divides the number by 128 and adds the modulo to the start of the string. This string is then passed into the struct library and encoded using 1 bit signed char. 

##### Time taken and size:

The whole code (WET files to inverted index) takes around 4 hours for 100 WET files.
The most time consuming process is the `get_all_postings` function which takes 60 to 75 seconds per WET file.
The files generated have the following sizes:
- Index.dat (which stores the inverted index) : 4.15gb
- Lexicon.dat : 511mb
- WebTextInfo.sqllite: 18.7 gb
- Webpage_infos.txt : 245mb

##### Packages needed: 

Packages needed for running the webIndex.py:

Following packages are needed to run this code, :
- Vbcode:
By github user utahta 
Link: https://pypi.org/project/vbcode/
To encode the inverted index
- glob
To get the files list in a directory
- Collections
For counting the frequencies of word
- Heapq
For merging sorting files
- Contextlib
In merge files, the exitstack function closes all the opened files.
- Timeit
To time the execution
- Concurrent.futures
For parallel execution of some parts of the code.
- Sqlitedict
By RaRe Technologies
Github link: https://github.com/RaRe-Technologies/sqlitedict
For creating the database

## Query Processing and execution:
The code for this part of the program is in queryproc.py file:
The code can be seen in the following flow:

##### Part 1: 

The first part of the program, as seen in the figure, gets the lexicon from the file. It reads each line and writes it to a dictionary where the word is the key and value is an array containing start position, number of bytes for docIds, number of byters for Freq Ids
This is implemented in the function names load_lex. It requires the lexicon.dat file to be located in the same directory as the code.

##### Part 2:

The second part of the program, as seen in the figure, gets the URL Table from the file. Program reads each line and writes it to a dictionary where the docId  is the key and value is an array containing URL and content length
Implementation in the function `load_webInfo`. The function requires the webpage_infos.txt file be located in the same directory as the code.

##### Part 3:

Implementation for part 3 is in the main function. Program only asks the user to input query and implementation of choice.

##### Part 4: 

The code then takes the query and splits it into an array of words. For each term, it gets the positions from the lexicon and the encoded docIDs and Frequencies based on these positions.
The code decodes the docIDs and Frequencies, storing them in the dociD and FreqId dictionary respectively.
implementation is in the function `getTermIndex`. The main function calls the gettermIndex for each term in parallel.

##### Part 5a: 

If the user chose union (choice 1)the function `get_union_results is called`. The function finds and stores unique values in docID in an array. A heap is initialized. The function then takes each docId and for each term it retrieves the nextGEQ and passes them in the function `getBM25_union`. The score and docId is then inserted in the heap. If the size of heap is greater than 10, it is sliced to include maximum of 10 values at any given point.
`getBM25_union` is called in parallel to speed up the process.
The function returns the heap.

##### Part 5b: 

If the user chooses intersection (choice 2), the function `get_common` is called. 
The function, docIds dictionary and the sorted terms, according to the number of docIds in each term.
`get_common` initializes an array for pointers and a heap if the number of terms is greater than 1.
Starting from the first docId in the smallest docIds list, it calls the nextGEQ for each term with the current docId. If the returned docId is equal to the current one, it continues to the next term. 
The pointers for each term list are updated. Sorted lists ensure the maximum number of docIds compared decreases with each step. 
If the docID exists in all the term lists, the function `getBM25` is called. `getMB25` returns a score and the docID. The score and docID is then entered in the heap.
If at any term the returned docId is not equal to the current docID, the current DocID is updated to the next DocID in the max DocIDs. 
Thie process is repeated for all docIds in shortest Term list.
The process is similar to union if there is only one term.
If the size of heap is greater than 10, it is sliced to include maximum of 10 values at any given point.

##### Part 6: 

The returned heaps are processed to get the URL from the URL Table and get the text from the database.

##### Part 7: 

The text is then processed to get a snippet. Snippet is the first substring which contains some terms if not all. 
At each word in text, the snippet is updated from the first time the text is encountered till the last for all matches. The text from start till the first match is returned if one match is encountered.
If the corresponding snippet is greater than 50 words, the snippet is sliced from start to 50 words.

##### Part 8: 

Process goes back to part 3 if users wants to search another query. Code is exited if no further query is required.

##### Time taken:

Loading lexicon and URL table takes 1 minute. It takes around 60 seconds to load the term indexes and then anywhere between 0 seconds to 15 minutes to get the query results. 
Queries like “Brooklyn metrotech” and “California wildfires” take less time whereas queries like “hello world” takes more time.
