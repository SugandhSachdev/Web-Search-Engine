#Sugandh Sachdev,ss11334
import os
import numpy as np
import concurrent.futures
from sqlitedict import SqliteDict
import warc
import re
from glob import glob
from collections import Counter
import heapq
import contextlib
from timeit import default_timer as timer
import vbcode
import datetime

def get_final_index():
    print('Compiling final index file')
    st = timer()
    lex_file = open('lexicon.dat', 'wb+')
    in_file = open('index.dat', 'wb+')
    i = 0
    with open('compiled_index.dat', 'r') as fil:
        cur_word = None
        arr_freqs = []
        arr_doc_ids = []
        while True:
            try:
                f_line = fil.readline()
                if not f_line:
                    break
                if f_line is not '\n':
                    f_line = re.sub("\n|\r", "", f_line)
                    arr = f_line.split(',')
                    if len(arr) >= 3:

                        if cur_word == arr[0]:  # appending to the same word list

                            doc_ids = list(map(int,arr[1:][0::2]))
                            freqs = list(map(int,arr[1:][1::2]))
                            arr_doc_ids+=doc_ids
                            arr_freqs+=freqs
                        else:
                            if len(arr_freqs) > 0:  # if its not the first word, add to file
                                st_pos = in_file.tell()
                                d_bytes = in_file.write(vbcode.encode(arr_doc_ids))
                                f_bytes = in_file.write(vbcode.encode(arr_freqs))
                                lex_file.write('{},{},{},{}\n'.format(cur_word, st_pos, d_bytes, f_bytes).encode(
                                    'ascii'))  # index file seek:start pos, then read d bytes then read f bytes
                            cur_word = arr[0]
                            doc_ids = list(map(int, arr[1:][0::2]))
                            freqs = list(map(int, arr[1:][1::2]))
                            arr_doc_ids =doc_ids
                            arr_freqs=freqs
            except Exception as e:
                print(e, i)
                break
            i += 1
    lex_file.close()
    in_file.close()
    end = timer()
    print('Time Taken: ', end - st)
def get_mid_index(file_name,file_num):
    print('starting intermediate index file: ',file_num)
    output_file = 'temp_index/{}in_index.dat'.format(file_num)
    in_file = open(output_file,'wb+')
    i =0
    with open(file_name, 'r') as fil:
        cur_word = None
        arr_vals = []
        while True:
            try:
                f_line = fil.readline()
                if not f_line:
                    break
                if f_line is not '\n':
                    arr = f_line.split(',')
                    if len(arr) >= 3:
                        if '\n' in arr[2]:
                            arr[2] = arr[2].rstrip()
                        if cur_word == arr[0]:  # appending to the same word list

                            arr_vals += [arr[1],arr[2]]
                        else:
                            if len(arr_vals) > 0:  # if its not the first word, add to file

                                in_file.write('{},{}\n'.format(cur_word,','.join(arr_vals)).encode('ascii'))
                            cur_word = arr[0]
                            arr_vals.clear()
                            arr_vals = [arr[1], arr[2]]

            except Exception as e:
                print(e, i)
                break
            i += 1

def get_all_postings(fn,tempfileNum,j,db):
    #tempfileNum = 0
    docId = j
    #reg = re.compile(r'[^a-zA-Z0-9._@\n\r ]', re.ASCII)
    reg = re.compile(r'[^a-zA-Z0-9 ]', re.ASCII)
    url_arr = []
    print('******************************************WET file no: ',tempfileNum,'***********************************************')
    f = warc.open(fn)
    output_file = 'temp_postings/{}temp_postings.dat'.format(tempfileNum)
    file_arr = []
    for record in f:
        
        #print('parsing',docId)
        try:
            url = record.header.get('warc-target-uri', None)
            #content_length = record.header.get('Content-Length', None)
            if not url:
                continue
            text = record.payload.read().decode('utf-8')
            stripped_text = re.sub(reg, ' ', text).lower()
            stripped_text=re.sub('\s+', ' ', stripped_text).strip()

            #stripped_text = re.sub(reg2, ' ', stripped_text)
            if stripped_text and len(stripped_text) >= (len(text)*0.75): #if 75% of the text is been removed then its probably not english
                split_text = stripped_text.split(' ')
                word_freqs= Counter(split_text) #get all frequencies
                content_length = sum(word_freqs.values())
                url_arr.append("{},{},{}\n".format(docId, url, content_length))
                db[str(docId)] = stripped_text
                if '' in word_freqs.keys():
                    del word_freqs['']
                for key, val in word_freqs.items():
                    file_arr.append("{},{},{}\n".format(key, docId, val).encode('ascii'))
        except Exception as e:
            print(e)
        docId += 1
    db.commit()
    file_arr.sort()
    temp_file = open(output_file, "wb+")
    temp_file.writelines(file_arr)
    temp_file.close()
    return docId,url_arr

def merge_all_intermediate(index_files,i):
    print("Merging postings")
    with contextlib.ExitStack() as stack:
        files = [stack.enter_context(open(fn)) for fn in index_files] #open all files
        file_name = 'int_postings/compiled_postings{}.dat'.format(i)
        with open(file_name, 'w+') as f:
            f.writelines(heapq.merge(*files))


def merge_indexes():
    print("Merging intermediate index files")
    index_files = glob('temp_index/*.dat')
    with contextlib.ExitStack() as stack:
        files = [stack.enter_context(open(fn)) for fn in index_files] #open all files
        file_name = 'compiled_index.dat'
        with open(file_name, 'w+') as f:
            f.writelines(heapq.merge(*files))

if __name__ == '__main__':
    #create directories to store intermediate files
    print(datetime.datetime.now())
    if not os.path.exists('temp_postings'):
        os.mkdir('temp_postings')
    if not os.path.exists('int_postings'):
        os.mkdir('int_postings')

    if not os.path.exists('temp_index'):
        os.mkdir('temp_index')

    warc_files = glob('wet_files/*.wet.gz')
    #warc_files = warc_files[0:4]
    webInfo_file = open('webpage_infos.txt', "w+")
    sstart = timer()
    i = 0
    j=0
    db = SqliteDict('webTextInfo.sqlite', autocommit=False)
    for fn in warc_files:
        start = timer()
        j,urls = get_all_postings(fn,i,j,db)
        j=j+1
        #db.dump()
        db.commit()
        webInfo_file.writelines(urls)
        end = timer()
        print('1 wet file time(Sec):', end - start)
        i+=1
    db.close()
    send = timer()
    print('All  files time(Sec): ', send - sstart)

    #merge postings file in parallel
    p_st = timer()
    with concurrent.futures.ProcessPoolExecutor() as executor:
        temp_files = glob('temp_postings/*.dat')
        temp_files = temp_files[0:91]
        n = len(temp_files)
        step = n//4
        file_arr = []
        if n >4:
            for i in range(0, 4):
                start = i * step
                file_arr .append(temp_files[start:start + step])
            if (start + step) != n:
                start = start + step
                file_arr[3] = file_arr[3] + temp_files[start:]

            executor.map(merge_all_intermediate, file_arr,[1,2,3,4])
        else:
            n = n//2
            executor.map(merge_all_intermediate, [temp_files[0:n], temp_files[n:]], [1, 2])
    p_end = timer()
    print("Merging Posting Files time(Sec): ", p_end - p_st)

    p_st = timer()
    with concurrent.futures.ProcessPoolExecutor() as executor: #find the intermediate index in parallel
        temp_files = glob('int_postings/*.dat')
        n = list(range(len(temp_files)))
        executor.map(get_mid_index, temp_files, n)
    p_end = timer()
    print("All Intermediate indexes time(Sec): ", p_end - p_st)


    p_st = timer()
    merge_indexes()
    print('Merging Index files time(Sec): ',timer()-p_st)
    get_final_index()
    print('all time:',timer()-sstart)
    print(datetime.datetime.now())
