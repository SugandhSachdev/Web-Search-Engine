#sugandh sachdev,ss11334
from timeit import default_timer as timer
import math
import heapq
import vbcode
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from sqlitedict import SqliteDict
import concurrent.futures
import numpy as np

def load_lex():
    start_lex = timer()
    dict_lex={}
    f = open("lexicon.dat",'r')
    while True:
        line = f.readline()
        if line:
            line = line.split(',')
            dict_lex[line[0]] = [line[1],line[2],line[3]] #word,start position, number of bytes for doc Ids, number of bytes for frequencies
        else:
            break

    f.close()
    end_lex = timer()
    print('Lexicon Load time:', end_lex - start_lex)

    return dict_lex

def load_webinfo():
    start_web = timer()
    dict_urls = {}
    sum_doclen = 0
    numOfDocs = 0
    f = open("webpage_infos.txt",'r')
    while True:
        line = f.readline()
        if line:
            c = line.count(',')
            if c ==2:
                line = line.split(',')
            else:
                frst = line.find(',')
                lst = line.rfind(',')

                line = [line[:frst],line[frst+1:lst],line[lst+1:]]
            dict_urls[int(line[0])] = [line[1],int(line[2])] #docID, URL,Content Length
            sum_doclen+=int(line[2])
            numOfDocs+=1
        else:
            break
    f.close()

    end_web = timer()
    print('Web Info Load time:', end_web - start_web)

    return dict_urls,sum_doclen,numOfDocs
#numOfDocs = len(dict_urls.keys())
def getTermIndex(term,positions):

    with open('index.dat','rb') as f:
        f.seek(int(positions[0]))
        doc_ids = vbcode.decode(f.read(int(positions[1])))
        freqs = f.read(int(positions[2][:-1]))
        freqs = vbcode.decode(freqs)
        
    return term,doc_ids,freqs

def nextgenQ(lp,did):
    i = 0
    n = len(lp)
    while i<n:
        if lp[i] >= did:
            return i,lp[i]
        else:
            i += 1
    return i-1, lp[i-1]
def getBM25_union(did,dict_urls,numOfTerms,terms,freqs_dict,numOfDocs,pointers):
    score = 0
    doc_len = dict_urls[did][1]
    k = 1.2 * (0.25 + (0.75 * (doc_len / avg_doclen)))
    for i in range(numOfTerms):
        arr = docids_dict[terms[i]]
        idx = pointers[i]
        #print(arr[idx],did)
        if int(arr[idx]) == int(did):
            f_dt = freqs_dict[terms[i]][idx]
            f_t = len(arr)
            a = (numOfDocs - f_t + 0.5) / (f_t + 0.5)
            b = (2.2 * f_dt) / (k + f_dt)
            score += math.log10(a * b)
    #print('end',did)
    return (-score,did)
    
def get_union_results(docids_dict,numOfTerms,dict_urls,freqs_dict,numOfDocs):
    #start = timer()
    a = []
    total_entries = 0
    vals = list(docids_dict.values())
    for i in vals:
        a = a + i
        total_entries+=len(i)
    b = np.array(a)
    docIds_arr = np.unique(b)
    docIds_arr.sort()
    print("Total document entries: ", total_entries)
    #print("Num of Terms: ",numOfTerms)
    #print(timer()-start)
    heap_scores = []
    n = len(docIds_arr)
    positions = [0] * numOfTerms
    for j in range(n):
        did = docIds_arr[j]
        for k in range(numOfTerms):
            ls = docids_dict[terms[k]][positions[k]:]
            idx_t,did_new = nextgenQ(ls, did)
            positions[k] = positions[k] + idx_t
        res = getBM25_union(did,dict_urls,numOfTerms,terms,freqs_dict,numOfDocs,positions)
        heapq.heappush(heap_scores,res)
        if len(heap_scores) > 20:
            heap_scores = heapq.nsmallest(20,heap_scores)
            #heap_scores = heap_scores[:20]
    return heap_scores

def getBM25(did,dict_urls,numOfTerms,terms,freqs_dict,numOfDocs,pointers):
    score = 0
    doc_len = dict_urls[did][1]
    k = 1.2 * (0.25 + (0.75 * (doc_len / avg_doclen)))
    for i in range(numOfTerms):
        arr = docids_dict[terms[i]]
        idx = pointers[i]
        f_dt = freqs_dict[terms[i]][idx]
        f_t = len(arr)
        a = (numOfDocs - f_t + 0.5) / (f_t + 0.5)
        b = (2.2 * f_dt) / (k + f_dt)
        score += math.log10(a * b)
    #print('end',did)
    if score == 0:
        return (150000, did)
    else:
        return (-score,did)

def get_common(docids_dict,numOfTerms,dict_urls,freqs_dict,numOfDocs):
    global terms
    heap_scores = []
    if numOfTerms>1:
        pointers = [0] * numOfTerms
        lens = [len(docids_dict[term]) for term in terms]
        print("Total document entries: ", sum(lens))
        j_max = len(docids_dict[terms[0]])
        while pointers[0] < j_max:
            did = docids_dict[terms[0]][pointers[0]]
            for i in range(1, len(terms)):
                #print(pointers)
                if pointers[i] < lens[i]: #if end of length reached that means no more intersections
                    ls = docids_dict[terms[i]][pointers[i]:]
                    idx,did_new = nextgenQ(ls, did)
                    pointers[i] = pointers[i]+idx #update pointer, since sliced need to add the index
                    if did_new == did:
                        pointers[i] += 1
                        if i+1 == numOfTerms:
                            pointers_new = [i-1 for i in pointers] #since pointers being incremented after every match, need to decrement
                            pointers_new[0]+=1
                            score, did = getBM25(did, dict_urls, numOfTerms, terms, freqs_dict, numOfDocs,pointers_new)
                            heapq.heappush(heap_scores, (score, did))
                            if len(heap_scores) > 20:
                                heap_scores = heapq.nsmallest(20,heap_scores)

                        
                    else:
                        break

                else:
                    pointers[0] = j_max
                    break

            pointers[0] +=1
    else:
        i =0
        l1 = docids_dict[terms[0]]
        heap_scores = []
        n = len(l1)
        print("Total document entries: ", n)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            to_dos = []
            k=0
            for j in range(n):
                fut = executor.submit(getBM25,l1[j],dict_urls,numOfTerms,terms,freqs_dict,numOfDocs,[j]) #process in parallel
                to_dos.append(fut)
            for future  in as_completed(to_dos):
                heapq.heappush(heap_scores,future.result())
                if len(heap_scores) > 20:
                    heap_scores = heapq.nsmallest(20,heap_scores)
    return heap_scores
if __name__ == '__main__':
    global numOfDocs
    global dict_urls
    global numOfDocs
    global sum_doclen
    global dict_lex
    global terms
    dict_lex = load_lex()
    dict_urls, sum_doclen, numOfDocs = load_webinfo()
    avg_doclen = sum_doclen / numOfDocs
    while True:

        query = input("Enter Search Query: ")
        choice = input("Press 1 for disjunctive, 2 for conjunctive: ")
        terms = np.array(query.split())
        terms = np.unique(terms)
        global docids_dict
        docids_dict = {}
        global freqs_dict
        freqs_dict = {}
        global heap_scores
        heap_scores = []
        global numOfTerms
        numOfTerms = len(terms)

        ###get term indexes
        start = timer()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(getTermIndex,term,dict_lex[term]) for term in terms]
            for future in as_completed(futures):
                rt = future.result()
                docids_dict[rt[0]] = rt[1]
                freqs_dict[rt[0]] = rt[2]
        end = timer()
        print("Term fetching time: ",end-start)

        start = timer()
        sorted_items = sorted(docids_dict.items(), key=lambda item: len(item[1])) 
        terms = [i[0] for i in sorted_items] #get terms, sorted according to the number of docIDs
        if choice == '2':
            #print('conjoint')
            heap_scores = get_common(docids_dict,numOfTerms,dict_urls,freqs_dict,numOfDocs)
        else:
            heap_scores = get_union_results(docids_dict,numOfTerms,dict_urls,freqs_dict,numOfDocs)
        end = timer()
        print("BM25 time: ", end - start)
        db = SqliteDict('webTextInfo.sqlite', autocommit=False)
        for k in range(min(10,len(heap_scores))):
            ur = heapq.heappop(heap_scores)
            snippet = ""
            txt = db[str(ur[1])]
            #print (txt)
            txt = txt.split(' ')
            if query in txt: #if whole query matches get that text and text surrounding it
                idx = txt.index (query)
                snippet = txt[0:idx]
                if len(snippet) <50:
                    rem = 50 - len(snippet)
                    snippet += txt[idx:rem]
                elif len(snippet) >50:
                    snippet = txt[idx-25:idx+25]
                #print(snippet)
            else:
                terms = query.split(' ')
                dict_count = {}
                pos = None
                count = 0
                dict_txt = {}
                for term in terms:
                    dict_count[term] = dict_count.get(term,0)+1
                for i in range(len(txt)):
                    dict_txt[txt[i]] = dict_txt.get(txt[i],0)+1
                    if txt[i] in dict_count.keys() and dict_txt[txt[i]] <= dict_count[txt[i]]:
                        count += 1
                        if pos is None:
                            pos=i
                        else:
                            snippet = txt[pos:i+1] #for when there are not all terms present
                    if count == len(terms): #all terms found
                        snippet = txt[pos:i+1]
                        break
                if snippet == "":
                    snippet = txt[pos:pos+50] #worst case, get text with the first match
                if len(snippet) > 50:
                    snippet = snippet[:50]
                
            print('score: ',-ur[0])
            url_info = dict_urls[ur[1]]
            print('url,doc length: ',url_info[0],url_info[1])
            print(' '.join(snippet))


        proceed = input("Another Query? [Y/N]: ")
        if proceed.lower() == 'n':
            print("Bye bye")
            exit()
