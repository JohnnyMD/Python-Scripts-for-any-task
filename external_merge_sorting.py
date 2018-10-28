from io     import StringIO
from time   import time

def external_merge_sort(n: int, source: open, sink: open, file_opener = open, key = 0, sep=" ")->None:
    '''
        Approach:
            Break the <source> into files of size <n> (bytes of lines)
            sort each of these files by the <key> (using <sep>)
            and merge these onto the <sink> file.
    '''
    start_EMST = time()
    print("\nBatch size:  <{:,}> Bytes".format(n))
    print("\nGoing to sort the single batches...\n" )
    # store SORTED chunks into files of size n
    mergers = []
    i = 0
    while True:
        i += 1
        lines = source.readlines(n)
        if not len(lines):
            break;
        
        lines = [line for line in sorted(lines, key=lambda line: line.split(sep)[key])]
        
        merge_me = file_opener()
        merge_me.write(''.join(lines))
        mergers.append(merge_me)
        merge_me.seek(0)
        print("\tTime passed to sort {} batches: < {} >  min\n".format( i, (time() - start_EMST)/60 ) )
 
    # merge onto sink
    print("\nGoing to merge the sorted files...\n")
    tops = [f.readline() for f in mergers]
    while tops:
        c = min(tops)
        sink.write(c)
        i = tops.index(c)
        t = mergers[i].readline()
        if t:
            tops[i] = t
        else:
            del tops[i]
            mergers[i].close()
            del mergers[i]  # __del__ method of file_opener should delete the file
            
    print("\tDone !\n\n\tTime passed for EXTERNAL MERGE SORTING of the big file: < {} >  min\n".format( (time() - start_EMST)/60 ) )  
