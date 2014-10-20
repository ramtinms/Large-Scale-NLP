from __future__ import division
import numpy as np
from pyspark import SparkContext

### Storage counts_file, lm_config.json{N, sums , counts}
### returning objects 

def get_ngrams(line,MIN_N = 1,MAX_N = 4):
    #TODO add start of sent
    results = []
    tokens = line.split(' ')
    n_tokens = len(tokens)
    for i in xrange(n_tokens):
        for j in xrange(i+MIN_N, min(n_tokens, i+MAX_N)+1):
            results.append((" ".join(tokens[i:j]),1))
    return results

def is_ngram(phrase,n):
    if len(phrase[0].split(' '))==n:
        return True
    else:
        return False

def word_count_compute(hdfs_input,hdfs_output,min_n,max_n):

    #TODO add start of sent
    # TODO large max size
    sums = []
    vocab_size = []
    sc = SparkContext("local","Simple Language Model Computing")
    file    = sc.textFile(hdfs_input)
    counts  = file.flatMap(lambda a : get_ngrams(a,min_n,max_n)).reduceByKey(lambda a, b: a + b)
    for i in range(min_n, max_n+1):
        temp = counts.filter(lambda a: is_ngram(a,i))
        temp_sum = sum(x[1] for x in temp.collect())
        sums.append(temp_sum)
        temp_counts = temp.count()
        vocab_size.append(temp_counts)
        #print i,temp_counts,temp_sum

    #print sums,vocab_size
    return counts,sums,vocab_size

#    trigrams_norm = trigrams.map(lambda a: (a[0], a[1]/totalsum)).reduceByKey(lambda a,b: a+b)
#    trigrams_numbers = trigrams_norm.count()


#TODO query

# Add one smoothing
#    temp = uni_norm.filter(lambda s: s[0] == "Hello")
#    if temp.count() > 0:
#        print temp.first()
#    else:
#        print "not found"

    
    #uni_norm.saveAsTextFile("hdfs://rcg-hadoop-01.rcg.sfu.ca:8020/user/rmehdiza/temp1")
    

    #counts.saveAsTextFile("hdfs://rcg-hadoop-01.rcg.sfu.ca:8020/user/rmehdiza/temp")
   
#class lm:
#    def give_probability(input_str):
#
#    

if __name__ == "__main__":

    c,s,v = word_count_compute("hdfs://rcg-hadoop-01.rcg.sfu.ca:8020/user/rmehdiza/test_java_input","hdfs://rcg-hadoop-01.rcg.sfu.ca:8020/user/rmehdiza/models/test2",1,3)
    temp = c.filter(lambda s: s[0] == "Hello")
    if temp.count() > 0:
        print temp.first()
    else:
        print "not found"

#    unittest = Simple_lm()

#    unittest.word_count_compute("hdfs://rcg-hadoop-01.rcg.sfu.ca:8020/user/rmehdiza/test_java_input")
    #if len(sys.argv) != 3:
    #    print >> sys.stderr, "Usage: lm <file> <k> <convergeDist>"
    #    exit(-1)

#    sc = SparkContext("local","Simple Language Model Computing")
#    file = sc.textFile("hdfs://rcg-hadoop-01.rcg.sfu.ca:8020/user/rmehdiza/test_java_input")
#    counts  = file.flatMap(ngram2) \
#             .reduceByKey(lambda a, b: a + b)
#    counts.saveAsTextFile("hdfs://rcg-hadoop-01.rcg.sfu.ca:8020/user/rmehdiza/results2")
