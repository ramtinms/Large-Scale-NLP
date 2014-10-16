from __future__ import division
import numpy as np
from pyspark import SparkContext

#def compute_unigram():


def ngram(line):
    MIN_N = 1
    MAX_N = 4
    results = []
    tokens = line.split(' ')
    n_tokens = len(tokens)
    for i in xrange(n_tokens):
        for j in xrange(i+MIN_N, min(n_tokens, i+MAX_N)+1):
            results.append((" ".join(tokens[i:j]),1))
    return results

def unigram(phrase):
    if len(phrase[0].split(' '))>1:
        return False
    else:
        return True

def bigram(phrase):
    if len(phrase[0].split(' '))==2:
        return True
    else:
        return False

def trigram(phrase):
    if len(phrase[0].split(' ')) == 3:
        return True
    else:
        return False

def unigram_probability(phrase):
    temp = uni_norm.filter(lambda s: phrase == s)
    print temp

def compute_lm(sentence):
    words = sentence.split(" ")
    #probability = 

def word_count_compute(hdfs_input):
    sc = SparkContext("local","Simple Language Model Computing")
    file    = sc.textFile(hdfs_input)
    counts  = file.flatMap(ngram).reduceByKey(lambda a, b: a + b)
    uni = counts.filter(unigram)

    totalsum = sum(x[1] for x in uni.collect())
    uni_norm = uni.map(lambda a: (a[0], a[1]/totalsum)).reduceByKey(lambda a,b: a+b)

    bigrams = counts.filter(bigram)
    totalsum = sum(x[1] for x in bigrams.collect())
    bigrams_norm = bigrams.map(lambda a: (a[0], a[1]/totalsum)).reduceByKey(lambda a,b: a+b)

    trigrams = counts.filter(trigram)
    totalsum = sum(x[1] for x in trigrams.collect())
    trigrams_norm = trigrams.map(lambda a: (a[0], a[1]/totalsum)).reduceByKey(lambda a,b: a+b)

# TODO object saving
#    uni_norm.saveAsObjectFile("hdfs://rcg-hadoop-01.rcg.sfu.ca:8020/user/rmehdiza/models/unigram")
#    bigrams_norm.saveAsObjectFile("hdfs://rcg-hadoop-01.rcg.sfu.ca:8020/user/rmehdiza/models/bigram")
#    trigrams_norm.saveAsObjectFile("hdfs://rcg-hadoop-01.rcg.sfu.ca:8020/user/rmehdiza/models/trigram")


#TODO query
    temp = uni_norm.filter(lambda s: "hadoop" == s)
    #print temp.first()
 

    
    #uni_norm.saveAsTextFile("hdfs://rcg-hadoop-01.rcg.sfu.ca:8020/user/rmehdiza/temp1")
    

    #counts.saveAsTextFile("hdfs://rcg-hadoop-01.rcg.sfu.ca:8020/user/rmehdiza/temp")
   



class Simple_lm:

    def __init__(self):
        self.sc = SparkContext("local","Simple Language Model Computing")

    def ngram(self, line):
        MIN_N = 1
        MAX_N = 4
        results = []
        tokens = line.split(' ')
        n_tokens = len(tokens)
        for i in xrange(n_tokens):
            for j in xrange(i+MIN_N, min(n_tokens, i+MAX_N)+1):
                results.append((" ".join(tokens[i:j]),1))
        return results

    def word_count_compute(self, hdfs_input):
        file    = self.sc.textFile(hdfs_input)
        counts  = file.flatMap(self.ngram).reduceByKey(lambda a, b: a + b)
        counts.saveAsTextFile("hdfs://rcg-hadoop-01.rcg.sfu.ca:8020/user/rmehdiza/temp")


if __name__ == "__main__":

    word_count_compute("hdfs://rcg-hadoop-01.rcg.sfu.ca:8020/user/rmehdiza/test_java_input")

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
