from __future__ import division
from phrase_counter import word_count_compute 

#Language Model Type
#N-gram

#TODO cleaning codes and adding comments
#TODO add alpha, backoff, interpolation 


#Simple ngram 
class LM:
    n = 0
    model = ""
    # model , n , counts , sums, smoothing method
    def __init__(self, model,n):
        self.n=n
        self.model= model
        if self.model == "simple_ngram":
            self.c,self.s,self.v = word_count_compute("hdfs://rcg-hadoop-01.rcg.sfu.ca:8020/user/rmehdiza/test_java_input","hdfs://rcg-hadoop-01.rcg.sfu.ca:8020/user/rmehdiza/models/test2",n,n)
            print self.c.count()

    def sentence_probability(self, input_sentence):
        if self.model == "simple_ngram":
            print self.get_probability(input_sentence)


    def get_probability(self, input_str):
        if self.model == "simple_ngram":
            temp_n = len(input_str.split(" "))
            return int(self.get_count(input_str)[1])/self.s[temp_n-1]

    def get_count(self, input_str):
        temp = self.c.filter(lambda s: s[0] == input_str)
        if temp.count() > 0:
            print input_str
            return temp.first()
        else:
            return 0



if __name__ == "__main__":
    unit_test = LM("simple_ngram",1)
    unit_test.sentence_probability("Hello")
