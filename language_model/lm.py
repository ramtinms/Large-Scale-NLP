from __future__ import division
from phrase_counter import word_count_compute 
#Language Model Type
#N-gram

#TODO cleaning codes and adding comments
#TODO add alpha, backoff, interpolation 


#Simple ngram 
class LM:
    n = 0
    def __init__(self, n, input_hdfs):
        self.n=n
        self.c,self.s,self.v = word_count_compute(input_hdfs,"hdfs://rcg-hadoop-01.rcg.sfu.ca:8020/user/rmehdiza/models/test2",1,n)

    def get_ngrams(self, line, n):
        line = "<S> "*(n-1)+line
        results = []
        tokens = line.split(' ')
        n_tokens = len(tokens)
        for i in xrange(n_tokens):
            for j in xrange(i+n, min(n_tokens, i+n)+1):
                results.append(" ".join(tokens[i:j]))
        return results

#########
# models = simple_ngram, interpolation, backoff
# parameters for simple_ngram : 
#TODO backoff (absolute, linear) , Kneser-Ney smoothing
########

    def simple_ngram_probability(self, input_sentence, smooth_method, smooth_parameters):
        prob = 1
        if self.n==1:
            tokens = input_sentence.split(" ")
            for word in tokens:
                p = self.get_probability(word)
                prob = prob * p
        else:
            tokens = self.get_ngrams(input_sentence,self.n)
            #print tokens
            for token in tokens:
                parts = token.split(" ")
                print parts[-1], parts[:-1]
                p = self.get_p_of_e_given_f(parts[-1]," ".join(parts[:-1]))
                prob = prob * p
        
    def discount(parts):
        sigma =1 
        #alpha = 0.5        
        return False

    def distribute(parts):
        return False

    def backoff(self, token):
        K1 = 1
        K2 = 3
        parts = token.split(" ")
        value = self.get_probability(token, "No")
        if len(parts) > 1 :
            p = self.get_p_of_e_given_f(parts[-1]," ".join(parts[:-1]),"No")    
            if value >= K2:
                return (model_parameters[1]*self.get_probability(parts[-1], "No"))
            elif value >= K1:
                return discount(parts)
            else:
                return distribute(parts)
        else:
            print "TODO"
            

    def sentence_probability(self, input_sentence, model, model_parameters, smooth_method):
        if model == "simple_ngram":
            prob = 1
            if self.n==1:
                tokens = input_sentence.split(" ")
                for word in tokens:
                    p = self.get_probability(word,smooth_method)
                    prob = prob * p
            else:
                tokens = self.get_ngrams(input_sentence,self.n)
                for token in tokens:
                    parts = token.split(" ")
                    p = self.get_p_of_e_given_f(parts[-1]," ".join(parts[:-1]),smooth_method)
                    prob = prob * p

        elif model == "backoff":
            tokens = self.get_ngrams(input_sentence,self.n)
            prob = 1
            for token in tokens:
                prob = prob * backoff(self,token) 

            print "TODO"

        elif model == "interpolation":
            tokens = self.get_ngrams(input_sentence,self.n)
            prob = 1
            for token in tokens:
                parts = token.split(" ")
                p  = ( 1/self.v[0]) * model_parameters[0]
                p  = p + (model_parameters[1]*self.get_probability(parts[-1], smooth_method))
                if self.n > 2:
                    for i in xrange(2,self.n):
                        parts = token.split(" ")
                        p = p + (model_parameters[i]*self.get_p_of_e_given_f(parts[-1]," ".join(parts[:-1]),smooth_method)) 
                prob = prob * p
        print prob
        return prob

    def get_probability(self, input_str, smooth_method):
        temp_n = len(input_str.split(" "))
        if smooth_method == "AddOne":
            print int(self.get_count(input_str)[1])+1
            print (self.s[temp_n-1]+self.v[temp_n-1])
            return int((self.get_count(input_str)[1])+1)/(self.s[temp_n-1]+self.v[temp_n-1])
        elif smooth_method == "No":
            return int(self.get_count(input_str)[1])/self.s[temp_n-1]

    def get_p_of_e_given_f(self, e, f, smooth_method):
        p1 = self.get_probability(f+" "+e,smooth_method)
        p2 = self.get_probability(f,smooth_method)
        return p1/p2

    def get_count(self, input_str):
        temp = self.c.filter(lambda s: s[0] == input_str)
        if temp.count() > 0:
            return temp.first()
        else:
            return (0,0)



if __name__ == "__main__":
    unit_test = LM(3,"hdfs://rcg-hadoop-01.rcg.sfu.ca:8020/user/rmehdiza/test_java_input")
    #sentence_probability(self, input_sentence, model, model_parameters, smooth_method)
    unit_test.sentence_probability("Hello World Bye","simple_ngram","","No")
    unit_test.sentence_probability("Hello World Bye","simple_ngram","","AddOne")
    unit_test.sentence_probability("Hello World Bye","interpolation",[0.6,0.3,0.1],"No")
    unit_test.sentence_probability("Hello World Bye","interpolation",[0.6,0.3,0.1],"AddOne")
