from corpus.document import Document
from language_model.lm import LM 

my_doc = Document()
print "loading the document to hdfs"
my_doc.load_to_hdfs("../example", "sample", "en")
print "End of loading"



unit_test = LM(3,"hdfs://rcg-hadoop-01.rcg.sfu.ca:8020/user/rmehdiza/test_java_input2")
####sentence_probability(self, input_sentence, model, model_parameters, smooth_method)
unit_test.sentence_probability("Hello World Bye","simple_ngram","","No")
#unit_test.sentence_probability("Hello World Bye","simple_ngram","","AddOne")
unit_test.sentence_probability("Hello World Bye","interpolation",[0.6,0.3,0.1],"No")
unit_test.sentence_probability("Hello World Bye","interpolation",[0.6,0.3,0.1],"AddOne")


