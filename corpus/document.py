# Processing Documents and Upload them in HDFS
# Author : Ramtin M. Seraj 
#

import subprocess
import json


class Document:
    def read_configuration(self, config_file):
        try:
            json_data = open(config_file)
            data = json.load(json_data)
            json_data.close()
        except IOError as e:
            print "I/O error({0}): {1}".format(e.errno, e.strerror)
        except:
            print "Unexpected error:", sys.exc_info()[0]
            raise

        hdfs_path = data["HDFS_PATH"] 

    # File format storage : monolingual_file.[tk](tokenized).[lc](lowercased).[Language]
    def load_to_hdfs(self, file_path, file_name, language):
        print "Start loading to hdfs ..."
        subprocess.call("hadoop fs -copyFromLocal "+file_path+"/"+file_name+" "+hdfs_path+"/corpus/monolingual/"file_name+"."+language)
        print "End of loading"
        #TODO storing other informations 

    #def list_documents(self):
        
