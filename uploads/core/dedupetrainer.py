import pandas as pd 
from future.builtins import next

import os
import csv
import re
import logging
import optparse

import dedupe 
from unidecode import unidecode


class Dedupetrainer(object):
    @staticmethod
    def preProcess( column):
      """
      Do a little bit of data cleaning with the help of Unidecode and Regex.
      Things like casing, extra spaces, quotes and new lines can be ignored.
      """
      try : # python 2/3 string differences
          column = column.decode('utf8')
      except AttributeError:
          pass
      column = unidecode(column)
      column = re.sub('  +', ' ', column)
      column = re.sub('\n', ' ', column)
      column = column.strip().strip('"').strip("'").lower().strip()
      # If data is missing, indicate that by setting the value to `None`
      if not column:
          column = None
      return column

    @staticmethod
    def readData(filename):
      """
      Read in our data from a CSV file and create a dictionary of records, R programming
      where the key is a unique record ID and each value is dict
      """
      try:
        data_d = {}
        with open(filename) as f:
            reader = csv.DictReader(f)
            for row in reader:
                clean_row = [(k, Dedupetrainer.preProcess(v)) for (k, v) in row.items()]
                row_id = int(row['Id'])
                data_d[row_id] = dict(clean_row)
      except :
        a = pd.read_csv(filename)
        if 'Id' in a.columns:
            #df_new = a[a[['DBA','BUS_ADDR','BUS_ADDR_CITY','BUS_ST_CODE','BUS_ADDR_ZIP','BUS_CNTRY_CDE','EMAIL_ADDR','WEBSITE','BUS_PHNE_NBR']].notnull()]
            a = a .applymap(str)
            data_d = a.to_dict(orient="index")
            return data_d
        else:
            msg = "Your file does not have column as Id"
            print("ERROR -----------", msg)
            return msg
    
    @staticmethod    
    def train(input_file,train_sample,fields):
      try :
        data_d = Dedupetrainer.readData(input_file)
        # Define the fields dedupe will pay attention to
        # Create a new deduper object and pass our data model to it.
        deduper = dedupe.Dedupe(fields)
        print("***********************")
        deduper.sample(data_d, train_sample)
        dedupe.consoleLabel(deduper)
        deduper.train()
        threshold = deduper.threshold(data_d, recall_weight=1)

        # ## Clustering

        # `match` will return sets of record IDs that dedupe
        # believes are all referring to the same entity.

        print('clustering...')
        clustered_dupes = deduper.match(data_d, threshold)

        print('# duplicate sets', len(clustered_dupes))
        cluster_membership = {}
        cluster_id = 0
        for (cluster_id, cluster) in enumerate(clustered_dupes):
            id_set, scores = cluster
            cluster_d = [data_d[c] for c in id_set]
            canonical_rep = dedupe.canonicalize(cluster_d)
            for record_id, score in zip(id_set, scores):
                cluster_membership[record_id] = {
                    "cluster id" : cluster_id,
                    "canonical representation" : canonical_rep,
                    "confidence": score
                }

        singleton_id = cluster_id + 1
        output_file = "./media/output.csv"
        with open(output_file, 'w') as f_output, open(input_file) as f_input:
            writer = csv.writer(f_output)
            reader = csv.reader(f_input)

            heading_row = next(reader)
            heading_row.insert(0, 'confidence_score')
            heading_row.insert(0, 'Cluster ID')
            canonical_keys = canonical_rep.keys()
            for key in canonical_keys:
                heading_row.append('canonical_' + key)

            writer.writerow(heading_row)

            for row in reader:
                row_id = int(row[0])
                if row_id in cluster_membership:
                    cluster_id = cluster_membership[row_id]["cluster id"]
                    canonical_rep = cluster_membership[row_id]["canonical representation"]
                    row.insert(0, cluster_membership[row_id]['confidence'])
                    row.insert(0, cluster_id)
                    for key in canonical_keys:
                        row.append(canonical_rep[key].encode('utf8'))
                else:
                    row.insert(0, None)
                    row.insert(0, singleton_id)
                    singleton_id += 1
                    for key in canonical_keys:
                        row.append(None)
                writer.writerow(row)

        print("process has been completed")
        return "success"  
      
      except Exception as e :
        print("Error in catch ",str(e))
        return "failure"