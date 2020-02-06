import numpy as np
import pandas as pd

def read_files(filename):

   filename = r"L:\Caswell\Data Dumps\Cross Client RDE.txt"

   rs_response = input('Make RS RDE? (Y/N)')

   df = pd.read_csv(filename, 
      sep='\t', 
      chunksize=200000,
   )

   for chunk in df:

      #change headers
      chunk = map_column_headers(chunk)

      #remove cancelled and null status
      chunk = chunk[chunk.Status != 'Cancelled']
      chunk = chunk[~chunk['Status'].isnull()]

      #map lookup tables
      chunk = map_lookup_tables(chunk)

      if rs_response == "Y":
         rs_ede_create(chunk)

def map_column_headers(df):

   #set mapping table
   mapping_table = pd.read_excel('L:\\Caswell\\Mappingv2.xlsx',
      sheet_name='Wand DB', 
      index_col= 'wand_DB',
   )

   for col in df.columns:

      df.rename(columns = {col : mapping_table.loc[col].Tibco}, 
         inplace = True,
      )

   return df

def map_lookup_tables(df):

   #set job category table
   mapping_table = pd.read_excel('L:\\Caswell\\Mappingv2.xlsx',
      sheet_name='JobCat',
      index_col= 'wand_jobcat',
   )

   #job category mapping   
   df = pd.merge(
      df,mapping_table, 
      left_on='Job_Category', 
      right_on='wand_jobcat', 
      how='left',
   )

   #set term reason table
   mapping_table = pd.read_excel('L:\\Caswell\\Mappingv2.xlsx',
      sheet_name='Term', 
      index_col= 'term_reason',
   )
   
   #reason for term mapping  
   df = pd.merge(
      df,mapping_table, 
      left_on='Reason_for_Term', 
      right_on='term_reason', 
      how='left',
   )
   
   #set client table
   mapping_table = pd.read_excel('L:\\Caswell\\Mappingv2.xlsx',
      sheet_name='Client', 
      index_col= 'Client',
   )
   
   #client mapping  
   df = pd.merge(
      df,mapping_table, 
      left_on='Client', 
      right_on='Client', 
      how='left',
   )

   return df

def rs_ede_create(df):

   df = df[df.pro_rs == 'Rightsourcing']

   #export chunk to .txt
   df.to_csv(r'L:\RightSourcing\Jon\MSTR Base Files\RS RDE.txt', 
      sep='\t',
      mode='a',
      index=False,
   )


def get_report_num():

   df = pd.read_excel(r'L:\Caswell\MstrFtpScript\File Summary.xlsx', 
      'Summary', 
      index_col="Report#",
   )

   report_num = int(input("Enter Report#"))

   print(df.loc[report_num, 'File Name'])
