#!/usr/bin/env python
# coding: utf-8

# In[153]:


import pandas as pd
import os
import csv
import sys
import pyodbc


# In[127]:


#initialize the final data frame
df=pd.DataFrame()

# Initialize the root folder where the input files will be present
loc = 'C:\\Users\\shilp\\Desktop\\Prutech\\Input'


# ### Sample ETL Exercise

# In[128]:


def find_delimiter(filename):
    # Every file has a different delimiter.This function fetches delimiter using sniffer in csv
    # Returns delimiter
    
    sniffer = csv.Sniffer()
    try:
        with open(filename) as fp:
            delimiter = sniffer.sniff(fp.read(5000)).delimiter
    except FileNotFoundError:
        print("Sorry,File Not Found")
        sys.exit()
        
    return delimiter


# In[129]:


for root,dirs,files in os.walk(loc):
    #This loops through every file in the root directory
    
    for file in files:
        
        # Collect data from files named like sample_data
        if file.startswith('sample_dat'):      
            delimiter = find_delimiter(os.path.join(root,file))
            df1 = pd.read_csv(os.path.join(root,file),sep =delimiter)
            
            # Get the datasource from the root path of the file 
            r=root.replace(loc,'')
            
            # Insert a new column into the dataframe for the source folder
            df1.insert(len(df1.columns),column ='source',value=r)
            
            # Append to the final dataframe
            #df=df.append(df1)
            df.append(df1)
    


# In[125]:


df


# In[ ]:


# Write to designated output location
df.to_csv(r"C:\Users\shilp\Desktop\Prutech\Output\consolidated_output.1.csv")


# ### Bonus 1

# In[156]:


# Read the material reference data 
df_mat = pd.read_csv("C:\\Users\\shilp\\Desktop\\Prutech\\Input\\data_source_2\\material_reference.csv.txt")

# Rename column name for joining with the original data frame
df_mat = df_mat.rename(columns={'id':'material_id'})

# Initialize the final dataframe
bonus1_df = pd.DataFrame()

for root,dirs,files in os.walk(loc):
    #This loops through every file in the root directory
    
    for file in files:
        
        # Collect data from files named like sample_data
        if file.startswith('sample_dat'):      
            delimiter = find_delimiter(os.path.join(root,file))
            df1 = pd.read_csv(os.path.join(root,file),sep =delimiter)
            
            #From sample_data.1 get the rows with worth more than 1.00
            if file.startswith('sample_data.1'):
                df1=df1[df1['worth']> 1.00]
                
            #From sample_data.2 get calculate worth as worth * material_id   
            if file.startswith('sample_data.3'):
                df1['worth'] =df1['worth']* df1['material_id'] 
                
            
            #From sample_data.3 aggregate on product_name
            if file.startswith('sample_data.2'):
                x = df1.groupby('product_name').agg({'quality':['first'],'material_id': ['max'],'worth': ['sum']})
                print("Aggregated on Product name")
                print(x)
            
            # Get the datasource from the root path of the file 
            r=root.replace(loc,'')
            
            # Insert a new column into the dataframe for the source folder
            df1.insert(len(df1.columns),column ='source',value=r)
            
            # Append the dataframe in each file to the final dataframe
            bonus1_df=bonus1_df.append(df1)
            
        # Join the data frame on material id to get  material name
        final_dataframe= pd.merge(bonus1_df,df_mat,on ="material_id")


# In[157]:


final_dataframe


# ### Load the data frame into table in DB

# In[155]:


# Mock code to load the dataframe into the DB
''' 
server = 'servername' 
database = 'DataFrameWorks' 
username = 'username' 
password = 'password' 
cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()
# Insert Dataframe into SQL Server:
for index, row in final_dataframe.iterrows():
     cursor.execute("INSERT INTO PRODUCT.ProductDetails (product_name,quality,material_id,worth,source,material_name) values(?,?,?,?,?,?)", row.product_name, row.quality, row.material_id,row.worth,row.source,row.material_name)
cnxn.commit()
cursor.close()
'''


# In[148]:


x


# In[ ]:




