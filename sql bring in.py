import os
import pandas as pd
import sqlite3 as sql


#Read in Seth's tables --- staging
#1- read in the cleaned csv to a dataframe
#cwd= os.getcwd() + '\\Documents\\ETL Project Data\\'
cwd= os.getcwd()
#cocktail dataframe
filepath=os.path.join(cwd,'ETL_Project_-SuperExtravagant-','csv_output','cocktails_cleaned.csv')
df1=pd.read_csv(filepath,index_col=0)
df1=df1.rename(columns={'city_id':'CityID','cocktail_name':'Name','bartender':'Bartender'})
df1=df1.dropna()


#cities dataframe
filepath=os.path.join(cwd,'ETL_Project_-SuperExtravagant-','csv_output','cities_cleaned.csv')
df2=pd.read_csv(filepath,index_col=0)
df2=df2.rename(columns={'city_id':'CityID','state':'State','lat':'Lat','lng':'Lng','population':'Population','timezone':'TimeZone'})

#cityjoin dataframe
filepath=os.path.join(cwd,'ETL_Project_-SuperExtravagant-','csv_output','city_id.csv')
df3=pd.read_csv(filepath,index_col=0)



#2 - stage table and prep them for the final push to sql
#cities -staging
City=df2.drop(['TimeZone'],axis=1)

#Timezone - staging
tz=df2[['CityID','TimeZone']]

#Timezone-dropping ALL duplicte values
tz_agg=tz.drop_duplicates(subset ="CityID",keep = 'first', inplace = False)



# #3 - make the tbales and push the data  
# #
# filepath=os.path.join(cwd,'SQLiteDB','ETL.db')
# conn = sql.connect(filepath)
# cur = conn.cursor()

# City.to_sql('Cities',conn, if_exists="replace")
# df1.to_sql('Cocktails',conn, if_exists="replace")
# df3.to_sql('CityJoin',conn, if_exists="replace")
# tz_agg.to_sql('TimeZones',conn, if_exists="replace")