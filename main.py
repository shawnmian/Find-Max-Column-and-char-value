import csv
import re

#open the files to be read and written


file_name = 'current-position_Export.csv'
file = open(file_name)
outputfilename = re.sub('.csv','_output',file_name)
outputFile = open(outputfilename, 'w', newline='', encoding='utf-8')
outputWriter = csv.writer(outputFile)
reader = csv.reader(file)
data = list(reader)

table_name = re.sub('-','_',file_name)
table_name = re.sub('_Export.csv', '', table_name)


outputWriter.writerow(['file column name','table column name','data type','key'])
#function to name columns properly
def column_namer(txt): 
  x = re.sub(" ", "_", txt)
  x = x.lower()
  return x

number_of_rows = len(data)
number_of_columns = len(data[0])

for y in range(number_of_columns):
  max_len = 0
  for x in range(number_of_rows):
    if len(data[x][y]) > max_len:
      max_len = len(data[x][y])
  outputWriter.writerow([data[0][y], column_namer(data[0][y]), 'varchar(' + str(max_len) + ')'])


#close files after writing or it will not write properly
file.close()
outputFile.close()

file2 = open(outputfilename)
reader2 = csv.reader(file2)
data2 = list(reader2)
column_name = []
varchar_columns = []

number_of_rows2 = len(data2)



x=1
for x in range(len(data2)):
  column_name.append(data2[x][1])
  varchar_columns.append(data2[x][2])

column_name.pop(0)
varchar_columns.pop(0)





#code to be printed
print("drop table if exists dw_ingestion." + table_name + ";") 
print("create table dw_ingestion."+table_name)
print("(")
for x in range(len(column_name)):
  print(column_name[x] +"    " + varchar_columns[x] + ",")
print(")")
print(";")
print()
print("truncate table dw_ingestion." + table_name + ";")
print("copy dw_ingestion."+table_name)
print("from \'s3://asu-s3dl-eds/working/interfolio/" + file_name + ".gz\'" )
print("iam_role \'arn:aws:iam::306276705765:role/redshift-asuedwnp\'")
print("  maxerror 0")
print("  acceptinvchars \'*\'")
print("  dateformat \'auto\'")
print("  timeformat \'auto\'")
print("  compupdate off")
print("  gzip")
print("  csv")
print("  ignoreheader 1")
print("-- truncatecolumns")
print(";")
print("select * from stl_load_errors where starttime = (select max(starttime) from stl_load_errors);")
print("select * from dw_ingestion."+table_name+" limit 250;")
      
      
  
