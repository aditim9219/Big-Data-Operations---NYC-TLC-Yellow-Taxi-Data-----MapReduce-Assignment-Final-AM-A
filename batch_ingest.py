
#############################################
#Task 3. Bulk import data from next two files in the dataset on your EMR cluster to your HBase Table using the relevant codes.
#Note: For the above task 3, you just need to import data from the subsequent 2 csv files (i.e. yellow_tripdata_2017-03.csv & yellow_tripdata_2017-04.csv) on your EMR cluster.
#############################################

#Importing all the required libraries for import
import happybase as hb
from datetime import datetime

#Creating connecton to hbase localhost
Conn = hb.Connection('localhost')

#open connection to perform operations
def open_connection():
    Conn.open()

#close the opened connection
def close_connection():
    Conn.close()

#get the pointer to a table
def get_table(name):
    open_connection()
    table = Conn.table(name)
    return table

#batch insert data
def batch_insert_data(filename):
    print("Starting batch insert")
    file = open(filename, "r")
    table_name = 'NYC_TRIPS_hbase' #table name
    table = get_table(table_name) #getting table object
    
    start_time = datetime.now()
    i = 0
    line_num = 0
    print ('batch insert started for file: ' , filename)
    #working on batches batch_size as 50000
    with table.batch(batch_size=50000) as bat:

        print('inside batch:')
        for line in file:
            if line_num % 1000 == 0:
                print(line_num, 'Lines loaded')
                if i!=0:
                    temp = line.strip().split(",")
                    bat.put(temp[0]+":"+temp[1]+":"+temp[2] ,{ 'Trip_Details:VendorID':temp[0] ,'Trip_Details:tpep_pickup_datetime':temp[1] ,'Trip_Details:tpep_dropoff_datetime':temp[2] ,'Trip_Details:passenger_count':temp[3] ,'Trip_Details:trip_distance':temp[4] , 'Trip_Details:RatecodeID':temp[5] ,'Trip_Details:store_and_fwd_flag':temp[6] ,'Trip_Details:PULocationID':temp[7] ,'Trip_Details:DOLocationID':temp[8] , 'Trip_Details:payment_type':temp[9] ,'Trip_Details:fare_amount':temp[10] , 'Trip_Details:extra':temp[11] ,'Trip_Details:mta_tax':temp[12] ,'Trip_Details:tip_amount':temp[13] ,'Trip_Details:tolls_amount':temp[14] ,'Trip_Details:improvement_surcharge':temp[15] ,'Trip_Details:total_amount':temp[16] ,'Trip_Details:congestion_surcharge':temp[17] ,'Trip_Details:airport_fee':temp[18] })
            i+=1
            line_num+= 1
        file.close()
        print("batch insert done for file: ", filename," line_num :",line_num)
        print(" ====================== Run time: ",datetime.now()-start_time," ========================= ")
    close_connection()

#defining the list of files to be bulk imported
Importfiles = ['yellow_tripdata_2017-03.csv','yellow_tripdata_2017-04.csv']

#performing batch insert for the files listed in Importfiles 
for file in Importfiles:
     batch_insert_data(file)

