
#############################################
#Code for : Calculate the average tips to revenue ratio of the drivers for different locations in sorted format.
#############################################

#importing all the libraries for this mapreduce task
import pandas as pd 
from mrjob.job import MRJob
from mrjob.step import MRStep

#Defining the class for MRJob
class MRAvgTipRevRatio(MRJob):
    
    #Defining all the steps for mapper and reducer jobs
    def steps(self):
        return [
            MRStep(
                mapper = self.MainMapper, #adding steps for mapper
                reducer = self.MainReducer, #adding step for reducer
            ),
            MRStep(
                reducer = self.sorting_reducer_data #adding step for sorting data after reducer runs

            )
        ]

    #reducer function
    def MainMapper(self, _, line):
        rec = line.split(",") #reading the line to get the record
        PULoc = rec[7] #Pickup location
        
        #Eliminating header row
        if rec[0] != 'VendorID':
            Tip = float(rec[13])
            Rev = float(rec[16])

            #Check to eliminate devide by zero issue.        
            if Rev > 0:
                TipRevRatio = Tip/Rev # getting ratio for tip and revenue (total_amount)
            else:
                TipRevRatio = Tip
            yield ( PULoc,TipRevRatio) # forming key value
    
    #defining reducer function
    def MainReducer(self, key, values):
        yield None,  (sum(values)/len(key),key)

    #Perfming sort operation
    def sorting_reducer_data(self,_,trip_rev_ratios):
        for tr_ratio,key  in sorted(trip_rev_ratios):
            yield ( tr_ratio, key)

#main function
if __name__ == '__main__':
    MRAvgTipRevRatio.run()
