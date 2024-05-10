
#############################################
#Code for - What are the different payment types used by customers and their count? The final results should be in a sorted format.
#############################################


#Importing all libraries required for map reduce job
from mrjob.job import MRJob
from mrjob.step import MRStep


#MRjob class for payment type and count.
class MRPmtTypeCount(MRJob):
    #steps for mapper, reducer and sorting the data
    def steps(self):
        return [
            MRStep(
                mapper = self.mapper,
                reducer = self.reducer,
            )
            ,
            MRStep(
                reducer = self.sorting_reducer_data

            )
        ]

    #mapper for mapping key value pair
    def mapper(self, _, line):
        rec = line.split(",") #getting the record
        pmttype = rec[9]  #Payment type
        
        #ignoring the header row
        if pmttype != 'payment_type':
            yield (pmttype, 1)

    #aggregates to form a pair of count for each payment type
    def reducer(self, key, values):  
        yield None, (sum(values),key)

    #Performing sort operation
    def sorting_reducer_data(self,_,pmt_kv):
        for pmt,key  in sorted(pmt_kv):
            yield ( pmt, key)
                     

if __name__ == '__main__':
    MRPmtTypeCount.run()
