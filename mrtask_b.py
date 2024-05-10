
#############################################
#Code for - Which pickup location generates the most revenue? 
#############################################

# importing required libraries
from mrjob.job import MRJob
from mrjob.step import MRStep
class MRLocRev(MRJob):

    #MR job steps for mapper, reducer and finding the max
    def steps(self):
        return [
            MRStep(
                mapper = self.mapper,
                reducer = self.reducer,
            )
            ,
            MRStep(
                reducer = self.reducer_find_max

            )
        ]

    #mapper to map key value
    def mapper(self, _, line):
        rec = line.split(",") #Reading the line
        PULoc = rec[7] #Pickup location
        TotAmt = rec[16] #Total amount / revenue
        
        #to skip the header
        if TotAmt != 'total_amount':
            yield ( PULoc, float(TotAmt.strip(',')))

    #reducer aggregates revenue per pickup location
    def reducer(self, key, values):
        yield None, (sum(values),key)

    #finding the maxium from the pair of revenue and pickup location
    def reducer_find_max(self, _,vendor_pair):
        yield max(vendor_pair)

#main function
if __name__ == '__main__':
    MRLocRev.run()
