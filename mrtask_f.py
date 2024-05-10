#############################################
#File Name: mrtask_f
# How does revenue vary over time? 
# Calculate the average trip revenue per month 
# - analysing it by hour of the day (day vs night) and the day of the week (weekday vs weekend).
#############################################

#Importing all libraries required for map reduce job
from mrjob.job import MRJob
from datetime import datetime

#defining class for analyzing average trip revenue
class AverageRevenueOverTime(MRJob):
    #function to parse datetime in a specific format
    def parse_datetime(self, datetime_str):
        formats = ['%d-%m-%Y %H:%M:%S', '%d-%m-%Y %H:%M', '%Y-%m-%d %H:%M', '%Y-%m-%d %H:%M:%S']
        for fmt in formats:
            try:
                return datetime.strptime(datetime_str, fmt)
            except ValueError:
                pass
        raise ValueError('no valid date format found')

    #Class for main mapper 
    def mapper(self, _, line):
        # Skip the header line
        if not line.startswith('VendorID'):
            row = line.split(',')
            revenue = float(row[16])
            pickup_datetime = self.parse_datetime(row[1])
            month = pickup_datetime.month
            hour = pickup_datetime.hour
            weekday = pickup_datetime.weekday()
            yield (month, hour, weekday), revenue
     #reducer function to find mean revenue value for each key
    def reducer(self, key, values):
        total_revenue = 0
        num_trips = 0

        for revenue in values:
            total_revenue += revenue
            num_trips += 1

        average_revenue = total_revenue / num_trips

        yield key, average_revenue
#main function
if __name__ == '__main__':
    AverageRevenueOverTime.run()