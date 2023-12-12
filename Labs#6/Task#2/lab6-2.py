#Task #2 - Get the Minimum Temperature per Capital
#Hadoop MapReduce

from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMinTemperature(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_temperatures,
                   reducer=self.reducer_min_temperature)
        ]

    def mapper_get_temperatures(self, _, line):
        
        # Splitting the CSV line into fields
        # Extracting relevant information
        station, date, temperature_type, temperature, *_ = line.split(',')
        
        # Checking if the temperature type is 'TMIN'
        if temperature_type == 'TMIN':
            # Emitting the station as the key and the temperature as the value
            yield station, int(temperature)

    def reducer_min_temperature(self, station, temperatures):
        # Emitting the station and the minimum temperature
        yield station, min(temperatures)

if __name__ == '__main__':
    MRMinTemperature.run()