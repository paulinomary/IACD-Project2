# Average Number of Friends by Age - Task #1
# Hadoop MapReduce

# Importing the libraries
from mrjob.job import MRJob
from mrjob.step import MRStep

# Creating the class
class FriendsByAge(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.get_friends_mapper, reducer=self.count_friends_reducer),
            MRStep(reducer=self.output_reducer)
        ]

    # Get the number of friends
    def get_friends_mapper(self, _, line):
        _, _, age, num_friends = line.split(',')
        yield age, float(num_friends)

    # Count the number of friends
    def count_friends_reducer(self, age, num_friends):
        total, num_elements = 0, 0
        for x in num_friends:
            total += x
            num_elements += 1
        
        average = total / num_elements
        yield age, average

    # Output the results
    def output_reducer(self, age, averages):
        for average in averages:
            yield age, average

if __name__ == '__main__':
    FriendsByAge.run()
