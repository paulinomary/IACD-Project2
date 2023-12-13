# Task 3 - Sort the word frequency in a book
from mrjob.job import MRJob
from mrjob.step import MRStep
import string

class MRWordFrequencySort(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   reducer=self.reducer_count_words),
            MRStep(mapper=self.mapper_sort_words,
                   reducer=self.reducer_output_sorted_words)
        ]

    def mapper_get_words(self, _, line):
        words = line.translate(str.maketrans('', '', string.punctuation)).split()
        for word in words:
            yield word.lower(), 1

    def reducer_count_words(self, word, counts):
        yield word, sum(counts)

    def mapper_sort_words(self, word, count):
        yield count, word

    def reducer_output_sorted_words(self, count, words):
        for word in words:
            yield count, word

if __name__ == '__main__':
    MRWordFrequencySort.run()
