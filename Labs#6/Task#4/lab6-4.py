# Task 4 - Get the Sorted Amount Spent by Customer

from mrjob.job import MRJob
from mrjob.step import MRStep

class TotalAmountbyCustomer(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_amount,
                   reducer=self.reducer_sum_amount),
            MRStep(mapper=self.mapper_make_amount_key,
                   reducer=self.reducer_output_amount)
        ]

    def mapper_get_amount(self, _, line):
        (customerID, itemID, amountSpent) = line.split(',')
        yield customerID, float(amountSpent)

    def reducer_sum_amount(self, customerID, amounts):
        yield customerID, sum(amounts)

    def mapper_make_amount_key(self, customerID, totalAmount):
        yield None, (totalAmount, customerID)

    def reducer_output_amount(self, _, customer_amounts):
        for totalAmount, customerID in sorted(customer_amounts, reverse=True):
            yield customerID, totalAmount

if __name__ == '__main__':
    TotalAmountbyCustomer.run()
