# -*- coding: utf-8 -*-
"""
Created on Sun Feb 18 17:58:23 2024

@author: Viren@InCred
Helper Class for making 
"""
class TradeGroup:
    _id_counter = 1  # Class variable to generate unique group IDs

    def __init__(self):
        self.group_id = TradeGroup._id_counter
        TradeGroup._id_counter += 1
        self.entry_date = None
        self.combined_entry_price = None
        self.tickers = {}  # Dictionary to store tickers and their details
        self.combinedSL = None
        self.combinedTarget = None

    def add_ticker(self, ticker, quantity, entry_price, sl, target):
        if ticker not in self.tickers:
            self.tickers[ticker] = {'quantity': quantity, 'entry_price': entry_price, 'sl': sl, 'target': target}
        else:
            print(f"Ticker '{ticker}' already exists in the trade group.")

    def set_entry_date(self, entry_date):
        self.entry_date = entry_date
        
    def set_combined_entry_price(self, combined_entry_price):
        self.combined_entry_price = combined_entry_price
        
    def set_combined_sl(self, combined_sl):
        self.combinedSL = combined_sl

    def set_combined_target(self, combined_target):
        self.combinedTarget = combined_target

    def search_by_group_id(self, group_id):
        if self.group_id == group_id:
            return self

    def delete_by_group_id(self, group_id):
        if self.group_id == group_id:
            del self
            return self.group_id

    def search_ticker_details(self, ticker):
        if ticker in self.tickers:
            return self.tickers[ticker]

    def drop_ticker(self, ticker):
        if ticker in self.tickers:
            del self.tickers[ticker]
            return self.group_id

    def get_group_id(self):
        return self.group_id

'''
# Example usage:
group1 = TradeGroup()

group1.entry_date ='2024-02-20'
group1.entry_price = 100 
group1.add_ticker('TCS', 10, 105, 95, 115)
group1.add_ticker('INFY', 5, 2000, 1900, 2100)
group1.set_combined_sl(90)

# Search by group ID
group_found = group1.search_by_group_id(1)
print("Trade Group Found by Group ID:", group_found.group_id)

# Delete by group ID
group1.delete_by_group_id(1)
print("Trade Group Deleted by Group ID")

# Search ticker details
ticker_details = group1.search_ticker_details('AAPL')
print("Ticker Details:", ticker_details)

# Drop ticker
group1.drop_ticker('AAPL')
print("AAPL Ticker Dropped")

# Printing modified group
print("Modified Trade Group:", group1.__dict__)

trade_groups = {}

# Add TradeGroup objects to the dictionary
for _ in range(5):  # Example: Adding 5 TradeGroup objects
    new_group = TradeGroup()
    trade_groups[new_group.get_group_id()] = new_group

'''