# Welcome to My M And A
My M And A

## Task
You've worked as a Junior Data Engineer at Plastic Free Boutique for three months.
Your first mission was to build a strong, robust, and scalable customers database for the exponential growth the company will soon have. Your manager is delighted.
We've just acquired a new company, Only Wood Box, which will be a perfect solution for our packaging department. They are experts in making wood packages at a competitive, light, and cheap price.
Expert in their technology, they didn't believe in the digital world. Despite the decent number of customers, they didn't have to invest in their infrastructure. Before quitting, their engineer told us that at least we had stored all the information; I don't understand what he meant.
Your mission will be to merge their three customers (yes 3 :D) table into ours.
Table 1
Table 2
Table 3

## Description
Your function will be called my_m_and_a and receive the 3 CSV content. 2# Import your function from my_ds_babel to save the CSV's content into SQL.

## Installation
import my_ds_babel
import pandas as pd
from io import StringIO
import warnings


## Usage
Merge all three table and convert it to SQL using my_ds_babel.py
merged_csv = my_m_and_a(content_database_1, content_database_2, content_database_3)
my_ds_babel.csv_to_sql(merged_csv, 'plastic_free_boutique.sql', 'customers')

### The Core Team


<span><i>Made at <a href='https://qwasar.io'>Qwasar SV -- Software Engineering School</a></i></span>
<span><img alt='Qwasar SV -- Software Engineering School's Logo' src='https://storage.googleapis.com/qwasar-public/qwasar-logo_50x50.png' width='20px' /></span>
