#!/bin/bash
python process_dimension_tables.py

python create_fact_tables.py
wait
python process_transactions_table.py
python process_details_table.py 

wait

python create_analytics_table.py