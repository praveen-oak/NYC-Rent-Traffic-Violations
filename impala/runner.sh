rm -rf *.csv
impala-shell --query_file=best_worst_tippers.sql -i compute-1-1:21000 --output_file=best_worst_tippers.csv --output_delimiter=','
impala-shell --query_file=payment_type_analysis.sql -i compute-1-1:21000 --output_file=payment_type_analysis.csv --output_delimiter=','
impala-shell --query_file=tip_fare_analysis.sql -i compute-1-1:21000 --output_file=tip_fare_analysis.csv --output_delimiter=','