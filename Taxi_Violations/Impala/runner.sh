impala-shell -i compute-1-1 -f drop.sql
impala-shell -i compute-1-1 -f setup.sql
impala-shell -i compute-1-1 -f manhattan.sql -B -o manhattan.csv --output_delimiter=','
impala-shell -i compute-1-1 -f bronx.sql -B -o bronx.csv --output_delimiter=','
impala-shell -i compute-1-1 -f brooklyn.sql -B -o brooklyn.csv --output_delimiter=','
impala-shell -i compute-1-1 -f queens.sql -B -o queens.csv --output_delimiter=','
impala-shell -i compute-1-1 -f staten_island.sql -B -o staten_island.csv --output_delimiter=','
