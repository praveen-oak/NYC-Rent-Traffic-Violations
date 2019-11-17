lines = LOAD '$input_file' USING PigStorage(',') AS (    vendor_id:chararray,
                                                        pickup_datetime:chararray,
                                                        dropoff_datetime:chararray,
                                                        passenger_count:int,
                                                        trip_distance:float,
                                                        pickup_longitude:float,
                                                        pickup_latitude:float,
                                                        rate_code:float,
                                                        store_and_fwd_flag: chararray,
                                                        dropoff_longitude:float,
                                                        dropoff_latitude:float,
                                                        payment_type:chararray,
                                                        fare_amount:float,
                                                        surcharge:float,
                                                        mta_tax:float,
                                                        tip_amount:float,
                                                        tolls_amount:float,
                                                        total_amount:float);

projection = FOREACH lines GENERATE     trip_distance,
                                        pickup_longitude,
                                        pickup_latitude,
                                        dropoff_longitude,
                                        dropoff_latitude,
                                        payment_type,
                                        surcharge,
                                        tip_amount,
                                        tolls_amount,
                                        total_amount;
temp_group = GROUP projection all;

pickup_longitude_range = FOREACH temp_group 
           GENERATE MAX(projection.pickup_longitude), MIN(projection.pickup_longitude);
STORE pickup_longitude_range INTO '$output_file';