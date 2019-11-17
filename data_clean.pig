lines = LOAD '$input_file' USING PigStorage(',') AS (    vendor_id:chararray,
                                                        pickup_datetime:chararray,
                                                        dropoff_datetime:chararray,
                                                        passenger_count:int,
                                                        trip_distance:float,
                                                        rate_code:int,
                                                        store_and_fwd_flag: chararray,
                                                        pickup_location_id: chararray,
                                                        dropoff_location_id: chararray,
                                                        payment_type:int,
                                                        fare_amount:float,
                                                        surcharge:float,
                                                        mta_tax:float,
                                                        tip_amount:float,
                                                        tolls_amount:float,
                                                        improvement_surcharge:float,
                                                        total_amount:float);

projection = FOREACH lines GENERATE     trip_distance,
                                        pickup_location_id,
                                        dropoff_location_id,
                                        payment_type,
                                        fare_amount,
                                        surcharge,
                                        tip_amount,
                                        tolls_amount,
                                        total_amount;


projection = FILTER projection BY trip_distance < 50 AND trip_distance > 0.01;
projection = FILTER projection BY tip_amount < 200 AND tip_amount >=0;
projection = FILTER projection BY total_amount < 500 AND total_amount >=0;
STORE projection INTO '$output_file' USING PigStorage (',');

