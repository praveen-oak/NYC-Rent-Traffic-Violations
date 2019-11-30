
#first get count of all payments per decile of pickup rent
select count(*), pickup_decile from ppo208.final_data where tip_percent < 0.7 group by pickup_decile;
#now repeat but for only credit cards
select count(*), pickup_decile from ppo208.final_data where tip_percent < 0.7 and payment_type = 1 group by pickup_decile;

#repeat the same for dropoff
select count(*), dropoff_decile from ppo208.final_data where tip_percent < 0.7 and payment_type = 1 group by dropoff_decile;
select count(*), dropoff_decile from ppo208.final_data where tip_percent < 0.7 group by dropoff_decile;