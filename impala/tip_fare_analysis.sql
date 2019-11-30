
select count(*), dropoff_decile, avg(dropoff_rent) from ppo208.final_data where tip_percent = 0 and fare_amount > 2 group by dropoff_decile;
select count(*), pickup_decile, avg(pickup_rent) from ppo208.final_data where tip_percent = 0 and fare_amount > 2 group by pickup_decile;

select count(tip_percent),avg(pickup_rent), pickup_decile from ppo208.final_data where tip_percent < 0.7 and fare_amount > 2 group by pickup_decile;
select count(tip_percent),avg(dropoff_rent), dropoff_decile from ppo208.final_data where tip_percent < 0.7 and fare_amount > 2 group by dropoff_decile;

select sum(tip_percent),avg(pickup_rent), pickup_decile from ppo208.final_data where tip_percent < 0.7 and fare_amount > 2 group by pickup_decile;
select sum(tip_percent),avg(dropoff_rent), dropoff_decile from ppo208.final_data where tip_percent < 0.7 and fare_amount > 2 group by dropoff_decile;