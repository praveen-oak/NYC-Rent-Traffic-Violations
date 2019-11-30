select avg(tip_percent) as pickup_worst_avg , pickup_location, avg(pickup_rent)
	from ppo208.final_data where tip_percent < 0.7 and fare_amount > 2 
	group by pickup_location having count(*) > 5000 
	order by pickup_worst_avg limit 10;

select avg(tip_percent) as pickup_best_avg , pickup_location, avg(pickup_rent)
	from ppo208.final_data where tip_percent < 0.7 and fare_amount > 2 
	group by pickup_location having count(*) > 5000 
	order by pickup_best_avg desc  limit 10;

select avg(tip_percent) as dropoff_worst_avg , dropoff_location, avg(dropoff_rent)
	from ppo208.final_data where tip_percent < 0.7 and fare_amount > 2 
	group by dropoff_location having count(*) > 5000 
	order by dropoff_worst_avg  limit 10;

select avg(tip_percent) as dropoff_best_avg , dropoff_location, avg(dropoff_rent)
	from ppo208.final_data where tip_percent < 0.7 and fare_amount > 2 
	group by dropoff_location having count(*) > 5000 
	order by dropoff_best_avg desc  limit 10;
