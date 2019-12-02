use ak7380;
create view manhattan as
  (select tdate as cdate, tboro as cboro, tcount, vcount from taxi_count join violations_count
  on tdate = vdate and tboro = vboro
  where tboro = "NY");
select * from manhattan order by cdate asc;
