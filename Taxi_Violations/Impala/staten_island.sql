use ak7380;
create view staten_island as
  (select tdate as cdate, tboro as cboro, tcount, vcount from taxi_count join violations_count
  on tdate = vdate and tboro = vboro
  where tboro = "ST");
select * from staten_island order by cdate asc;
