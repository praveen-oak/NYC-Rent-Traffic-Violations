use ak7380;
create view queens as
  (select tdate as cdate, tboro as cboro, tcount, vcount from taxi_count join violations_count
  on tdate = vdate and tboro = vboro
  where tboro = "Q");
select * from queens order by cdate asc;
