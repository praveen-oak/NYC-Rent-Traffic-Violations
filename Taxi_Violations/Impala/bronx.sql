use ak7380;
create view bronx as
  (select tdate as cdate, tboro as cboro, tcount, vcount from taxi_count join violations_count
  on tdate = vdate and tboro = vboro
  where tboro = "BX");
select * from bronx order by cdate asc;
