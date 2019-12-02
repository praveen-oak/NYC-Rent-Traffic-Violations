use ak7380;
create view brooklyn as
  (select tdate as cdate, tboro as cboro, tcount, vcount from taxi_count join violations_count
  on tdate = vdate and tboro = vboro
  where tboro = "BK");
select * from brooklyn order by cdate asc;
