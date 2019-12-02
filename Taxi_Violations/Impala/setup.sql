use ak7380;
create external table taxi(tdate string, tboro string)
  row format delimited fields terminated by '\t'
  location '/user/ak7380/TaxiDataOutput';
create external table violations(vdate string, vboro string)
  row format delimited fields terminated by '\t'
  location '/user/ak7380/ViolationsOutput';
create view taxi_count as
  (select tdate, tboro, count(*) as tcount from taxi
  group by tboro, tdate);
create view violations_count as
  (select vdate, vboro, count(*) as vcount from violations
  group by vboro, vdate);
