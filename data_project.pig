lines = LOAD '$input_file' AS (line:chararray);
tuples = FOREACH lines GENERATE FLATTEN(STRSPLIT(line, ','));
projection = FOREACH tuples GENERATE $3, $4, $5, $6, $9, $10, $11, $12, $13, $14, $15, $16, $17;
STORE projection INTO '$output_file' USING PigStorage (',');



