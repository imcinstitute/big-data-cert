raw_log_entries = LOAD '/user/cloudera/weblog/nasa.dat' USING TextLoader AS (line:chararray);


logs_base = FOREACH raw_log_entries GENERATE FLATTEN(REGEX_EXTRACT_ALL(line,'^([^\\s]+) - - \\[(.+)\\] \\"([^\\s]+) (/[^\\s]*) HTTP/[^\\s]+\\" [^\\s]+ ([0-9]+)$')) AS (host: chararray, time: chararray, method: chararray, object: chararray, size: chararray);


store logs_base into '/user/cloudera/csvoutput' using PigStorage(',','-schema');

