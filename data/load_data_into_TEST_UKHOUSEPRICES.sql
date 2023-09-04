LOAD DATA INFILE '/home/hduser/PycharmProjects/DSBQ/data/ttt.csv'
INTO TABLE scratchpad.test_ukhouseprices
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '/n'
;
