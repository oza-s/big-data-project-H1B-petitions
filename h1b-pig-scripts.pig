1) Which employer filed the most number of petitions each year from 2012 to 2016?
h1bdata = LOAD 'hdfs://localhost:9000/h1b-analysis/h1b_dataset.csv' using PigStorage(',') as (ID:int,CASE_STATUS:chararray,EMPLOYER_NAME:chararray,SOC_NAME:chararray,JOB_TITLE:chararray,FULL_TIME_POSITION:chararray,PREVAILING_WAGE:int,YEAR:int,WORKSITE:chararray,LONGITUDE:chararray,LATITUDE:chararray);
groupByEmp = GROUP h1bdata BY (EMPLOYER_NAME,YEAR);
countData = FOREACH groupByEmp GENERATE group.$0, group.$1, COUNT(h1bdata) as APPLICATIONS;
orderBy = ORDER countData BY APPLICATIONS DESC;
STORE orderBy INTO '/h1b-analysis-output/most_petitions_by_employer_analysis' using PigStorage(',');


2) Which state/place had most number of H1B petition with status CERTIFIED?
h1bdata = LOAD 'hdfs://localhost:9000/h1b-analysis/h1b_dataset.csv' using PigStorage(',') as (ID:int,CASE_STATUS:chararray,EMPLOYER_NAME:chararray,SOC_NAME:chararray,JOB_TITLE:chararray,FULL_TIME_POSITION:chararray,PREVAILING_WAGE:int,YEAR:int,WORKSITE:chararray,LONGITUDE:chararray,LATITUDE:chararray);
filterdata = FILTER h1bdata by CASE_STATUS == 'CERTIFIED';
siteLatLongStatus = FOREACH filterdata GENERATE CASE_STATUS, WORKSITE, LATITUDE, LONGITUDE;
groupBySite = GROUP siteLatLongStatus BY (WORKSITE);
countData = FOREACH groupBySite GENERATE group, COUNT(siteLatLongStatus) as APPLICATIONS_DENIED;
orderBydata = ORDER countData BY APPLICATIONS_DENIED ASC;
STORE orderBydata INTO '/h1b-analysis-output/most_petitions_certified_places_analysis' using PigStorage(',');


3) Which state/place had most number of H1B petition with status DENIED?
h1bdata = LOAD 'hdfs://localhost:9000/h1b-analysis/h1b_dataset.csv' using PigStorage(',') as (ID:int,CASE_STATUS:chararray,EMPLOYER_NAME:chararray,SOC_NAME:chararray,JOB_TITLE:chararray,FULL_TIME_POSITION:chararray,PREVAILING_WAGE:int,YEAR:int,WORKSITE:chararray,LONGITUDE:chararray,LATITUDE:chararray);
filterdata = FILTER h1bdata by CASE_STATUS == 'DENIED';
siteLatLongStatus = FOREACH filterdata GENERATE CASE_STATUS, WORKSITE, LATITUDE, LONGITUDE;
groupBySite = GROUP siteLatLongStatus BY (WORKSITE);
countData = FOREACH groupBySite GENERATE group, COUNT(siteLatLongStatus) as APPLICATIONS_DENIED;
orderBydata = ORDER countData BY APPLICATIONS_DENIED ASC;
STORE orderBydata INTO '/h1b-analysis-output/most_petitions_denied_places_analysis' using PigStorage(',');


4) Average salary for 'CERTIFIED' H1B cases in USA over the year?
h1bdata = LOAD 'hdfs://localhost:9000/h1b-analysis/h1b_dataset.csv' using PigStorage(',') as (ID:int,CASE_STATUS:chararray,EMPLOYER_NAME:chararray,SOC_NAME:chararray,JOB_TITLE:chararray,FULL_TIME_POSITION:chararray,PREVAILING_WAGE:int,YEAR:int,WORKSITE:chararray,LONGITUDE:chararray,LATITUDE:chararray);
filterdata = FILTER h1bdata by CASE_STATUS == 'CERTIFIED';
groupByYear = GROUP filterdata BY (YEAR);
avgData = FOREACH groupByYear GENERATE group as YEAR, ROUND(AVG($1.PREVAILING_WAGE)*100.00)/100.00 as AVERAGE_SALARY;
STORE avgData INTO '/h1b-analysis-output/average_salary_approval_cases_analysis.csv' using PigStorage(',');


5) Top 10 cities hiring the most
h1bdata = LOAD 'hdfs://localhost:9000/h1b-analysis/h1b_dataset.csv' using PigStorage(',') as (ID:int,CASE_STATUS:chararray,EMPLOYER_NAME:chararray,SOC_NAME:chararray,JOB_TITLE:chararray,FULL_TIME_POSITION:chararray,PREVAILING_WAGE:int,YEAR:int,WORKSITE:chararray,LONGITUDE:chararray,LATITUDE:chararray);
filterdata = FILTER h1bdata by CASE_STATUS == 'CERTIFIED';
groupByYear = GROUP filterdata BY SUBSTRING(WORKSITE,(int)INDEXOF(WORKSITE,'|',0)+2,(int)SIZE(WORKSITE));
countData = FOREACH groupByYear GENERATE group, COUNT(filterdata) as HIRING_COUNT;
orderByHiringCount = ORDER countData BY HIRING_COUNT DESC;
top15 = LIMIT orderByHiringCount 10;
STORE top15 INTO '/h1b-analysis-output/top10_states_hiring_analysis' using PigStorage(',');


6) Top 15 Job Titles
h1bdata = LOAD 'hdfs://localhost:9000/h1b-analysis/h1b_dataset.csv' using PigStorage(',') as (ID:int,CASE_STATUS:chararray,EMPLOYER_NAME:chararray,SOC_NAME:chararray,JOB_TITLE:chararray,FULL_TIME_POSITION:chararray,PREVAILING_WAGE:int,YEAR:int,WORKSITE:chararray,LONGITUDE:chararray,LATITUDE:chararray);
filterdata = FILTER h1bdata BY YEAR == 2015;
groupByYear = GROUP filterdata BY JOB_TITLE;
countData = FOREACH groupByYear GENERATE group, COUNT(filterdata) as JOB_COUNT;
orderByHiringCount = ORDER countData BY JOB_COUNT DESC;
top15 = LIMIT orderByHiringCount 15;
STORE top15 INTO '/h1b-analysis-output/top15_job_title_hiring_analysis' using PigStorage(',');


7) PERCENTAGE and PREDICTION of getting CERTIFIED H1B status
mostpetition = LOAD 'hdfs://localhost:9000/h1b-analysis-output/most_petitions_by_employer_analysis/most_petitions_by_employer.csv' using PigStorage(',') as (EMPLOYER:chararray,YEAR_MP:int,PETITION_COUNT:int);
h1bdata = LOAD 'hdfs://localhost:9000/h1b-analysis/h1b_dataset.csv' using PigStorage(',') as (ID:int,CASE_STATUS:chararray,EMPLOYER_NAME:chararray,SOC_NAME:chararray,JOB_TITLE:chararray,FULL_TIME_POSITION:chararray,PREVAILING_WAGE:int,YEAR:int,WORKSITE:chararray,LONGITUDE:chararray,LATITUDE:chararray);
filterCertifiedData = FILTER h1bdata BY CASE_STATUS == 'CERTIFIED'; 
groupfilterCertifiedData = GROUP filterCertifiedData BY (EMPLOYER_NAME,YEAR);
countFilterData = FOREACH groupfilterCertifiedData GENERATE group.$0, group.$1, COUNT(filterCertifiedData) as APPLICATION_CERTIFIED_COUNT;
joinEmployerdata = JOIN countFilterData BY (EMPLOYER_NAME,YEAR), mostpetition BY (EMPLOYER,YEAR_MP);
percentData = FOREACH joinEmployerdata GENERATE $0 AS COMPANY_NAME, $1 AS YEAR, $2 AS APPLICATIONS_APPROVED, $5 AS TOTAL_PETITIONS_COUNT, 100*(double)$2/(double)$5 AS APPROVED_PERCENTAGE;
orderBy = ORDER percentData BY APPROVED_PERCENTAGE ASC;
STORE orderBy INTO '/h1b-analysis-output/percenatage_h1b_approval_analysis' USING PigStorage(',');


7)

