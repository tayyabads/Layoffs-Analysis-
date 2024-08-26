  #Data Cleaning 
  select *from layoffs;
  #Steps
  #1: Removing Duplicates
  #2: Standardize the data
  #3: Null values or blank alues
  #4: Remove any columns 
  

-- Create a staging table with the same structure as the layoffs table
CREATE TABLE layoffs_staging LIKE layoffs;

-- Copy data from the raw existing table into the staging table
INSERT INTO layoffs_staging
SELECT * FROM layoffs;

-- Select all data from the staging table
SELECT * FROM layoffs_staging;

-- Identify duplicates
WITH duplicate_cte AS 
(
    SELECT 
        company,
        location,
        industry,
        total_laid_off,
        percentage_laid_off,
        date, 
        stage, 
        country, 
        funds_raised_millions,
        ROW_NUMBER() OVER (PARTITION BY company, industry, total_laid_off, date order by company) AS row_num
    FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte 
WHERE row_num > 1;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

##inserting values
insert into layoffs_staging2 SELECT 
        company,
        location,
        industry,
        total_laid_off,
        percentage_laid_off,
        date, 
        stage, 
       country, 
        funds_raised_millions,
        ROW_NUMBER() OVER (PARTITION BY company, industry, total_laid_off, date order by company) AS row_num
    FROM layoffs_staging;
DELETE FROM layoffs_staging2 WHERE row_num % 2 <> 0;
select * from layoffs_staging2 where row_num>1;
DELETE FROM layoffs_staging2 WHERE row_num > 1;
select *from layoffs_staging2;

#standardizing data
select company, trim(company) from layoffs_staging2; 

update layoffs_staging2
set cpmpany= trim(company);

select distinct industry  from layoffs_staging2; 
#changing the crypto and crypto currency into 1 lable 
select * from layoffs_staging2 where industry like 'Crypto%';
update layoffs_staging2 set industry = 'Crypto' where industry like 'Crypto%';

select distinct location from layoffs_staging2 order by 1; 
select distinct country from layoffs_staging2 order by 1; 
select * from layoffs_staging2 where country like 'United States%' order by 1;
select distinct country, trim(trailing '.' from country) from layoffs_staging2 order by 1;

update layoffs_staging2 set country =trim(trailing '.' from country) 
where country like 'United States%';
select *from layoffs_staging2;
select `date`
from layoffs_staging2;

update layoffs_staging2 set `date`=str_to_date(`date`,'%m/%d/%Y');

#since date column is still int ext formate so we need to changeit into actual date column 
alter table layoffs_staging2 modify column `date` date;

select *from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null;
select * from layoffs_staging2 where industry is null or industry='';
#to populate the data for company airbnb and likewise 
select *from layoffs_staging2 where company ='Airbnb';
update layoffs_staging2 set industry = null where industry='';
select *from layoffs_staging2 t1 join layoffs_staging2 t2 on t1.company= t2.company
where (t1.industry is null or t1.industry='')
and t2.industry is not null;

update layoffs_staging2 t1 join layoffs_staging2 t2 on t1.company=t2.company
set t1.industry= t2.industry where t1.industry is null 
and t2.industry is not null;

select *from layoffs_staging2 where company like 'Bally%';

select *from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null;
delete from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null;
select *from layoffs_staging2;
 alter table layoffs_staging2 drop column row_num;