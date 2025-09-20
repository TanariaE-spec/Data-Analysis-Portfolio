-- Data Cleaning Project


SELECT *
FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or Blank Values
-- 4. Remove Unnecessary Columns 

#create a new table to work on data

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

-- Step 1. Remove Duplicates

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

#check to make sure these are duplicates
SELECT *
FROM layoffs_staging
WHERE company = 'Casper';



#cannot make updates to CTE so creating a new table to remove duplicates from
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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



SELECT *
FROM layoffs_staging2;


INSERT INTO layoffs_staging2 (
    company, location, industry, total_laid_off, percentage_laid_off, 
    `date`, stage, country, funds_raised_millions, row_num
)
SELECT
    company, location, industry,
    CASE														#converting 'NULL' strings
        WHEN total_laid_off = 'NULL' THEN NULL
        ELSE total_laid_off
    END AS total_laid_off,
    CASE
        WHEN percentage_laid_off = 'NULL' THEN NULL
        ELSE percentage_laid_off
    END AS percentage_laid_off, 
	CASE
        WHEN `date` = 'NULL' THEN NULL
        ELSE `date`
    END AS `date`,
    stage, country, 
    CASE
        WHEN funds_raised_millions = 'NULL' THEN NULL
        ELSE funds_raised_millions
    END AS funds_raised_millions,
    ROW_NUMBER() OVER(
        PARTITION BY company, location, industry, total_laid_off, 
        percentage_laid_off, `date`, stage, country, funds_raised_millions
    ) AS row_num
FROM
    layoffs_staging;
    

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;


-- Step 2. Standardizing Data

#removing spaces
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

#condensing data
SELECT DISTINCT industry
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';


SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

#formatting date from 'text' to 'datetime'
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;


UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');
    
SELECT `date`
FROM layoffs_staging2;


ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';


-- Step 3. Nulls and Blanks

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '' 
OR industry = 'NULL';

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = ''
OR industry = 'NULL';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

#fill in the blanks/NULLs with the info we have

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

#checking for any NULL, string 'NULL', and blanks left

SELECT *
FROM layoffs_staging2
WHERE company = 'NULL'
OR location = 'NULL'
OR industry = 'NULL'
OR total_laid_off = 'NULL'
OR percentage_laid_off = 'NULL'
OR stage = 'NULL'
OR country = 'NULL'
OR funds_raised_millions = 'NULL';


SELECT stage
FROM layoffs_staging2
WHERE stage = 'NULL';

UPDATE layoffs_staging2
SET stage = NULLIF(stage, 'NULL')
WHERE stage = 'NULL';


-- Step 4. Remove Unnecessary Columns

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;





