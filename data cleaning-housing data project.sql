USE housingdata;

-- CREATE A TABLE
CREATE TABLE housing (
UniqueID 	BIGINT,
ParcelID	DOUBLE,
LandUse	    VARCHAR(255),
PropertyAddress	VARCHAR(255),
SaleDate	DATE,
SalePrice	BIGINT,
LegalReference BIGINT,
SoldAsVacant  BOOLEAN,
OwnerName	VARCHAR(255),
OwnerAddress  VARCHAR(255),	
Acreage	   DOUBLE,
TaxDistrict	 VARCHAR(255),
LandValue	BIGINT,
BuildingValue	BIGINT,
TotalValue	BIGINT,
YearBuilt	BIGINT,
Bedrooms	INT,
FullBath	INT,
HalfBath    INT
);

-- LOADING DATA INTO THE TABLE
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Housing Data for Data Cleaning2.csv'
INTO TABLE housing
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@UniqueID, @ParcelID, @LandUse, @PropertyAddress, @SaleDate, @SalePrice, @LegalReference, 
 @SoldAsVacant, @OwnerName, @OwnerAddress, @Acreage, @TaxDistrict, @LandValue, 
 @BuildingValue, @TotalValue, @YearBuilt, @Bedrooms, @FullBath, @HalfBath)
SET 
 UniqueID = NULLIF(@UniqueID, ''),
 ParcelID = NULLIF(@ParcelID, ''),
 LandUse = NULLIF(@LandUse, ''),
 PropertyAddress = NULLIF(@PropertyAddress, ''),
 SaleDate = IF(@SaleDate = '', NULL, STR_TO_DATE(@SaleDate, '%Y-%m-%d')),
 SalePrice = NULLIF(@SalePrice, ''),
 LegalReference = NULLIF(@LegalReference, ''),
 SoldAsVacant = NULLIF(@SoldAsVacant, ''),
 OwnerName = NULLIF(@OwnerName, ''),
 OwnerAddress = NULLIF(@OwnerAddress, ''),
 Acreage = NULLIF(@Acreage, ''),
 TaxDistrict = NULLIF(@TaxDistrict, ''),
 LandValue = NULLIF(@LandValue, ''),
 BuildingValue = NULLIF(@BuildingValue, ''),
 TotalValue = NULLIF(@TotalValue, ''),
 YearBuilt = NULLIF(@YearBuilt, ''),
 Bedrooms = NULLIF(@Bedrooms, ''),
 FullBath = NULLIF(@FullBath, ''),
 HalfBath = IF(TRIM(@HalfBath) RLIKE '^[0-9]+$', TRIM(@HalfBath), NULL);

ALTER TABLE housing 
MODIFY COLUMN ParcelID VARCHAR(255);
ALTER TABLE housing 
MODIFY COLUMN LegalReference VARCHAR(255);
ALTER TABLE housing 
MODIFY COLUMN SoldAsVacant VARCHAR(255);

TRUNCATE TABLE housing;

SELECT * FROM housing;

-- Populate property address data where it is null

SELECT a.ParcelID, a.PropertyAddress, b.ParcelID, b.PropertyAddress  -- IFNULL(a.PropertyAddress,b.PropertyAddress) --IFNULL(what we want to change, replacement)
FROM housing as a
JOIN housing as b
    ON a.ParcelID = b.ParcelID
    AND a.UniqueID <> b.UniqueID
WHERE a.PropertyAddress IS NULL;

UPDATE housing AS a
JOIN housing AS b
    ON a.ParcelID = b.ParcelID
    AND a.UniqueID <> b.UniqueID
SET a.PropertyAddress = IFNULL(a.PropertyAddress, b.PropertyAddress)
WHERE a.PropertyAddress IS NULL;

-- Breaking out address into individual columns(Address, City, State)

SELECT *
FROM housing;

SELECT
SUBSTRING(PropertyAddress, 1, LOCATE(',', PropertyAddress) -1) AS Address,
SUBSTRING(PropertyAddress, LOCATE(',', PropertyAddress) +1, LENGTH(PropertyAddress)) AS City
FROM housing;

ALTER TABLE housing 
ADD COLUMN PropertyAddressNew VARCHAR(255);

ALTER TABLE housing 
ADD COLUMN PropertyCity VARCHAR(255);

UPDATE housing
SET PropertyAddressNew = SUBSTRING(PropertyAddress, 1, LOCATE(',', PropertyAddress) -1);

UPDATE housing
SET PropertyCity = SUBSTRING(PropertyAddress, LOCATE(',', PropertyAddress) +1, LENGTH(PropertyAddress));

-- seperating the owner address column into address, city, state

SELECT 
SUBSTRING_INDEX(OwnerAddress, ',', 1) AS Address,
TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(OwnerAddress, ',', 2), ',', -1)) AS City,
TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(OwnerAddress, ',', 3), ',', -1)) AS State
FROM housing;

ALTER TABLE housing 
ADD COLUMN OwnerAddressNew VARCHAR(255);

ALTER TABLE housing 
ADD COLUMN OwnerCity VARCHAR(255);

ALTER TABLE housing 
ADD COLUMN OwnerState VARCHAR(255);

UPDATE housing
SET OwnerAddressNew = SUBSTRING_INDEX(OwnerAddress, ',', 1);

UPDATE housing
SET OwnerCity = TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(OwnerAddress, ',', 2), ',', -1));

UPDATE housing
SET OwnerState = TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(OwnerAddress, ',', 3), ',', -1));

-- Change Y and N to Yes or No in SoldAsVacant column

SELECT DISTINCT(SoldAsVacant), COUNT(SoldAsVacant)
FROM housing
GROUP BY SoldAsVacant;

SELECT SoldAsVacant,
CASE WHEN SoldAsVacant = 'Y' THEN 'Yes'
     WHEN SoldAsVacant = 'N' THEN 'No'
     ELSE SoldAsVacant
END
FROM housing;

UPDATE housing
SET SoldAsVacant = 
CASE WHEN SoldAsVacant = 'Y' THEN 'Yes'
     WHEN SoldAsVacant = 'N' THEN 'No'
     ELSE SoldAsVacant
END;

-- Remove Duplicates

WITH ROW_NUMCTE AS(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY ParcelID, PropertyAddress, SaleDate, SalePrice, LegalReference ORDER BY UniqueID) AS row_num
FROM housing
)
DELETE h
FROM housing h
JOIN ROW_NUM_CTE cte
  ON h.UniqueID = cte.UniqueID
WHERE cte.row_num > 1;

SELECT *
FROM housing h
JOIN (
    SELECT UniqueID
    FROM (
        SELECT UniqueID,
               ROW_NUMBER() OVER (
                   PARTITION BY ParcelID, PropertyAddress, SaleDate, SalePrice, LegalReference
                   ORDER BY UniqueID
               ) AS row_num
        FROM housing
    ) AS numbered
    WHERE row_num > 1
) AS to_delete
ON h.UniqueID = to_delete.UniqueID;
DELETE h
FROM housing h
JOIN (
    SELECT UniqueID
    FROM (
        SELECT UniqueID,
               ROW_NUMBER() OVER (
                   PARTITION BY ParcelID, PropertyAddress, SaleDate, SalePrice, LegalReference
                   ORDER BY UniqueID
               ) AS row_num
        FROM housing
    ) AS numbered
    WHERE row_num > 1
) AS to_delete
ON h.UniqueID = to_delete.UniqueID;

-- Delete unused columns

ALTER TABLE housing
DROP COLUMN PropertyAddress,
DROP COLUMN OwnerAddress,
DROP COLUMN TaxDistrict;

select *
from housing;

