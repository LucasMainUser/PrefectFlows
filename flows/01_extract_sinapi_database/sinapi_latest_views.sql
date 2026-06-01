--  CREATED AT:     01/06/2026
--  LAST UPDATED:   01/06/2026

--  EXTRACT THE LASTEST INSERTED VALUES FOR SINAPI TABLES (COUNT=6)
--  ACTIVE SCHEMA: 'sinapi_extraction'


CREATE OR REPLACE VIEW sinapi_extraction.compositions_ccd_latest AS
    SELECT source.* FROM sinapi_extraction.compositions_ccd AS source
    JOIN(
        SELECT "YEAR_MONTH", MAX("CREATED_AT") AS "LATEST_INSERTION"
        FROM sinapi_extraction.compositions_ccd
        GROUP BY "YEAR_MONTH"
    ) AS groups
    ON source."YEAR_MONTH" = groups."YEAR_MONTH"
    AND source."CREATED_AT" = groups."LATEST_INSERTION";



CREATE OR REPLACE VIEW sinapi_extraction.compositions_csd_latest AS
    SELECT source.* FROM sinapi_extraction.compositions_csd AS source
    JOIN(
        SELECT "YEAR_MONTH", MAX("CREATED_AT") AS "LATEST_INSERTION"
        FROM sinapi_extraction.compositions_csd 
        GROUP BY "YEAR_MONTH"
    ) AS groups
    ON source."YEAR_MONTH" = groups."YEAR_MONTH"
    AND source."CREATED_AT" = groups."LATEST_INSERTION";



CREATE OR REPLACE VIEW sinapi_extraction.compositions_cse_latest AS
    SELECT source.* FROM sinapi_extraction.compositions_cse AS source
    JOIN (
        SELECT "YEAR_MONTH", MAX("CREATED_AT") AS "LATEST_INSERTION"
        FROM sinapi_extraction.compositions_cse 
        GROUP BY "YEAR_MONTH"
    ) AS groups
    ON source."YEAR_MONTH" = groups."YEAR_MONTH"
    AND source."CREATED_AT" = groups."LATEST_INSERTION";



CREATE OR REPLACE VIEW sinapi_extraction.materials_services_icd_latest AS
    SELECT source.* FROM sinapi_extraction.materials_services_icd AS source
    JOIN (
        SELECT "YEAR_MONTH", MAX("CREATED_AT") AS "LATEST_INSERTION"
        FROM sinapi_extraction.materials_services_icd 
        GROUP BY "YEAR_MONTH"
    ) AS groups
    ON source."YEAR_MONTH" = groups."YEAR_MONTH"
    AND source."CREATED_AT" = groups."LATEST_INSERTION";




CREATE OR REPLACE VIEW sinapi_extraction.materials_services_isd_latest AS 
    SELECT source.* FROM sinapi_extraction.materials_services_isd AS source
    JOIN (
        SELECT "YEAR_MONTH", MAX("CREATED_AT") AS "LATEST_INSERTION"
        FROM sinapi_extraction.materials_services_isd 
        GROUP BY "YEAR_MONTH"
    ) AS groups
    ON source."YEAR_MONTH" = groups."YEAR_MONTH"
    AND source."CREATED_AT" = groups."LATEST_INSERTION";



CREATE OR REPLACE VIEW sinapi_extraction.materials_services_ise_latest AS 
    SELECT source.* FROM sinapi_extraction.materials_services_ise AS source
    JOIN (
        SELECT "YEAR_MONTH", MAX("CREATED_AT") AS "LATEST_INSERTION"
        FROM sinapi_extraction.materials_services_ise 
        GROUP BY "YEAR_MONTH"
    ) AS groups
    ON source."YEAR_MONTH" = groups."YEAR_MONTH"
    AND source."CREATED_AT" = groups."LATEST_INSERTION";
