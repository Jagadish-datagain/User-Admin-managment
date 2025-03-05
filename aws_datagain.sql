use vendor_portal;

SET SQL_SAFE_UPDATES = 0;

show tables;

select count(*) from Managed;

select * from Managed limit 10;
select count(*) from admin;
select count(*) from sample_admin;



select count(*) from Managed_Services;
drop table Managed_Services;
rename table Managed_Services to managed_service;
desc managed_service;




alter table managed_service modify `Email Address` varchar(255);
alter table managed_service modify `ocr` varchar(25);
alter table managed_service modify `payer` varchar(255);
alter table managed_service modify `bill ref code` varchar(200);
alter table managed_service modify `matched payee id` varchar(200);
alter table managed_service rename column `invoice_` to invoice;
alter table managed_service modify `document type` varchar(100);
# alter table managed_service rename column `PO_` TO PO;
alter table managed_service modify `track id` varchar(200);
alter table managed_service modify review varchar(10);
alter table managed_service modify `Bill lines` int;
alter table managed_service modify `payee name` text;
alter table managed_service modify invoice bigint;
alter table managed_service modify terms text;
alter table managed_service modify po text;
alter table managed_service modify `tax amt` decimal(10,2);
alter table managed_service modify `total amt` decimal(10,2);
alter table managed_service modify `currency` varchar(10);
alter table managed_service modify `foreign language` varchar(15);
alter table managed_service modify `Quality analyst` varchar(50);
alter table managed_service modify `invoice pages` int;
alter table managed_service modify `Multiple payees` varchar(15);
alter table managed_service modify `pst to ist` time;
alter table managed_service modify team text;
alter table managed_service modify Agent varchar(20);
alter table managed_service modify `Unique id` text(200);
# alter table managed_service add constraint unique(Unique_id (255) );
alter table managed_service modify priority varchar(50);
alter table managed_service modify month varchar(25);
alter table managed_service modify hour int;
alter table managed_service modify EMEA varchar(25);
alter table managed_service modify timestamp date;
alter table managed_service modify `us date` date;
alter table managed_service modify `ind date` date;

desc managed_service;
UPDATE managed_service 
SET `us date` = STR_TO_DATE(`us date`, '%d-%m-%Y')
WHERE timestamp IS NOT NULL;

UPDATE managed_service 
SET `ind date` = STR_TO_DATE(`ind date`, '%d-%m-%Y')
WHERE timestamp IS NOT NULL;

SELECT `Unique id`, COUNT(*) 
FROM managed_service 
GROUP BY `Unique id` 
HAVING COUNT(*) > 1;  # duplicate records in the table 


desc admin_data;