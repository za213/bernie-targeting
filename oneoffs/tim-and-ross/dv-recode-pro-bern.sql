
DROP TABLE IF EXISTS ernie_tryan.nh_gotc_dvs; b
CREATE TABLE ernie_tryan.nh_gotc_dvs
  DISTSTYLE KEY
  DISTKEY (person_id)
  SORTKEY (person_id)
AS 
(select * 
from
(select 
 person_id::varchar(10)
-- Candidate Support DVs
,case when first_choice = 'Bernie Sanders' then 1 when first_choice is not null or first_choice <> 'Donald Trump' then 0 else NULL end as like_bernie
,case when first_choice = 'Joe Biden' then 1 when first_choice is not null or first_choice <> 'Donald Trump' then 0 else NULL end as like_biden
,case when first_choice = 'Elizabeth Warren' then 1 when first_choice is not null or first_choice <> 'Donald Trump' then 0 else NULL end as like_warren
,case when first_choice = 'Pete Buttigieg' then 1 when first_choice is not null or first_choice <> 'Donald Trump' then 0 else NULL end as like_buttigieg
,case when first_choice IN ('Kamala Harris','Julian Castro','Andrew Yang','Tulsi Gabbard','Cory Booker','Beto ORourke','Michael Bennett','Deval Patrick','Mike Bloomberg','Amy Klobuchar','Tom Steyer','Other','Someone else','Dont Know') then 1 when first_choice is not null or first_choice <> 'Donald Trump' then 0 else NULL end as like_other
-- Support ID DVs
,case when support_int = 1 then 1 when support_int is not null or support_int <> 0 or first_choice <> 'Donald Trump' then 0 else NULL end as pro_bernie_1_id
,case when support_int IN (1,2) then 1 when support_int is not null or support_int <> 0 or first_choice <> 'Donald Trump' then 0 else NULL end as pro_bernie_1_2_id
from bernie_data_commons.third_party_ids where state = 'NH')
full join
(select * from (SELECT person_id::varchar(10) ,
       CASE
           WHEN politicalparty <> 'Republican' AND latestsupportid = 1 THEN 1
           WHEN politicalparty <> 'Republican' AND latestsupportid IN (2,3,4,5) THEN 0
           ELSE NULL END AS pro_bernie_fieldid_1_no_gop ,
       CASE
           WHEN latestsupportid = 1 THEN 1
           WHEN latestsupportid IN (2,3,4,5) THEN 0
           ELSE NULL END AS pro_bernie_fieldid_1 
FROM bernie_nh.dim_voters) where pro_bernie_fieldid_1_no_gop is not null and pro_bernie_fieldid_1 is not null)
using(person_id));
	
	
	
