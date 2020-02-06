set query_group to 'importers';
set wlm_query_slot_count to 1;

/* 
First lets look at a common list pull for Utah. Let's say the goal is to create a rank ordered list of 
Democratic primary voters. The below query partitions on the state code and dem primary eligibility status
which is a flag that designates if the voter is eligible based on whether their state is open, closed, or mixed
and their party affiliation. What the below query does is that it orders people by the support guardrail tiers 
and then orders each tier by the field_1_score. The final gotv_tiers_20 column creates a ventile of support that automatically
prioritizes people based on the tier order and support who are Primary eligible. 
*/ 

begin;

-- Only eligible Dem Primary Voters 
CREATE TABLE gotv_universes.ut_gotv_dem_primary_eligible_vanilla
distkey(person_id) 
sortkey(person_id) as
(SELECT *
        ,NTILE(20) OVER (PARTITION BY state_code||dem_primary_eligible_2way ORDER BY gotv_rank ASC) AS gotv_tiers_20
 from
 (select  person_id
         ,state_code
         ,dem_primary_eligible_2way
         ,support_guardrail
         ,round(1.0*count(*) OVER (partition BY state_code||dem_primary_eligible_2way  ORDER BY support_guardrail ASC, field_id_1_score DESC ROWS UNBOUNDED PRECEDING)/pturnout_2016,4) AS rolling_electorate_share
         ,row_number() OVER (PARTITION BY state_code||dem_primary_eligible_2way  ORDER BY support_guardrail ASC, field_id_1_score DESC) as gotv_rank
from bernie_data_commons.base_universe 
where dem_primary_eligible_2way  = '1 - Dem Primary Eligible' 
and state_code = 'UT'
and support_guardrail IN ('0 - Donors, Activists, Supporters','1 - Inside Support Guardrail')));

commit;

/* 
Another option is that since Utah is an open primary it might make sense to restrict to only Democrats even 
though anyone can participate. As a result this query might be a better fit since it limits the GOTV universe
to Primary Eligible people who have a high Dem partisanship score or who are registered as Democrats
*/ 

begin;

-- Only eligible Dem Primary Voters who are likely Democrats
CREATE TABLE gotv_universes.ut_gotv_dem_primary_eligible_likely_dems
distkey(person_id) 
sortkey(person_id) as
(SELECT *
        ,NTILE(20) OVER (PARTITION BY state_code||dem_primary_eligible_2way ORDER BY gotv_rank ASC) AS gotv_tiers_20
 from
 (select  person_id
         ,state_code
         ,dem_primary_eligible_2way
         ,support_guardrail
         ,round(1.0*count(*) OVER (partition BY state_code||dem_primary_eligible_2way  ORDER BY support_guardrail ASC, field_id_1_score DESC ROWS UNBOUNDED PRECEDING)/pturnout_2016,4) AS rolling_electorate_share
         ,row_number() OVER (PARTITION BY state_code||dem_primary_eligible_2way  ORDER BY support_guardrail ASC, field_id_1_score DESC) as gotv_rank
from bernie_data_commons.base_universe 
where dem_primary_eligible_2way  = '1 - Dem Primary Eligible' 
and state_code = 'UT' 
and (civis_2020_partisanship >= .66 or party_8way = '1 - Democratic')
and support_guardrail IN ('0 - Donors, Activists, Supporters','1 - Inside Support Guardrail') ));

commit;

/* 
For more selective targeting we can also add in the flag for early vote history. In 2020 a new rule gives UT all Democrats
are sent a vote by mail ballot. To prioritize people who have a history of voting by mail we can order by the early_vote_history_3way 
column in addition to the usual support_guardrail and field_id_1_score.
*/ 

begin;

-- Only eligible Dem Primary Voters who are Democrats and prioritizing early voters first
CREATE TABLE gotv_universes.ut_gotv_dem_primary_eligible_early_voters
distkey(person_id) 
sortkey(person_id) as
(SELECT *
        ,NTILE(20) OVER (PARTITION BY state_code||dem_primary_eligible_2way ORDER BY gotv_rank ASC) AS gotv_tiers_20
 from
 (select  person_id
         ,state_code
         ,dem_primary_eligible_2way
         ,early_vote_history_3way
         ,support_guardrail
         ,round(1.0*count(*) OVER (partition BY state_code||dem_primary_eligible_2way  ORDER BY support_guardrail ASC, early_vote_history_3way ASC, field_id_1_score DESC ROWS UNBOUNDED PRECEDING)/pturnout_2016,4) AS rolling_electorate_share
         ,row_number() OVER (PARTITION BY state_code||dem_primary_eligible_2way  ORDER BY support_guardrail ASC, early_vote_history_3way ASC, field_id_1_score DESC) as gotv_rank
from bernie_data_commons.base_universe 
where dem_primary_eligible_2way  = '1 - Dem Primary Eligible' 
and state_code = 'UT' 
and (civis_2020_partisanship >= .66 or party_8way = '1 - Democratic')
and support_guardrail IN ('0 - Donors, Activists, Supporters','1 - Inside Support Guardrail') ));

commit;

/* 
Sometimes when we are facing more limited volunteer capacity it may make sense to create lists of people who are highly active voters. One way to
do this is with the vote_ready_5way flag which limits to people who have registered since 2018, voted in the 2018 midterm, or who have ever voted 
in a Democratic primary.
*/ 

begin;

-- Only eligible Dem Primary Voters who are Democrats and limiting to more active voters
CREATE TABLE gotv_universes.ut_gotv_dem_primary_eligible_vote_ready
distkey(person_id) 
sortkey(person_id) as
(SELECT *
        ,NTILE(20) OVER (PARTITION BY state_code||dem_primary_eligible_2way ORDER BY gotv_rank ASC) AS gotv_tiers_20
 from
 (select  person_id
         ,state_code
         ,dem_primary_eligible_2way
         ,vote_ready_5way
         ,support_guardrail
         ,round(1.0*count(*) OVER (partition BY state_code||dem_primary_eligible_2way  ORDER BY support_guardrail ASC, early_vote_history_3way ASC, field_id_1_score DESC ROWS UNBOUNDED PRECEDING)/pturnout_2016,4) AS rolling_electorate_share
         ,row_number() OVER (PARTITION BY state_code||dem_primary_eligible_2way  ORDER BY support_guardrail ASC, early_vote_history_3way ASC, field_id_1_score DESC) as gotv_rank
from bernie_data_commons.base_universe 
where dem_primary_eligible_2way  = '1 - Dem Primary Eligible' 
and state_code = 'UT' 
and (civis_2020_partisanship >= .66 or party_8way = '1 - Democratic')
and support_guardrail IN ('0 - Donors, Activists, Supporters','1 - Inside Support Guardrail') 
and vote_ready_5way =  '1 - Vote-ready'));

commit;

/* 
Lastly sometimes it may make sense to add in custom targeting criteria. This process is simple and all it involves is writing a simple case when
using the standard numbering order logic. In this case we add millennials to the very top bucket so that we can ensure they are targeted first.
*/ 

begin;

-- Custom tiers that insert millenials at top of rank order with activists, donors, and verified supporters
CREATE TABLE gotv_universes.ut_gotv_dem_primary_eligible_millennials
distkey(person_id) 
sortkey(person_id) as
(SELECT *
        ,NTILE(20) OVER (PARTITION BY state_code||dem_primary_eligible_2way ORDER BY gotv_rank ASC) AS gotv_tiers_20
 from
 (select  person_id
         ,state_code
         ,dem_primary_eligible_2way
         ,support_guardrail
         ,case 
        WHEN electorate_2way = '2 - Non-target' then '3 - Non-target' 
        WHEN activist_flag = 1
          OR activist_household_flag = 1
          OR donor_1plus_flag = 1 
          OR donor_1plus_household_flag = 1
          OR ccj_id_1 = 1
          OR thirdp_support_1_id = 1
          OR thirdp_first_choice_bernie = 1
          OR age_5way = '1 - 18-34' then '0 - Donors, Activists, Supporters, Millennials'
        when field_id_1_score_100 >= 70 
          or field_id_composite_score_100 >= 70 
          or current_support_raw_100 >= 70
          or sanders_strong_support_score_100 >= 70 
          or student_flag = 1 then '1 - Inside Support Guardrail'
        else '2 - Outside Support Guardrail' end as custom_tiers
         ,round(1.0*count(*) OVER (partition BY state_code||dem_primary_eligible_2way  ORDER BY custom_tiers ASC, field_id_1_score DESC ROWS UNBOUNDED PRECEDING)/pturnout_2016,4) AS rolling_electorate_share
         ,row_number() OVER (PARTITION BY state_code||dem_primary_eligible_2way  ORDER BY custom_tiers ASC, field_id_1_score DESC) as gotv_rank
from bernie_data_commons.base_universe 
where dem_primary_eligible_2way  = '1 - Dem Primary Eligible' 
and state_code = 'UT' 
and (civis_2020_partisanship >= .66 or party_8way = '1 - Democratic')
and support_guardrail IN ('0 - Donors, Activists, Supporters','1 - Inside Support Guardrail')));

commit;
