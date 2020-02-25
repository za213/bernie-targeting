
drop table if exists bernie_nmarchio2.xwalk_qc;

create table bernie_nmarchio2.xwalk_qc as (
select * from 
(
(select 
 'actionkit_id' as id_type
,'inner join' as join_type
,count(distinct case when prod.lalvoterid = dev.lalvoterid then prod.lalvoterid end) as matched_lalvoterid
,count(distinct case when prod.voters_statevoterid = dev.voters_statevoterid then prod.voters_statevoterid end) as matched_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 = dev.sos_join_key_l2 then prod.sos_join_key_l2 end) as matched_sos_join_key_l2
,count(distinct case when prod.l2_precinct = dev.l2_precinct then prod.l2_precinct end) as matched_l2_precinct
,count(distinct case when prod.person_id = dev.person_id then prod.person_id end) as matched_person_id
,count(distinct case when prod.voterbase_id = dev.voterbase_id then prod.voterbase_id end) as matched_voterbase_id
,count(distinct case when prod.dnc_precinct_id = dev.dnc_precinct_id then prod.dnc_precinct_id end) as matched_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id = dev.dnc_sos_id then prod.dnc_sos_id end) as matched_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc = dev.sos_join_key_dnc then prod.sos_join_key_dnc end) as matched_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id = dev.dnc_county_voter_id then prod.dnc_county_voter_id end) as matched_dnc_county_voter_id
,count(distinct case when prod.county_fips = dev.county_fips then prod.county_fips end) as matched_county_fips
,count(distinct case when prod.pdiid = dev.pdiid then prod.pdiid end) as matched_pdiid
,count(distinct case when prod.pdi_sos_id = dev.pdi_sos_id then prod.pdi_sos_id end) as matched_pdi_sos_id
,count(distinct case when prod.pdi_precinct = dev.pdi_precinct then prod.pdi_precinct end) as matched_pdi_precinct
,count(distinct case when prod.actionkit_id = dev.actionkit_id then prod.actionkit_id end) as matched_actionkit_id
,count(distinct case when prod.ak_updated_at = dev.ak_updated_at then prod.ak_updated_at end) as matched_ak_updated_at
,count(distinct case when prod.bern_id = dev.bern_id then prod.bern_id end) as matched_bern_id
,count(distinct case when prod.bern_canvasser_id = dev.bern_canvasser_id then prod.bern_canvasser_id end) as matched_bern_canvasser_id
,count(distinct case when prod.myc_van_id = dev.myc_van_id then prod.myc_van_id end) as matched_myc_van_id
,count(distinct case when prod.st_myc_van_id = dev.st_myc_van_id then prod.st_myc_van_id end) as matched_st_myc_van_id
,count(distinct case when prod.myc_dupe = dev.myc_dupe then prod.myc_dupe end) as matched_myc_dupe
,count(distinct case when prod.myc_voter_type_id = dev.myc_voter_type_id then prod.myc_voter_type_id end) as matched_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked = dev.myc_myv_linked then prod.myc_myv_linked end) as matched_myc_myv_linked
,count(distinct case when prod.myv_van_id = dev.myv_van_id then prod.myv_van_id end) as matched_myv_van_id
,count(distinct case when prod.st_myv_van_id = dev.st_myv_van_id then prod.st_myv_van_id end) as matched_st_myv_van_id
,count(distinct case when prod.email = dev.email then prod.email end) as matched_email
,count(distinct case when prod.aux_myc_email = dev.aux_myc_email then prod.aux_myc_email end) as matched_aux_myc_email
,count(distinct case when prod.student_hash = dev.student_hash then prod.student_hash end) as matched_student_hash
,count(distinct case when prod.student_rn = dev.student_rn then prod.student_rn end) as matched_student_rn
,count(distinct case when prod.state_code = dev.state_code then prod.state_code end) as matched_state_code

,count(distinct case when prod.lalvoterid is not null then prod.lalvoterid end) as prod_distinct_lalvoterid
,count(distinct case when prod.voters_statevoterid is not null then prod.voters_statevoterid end) as prod_distinct_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 is not null then prod.sos_join_key_l2 end) as prod_distinct_sos_join_key_l2
,count(distinct case when prod.l2_precinct is not null then prod.l2_precinct end) as prod_distinct_l2_precinct
,count(distinct case when prod.person_id is not null then prod.person_id end) as prod_distinct_person_id
,count(distinct case when prod.voterbase_id is not null then prod.voterbase_id end) as prod_distinct_voterbase_id
,count(distinct case when prod.dnc_precinct_id is not null then prod.dnc_precinct_id end) as prod_distinct_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id is not null then prod.dnc_sos_id end) as prod_distinct_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc is not null then prod.sos_join_key_dnc end) as prod_distinct_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id is not null then prod.dnc_county_voter_id end) as prod_distinct_dnc_county_voter_id
,count(distinct case when prod.county_fips is not null then prod.county_fips end) as prod_distinct_county_fips
,count(distinct case when prod.pdiid is not null then prod.pdiid end) as prod_distinct_pdiid
,count(distinct case when prod.pdi_sos_id is not null then prod.pdi_sos_id end) as prod_distinct_pdi_sos_id
,count(distinct case when prod.pdi_precinct is not null then prod.pdi_precinct end) as prod_distinct_pdi_precinct
,count(distinct case when prod.actionkit_id is not null then prod.actionkit_id end) as prod_distinct_actionkit_id
,count(distinct case when prod.ak_updated_at is not null then prod.ak_updated_at end) as prod_distinct_ak_updated_at
,count(distinct case when prod.bern_id is not null then prod.bern_id end) as prod_distinct_bern_id
,count(distinct case when prod.bern_canvasser_id is not null then prod.bern_canvasser_id end) as prod_distinct_bern_canvasser_id
,count(distinct case when prod.myc_van_id is not null then prod.myc_van_id end) as prod_distinct_myc_van_id
,count(distinct case when prod.st_myc_van_id is not null then prod.st_myc_van_id end) as prod_distinct_st_myc_van_id
,count(distinct case when prod.myc_dupe is not null then prod.myc_dupe end) as prod_distinct_myc_dupe
,count(distinct case when prod.myc_voter_type_id is not null then prod.myc_voter_type_id end) as prod_distinct_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked is not null then prod.myc_myv_linked end) as prod_distinct_myc_myv_linked
,count(distinct case when prod.myv_van_id is not null then prod.myv_van_id end) as prod_distinct_myv_van_id
,count(distinct case when prod.st_myv_van_id is not null then prod.st_myv_van_id end) as prod_distinct_st_myv_van_id
,count(distinct case when prod.email is not null then prod.email end) as prod_distinct_email
,count(distinct case when prod.aux_myc_email is not null then prod.aux_myc_email end) as prod_distinct_aux_myc_email
,count(distinct case when prod.student_hash is not null then prod.student_hash end) as prod_distinct_student_hash
,count(distinct case when prod.student_rn is not null then prod.student_rn end) as prod_distinct_student_rn
,count(distinct case when prod.state_code is not null then prod.state_code end) as prod_distinct_state_code

,count(distinct case when dev.lalvoterid is not null then dev.lalvoterid end) as dev_distinct_lalvoterid
,count(distinct case when dev.voters_statevoterid is not null then dev.voters_statevoterid end) as dev_distinct_voters_statevoterid
,count(distinct case when dev.sos_join_key_l2 is not null then dev.sos_join_key_l2 end) as dev_distinct_sos_join_key_l2
,count(distinct case when dev.l2_precinct is not null then dev.l2_precinct end) as dev_distinct_l2_precinct
,count(distinct case when dev.person_id is not null then dev.person_id end) as dev_distinct_person_id
,count(distinct case when dev.voterbase_id is not null then dev.voterbase_id end) as dev_distinct_voterbase_id
,count(distinct case when dev.dnc_precinct_id is not null then dev.dnc_precinct_id end) as dev_distinct_dnc_precinct_id
,count(distinct case when dev.dnc_sos_id is not null then dev.dnc_sos_id end) as dev_distinct_dnc_sos_id
,count(distinct case when dev.sos_join_key_dnc is not null then dev.sos_join_key_dnc end) as dev_distinct_sos_join_key_dnc
,count(distinct case when dev.dnc_county_voter_id is not null then dev.dnc_county_voter_id end) as dev_distinct_dnc_county_voter_id
,count(distinct case when dev.county_fips is not null then dev.county_fips end) as dev_distinct_county_fips
,count(distinct case when dev.pdiid is not null then dev.pdiid end) as dev_distinct_pdiid
,count(distinct case when dev.pdi_sos_id is not null then dev.pdi_sos_id end) as dev_distinct_pdi_sos_id
,count(distinct case when dev.pdi_precinct is not null then dev.pdi_precinct end) as dev_distinct_pdi_precinct
,count(distinct case when dev.actionkit_id is not null then dev.actionkit_id end) as dev_distinct_actionkit_id
,count(distinct case when dev.ak_updated_at is not null then dev.ak_updated_at end) as dev_distinct_ak_updated_at
,count(distinct case when dev.bern_id is not null then dev.bern_id end) as dev_distinct_bern_id
,count(distinct case when dev.bern_canvasser_id is not null then dev.bern_canvasser_id end) as dev_distinct_bern_canvasser_id
,count(distinct case when dev.myc_van_id is not null then dev.myc_van_id end) as dev_distinct_myc_van_id
,count(distinct case when dev.st_myc_van_id is not null then dev.st_myc_van_id end) as dev_distinct_st_myc_van_id
,count(distinct case when dev.myc_dupe is not null then dev.myc_dupe end) as dev_distinct_myc_dupe
,count(distinct case when dev.myc_voter_type_id is not null then dev.myc_voter_type_id end) as dev_distinct_myc_voter_type_id
,count(distinct case when dev.myc_myv_linked is not null then dev.myc_myv_linked end) as dev_distinct_myc_myv_linked
,count(distinct case when dev.myv_van_id is not null then dev.myv_van_id end) as dev_distinct_myv_van_id
,count(distinct case when dev.st_myv_van_id is not null then dev.st_myv_van_id end) as dev_distinct_st_myv_van_id
,count(distinct case when dev.email is not null then dev.email end) as dev_distinct_email
,count(distinct case when dev.aux_myc_email is not null then dev.aux_myc_email end) as dev_distinct_aux_myc_email
,count(distinct case when dev.student_hash is not null then dev.student_hash end) as dev_distinct_student_hash
,count(distinct case when dev.student_rn is not null then dev.student_rn end) as dev_distinct_student_rn
,count(distinct case when dev.state_code is not null then dev.state_code end) as dev_distinct_state_code

,count(*)
 from 
bernie_data_commons.master_xwalk_ak prod
inner join 
bernie_nmarchio2.master_xwalk_ak_20200224 dev
using(actionkit_id)
group by 1,2)
union all
(select 
 'person_id' as id_type
,'inner join' as join_type
,count(distinct case when prod.lalvoterid = dev.lalvoterid then prod.lalvoterid end) as matched_lalvoterid
,count(distinct case when prod.voters_statevoterid = dev.voters_statevoterid then prod.voters_statevoterid end) as matched_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 = dev.sos_join_key_l2 then prod.sos_join_key_l2 end) as matched_sos_join_key_l2
,count(distinct case when prod.l2_precinct = dev.l2_precinct then prod.l2_precinct end) as matched_l2_precinct
,count(distinct case when prod.person_id = dev.person_id then prod.person_id end) as matched_person_id
,count(distinct case when prod.voterbase_id = dev.voterbase_id then prod.voterbase_id end) as matched_voterbase_id
,count(distinct case when prod.dnc_precinct_id = dev.dnc_precinct_id then prod.dnc_precinct_id end) as matched_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id = dev.dnc_sos_id then prod.dnc_sos_id end) as matched_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc = dev.sos_join_key_dnc then prod.sos_join_key_dnc end) as matched_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id = dev.dnc_county_voter_id then prod.dnc_county_voter_id end) as matched_dnc_county_voter_id
,count(distinct case when prod.county_fips = dev.county_fips then prod.county_fips end) as matched_county_fips
,count(distinct case when prod.pdiid = dev.pdiid then prod.pdiid end) as matched_pdiid
,count(distinct case when prod.pdi_sos_id = dev.pdi_sos_id then prod.pdi_sos_id end) as matched_pdi_sos_id
,count(distinct case when prod.pdi_precinct = dev.pdi_precinct then prod.pdi_precinct end) as matched_pdi_precinct
,count(distinct case when prod.actionkit_id = dev.actionkit_id then prod.actionkit_id end) as matched_actionkit_id
,count(distinct case when prod.ak_updated_at = dev.ak_updated_at then prod.ak_updated_at end) as matched_ak_updated_at
,count(distinct case when prod.bern_id = dev.bern_id then prod.bern_id end) as matched_bern_id
,count(distinct case when prod.bern_canvasser_id = dev.bern_canvasser_id then prod.bern_canvasser_id end) as matched_bern_canvasser_id
,count(distinct case when prod.myc_van_id = dev.myc_van_id then prod.myc_van_id end) as matched_myc_van_id
,count(distinct case when prod.st_myc_van_id = dev.st_myc_van_id then prod.st_myc_van_id end) as matched_st_myc_van_id
,count(distinct case when prod.myc_dupe = dev.myc_dupe then prod.myc_dupe end) as matched_myc_dupe
,count(distinct case when prod.myc_voter_type_id = dev.myc_voter_type_id then prod.myc_voter_type_id end) as matched_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked = dev.myc_myv_linked then prod.myc_myv_linked end) as matched_myc_myv_linked
,count(distinct case when prod.myv_van_id = dev.myv_van_id then prod.myv_van_id end) as matched_myv_van_id
,count(distinct case when prod.st_myv_van_id = dev.st_myv_van_id then prod.st_myv_van_id end) as matched_st_myv_van_id
,count(distinct case when prod.email = dev.email then prod.email end) as matched_email
,count(distinct case when prod.aux_myc_email = dev.aux_myc_email then prod.aux_myc_email end) as matched_aux_myc_email
,count(distinct case when prod.student_hash = dev.student_hash then prod.student_hash end) as matched_student_hash
,count(distinct case when prod.student_rn = dev.student_rn then prod.student_rn end) as matched_student_rn
,count(distinct case when prod.state_code = dev.state_code then prod.state_code end) as matched_state_code

,count(distinct case when prod.lalvoterid is not null then prod.lalvoterid end) as prod_distinct_lalvoterid
,count(distinct case when prod.voters_statevoterid is not null then prod.voters_statevoterid end) as prod_distinct_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 is not null then prod.sos_join_key_l2 end) as prod_distinct_sos_join_key_l2
,count(distinct case when prod.l2_precinct is not null then prod.l2_precinct end) as prod_distinct_l2_precinct
,count(distinct case when prod.person_id is not null then prod.person_id end) as prod_distinct_person_id
,count(distinct case when prod.voterbase_id is not null then prod.voterbase_id end) as prod_distinct_voterbase_id
,count(distinct case when prod.dnc_precinct_id is not null then prod.dnc_precinct_id end) as prod_distinct_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id is not null then prod.dnc_sos_id end) as prod_distinct_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc is not null then prod.sos_join_key_dnc end) as prod_distinct_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id is not null then prod.dnc_county_voter_id end) as prod_distinct_dnc_county_voter_id
,count(distinct case when prod.county_fips is not null then prod.county_fips end) as prod_distinct_county_fips
,count(distinct case when prod.pdiid is not null then prod.pdiid end) as prod_distinct_pdiid
,count(distinct case when prod.pdi_sos_id is not null then prod.pdi_sos_id end) as prod_distinct_pdi_sos_id
,count(distinct case when prod.pdi_precinct is not null then prod.pdi_precinct end) as prod_distinct_pdi_precinct
,count(distinct case when prod.actionkit_id is not null then prod.actionkit_id end) as prod_distinct_actionkit_id
,count(distinct case when prod.ak_updated_at is not null then prod.ak_updated_at end) as prod_distinct_ak_updated_at
,count(distinct case when prod.bern_id is not null then prod.bern_id end) as prod_distinct_bern_id
,count(distinct case when prod.bern_canvasser_id is not null then prod.bern_canvasser_id end) as prod_distinct_bern_canvasser_id
,count(distinct case when prod.myc_van_id is not null then prod.myc_van_id end) as prod_distinct_myc_van_id
,count(distinct case when prod.st_myc_van_id is not null then prod.st_myc_van_id end) as prod_distinct_st_myc_van_id
,count(distinct case when prod.myc_dupe is not null then prod.myc_dupe end) as prod_distinct_myc_dupe
,count(distinct case when prod.myc_voter_type_id is not null then prod.myc_voter_type_id end) as prod_distinct_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked is not null then prod.myc_myv_linked end) as prod_distinct_myc_myv_linked
,count(distinct case when prod.myv_van_id is not null then prod.myv_van_id end) as prod_distinct_myv_van_id
,count(distinct case when prod.st_myv_van_id is not null then prod.st_myv_van_id end) as prod_distinct_st_myv_van_id
,count(distinct case when prod.email is not null then prod.email end) as prod_distinct_email
,count(distinct case when prod.aux_myc_email is not null then prod.aux_myc_email end) as prod_distinct_aux_myc_email
,count(distinct case when prod.student_hash is not null then prod.student_hash end) as prod_distinct_student_hash
,count(distinct case when prod.student_rn is not null then prod.student_rn end) as prod_distinct_student_rn
,count(distinct case when prod.state_code is not null then prod.state_code end) as prod_distinct_state_code

,count(distinct case when dev.lalvoterid is not null then dev.lalvoterid end) as dev_distinct_lalvoterid
,count(distinct case when dev.voters_statevoterid is not null then dev.voters_statevoterid end) as dev_distinct_voters_statevoterid
,count(distinct case when dev.sos_join_key_l2 is not null then dev.sos_join_key_l2 end) as dev_distinct_sos_join_key_l2
,count(distinct case when dev.l2_precinct is not null then dev.l2_precinct end) as dev_distinct_l2_precinct
,count(distinct case when dev.person_id is not null then dev.person_id end) as dev_distinct_person_id
,count(distinct case when dev.voterbase_id is not null then dev.voterbase_id end) as dev_distinct_voterbase_id
,count(distinct case when dev.dnc_precinct_id is not null then dev.dnc_precinct_id end) as dev_distinct_dnc_precinct_id
,count(distinct case when dev.dnc_sos_id is not null then dev.dnc_sos_id end) as dev_distinct_dnc_sos_id
,count(distinct case when dev.sos_join_key_dnc is not null then dev.sos_join_key_dnc end) as dev_distinct_sos_join_key_dnc
,count(distinct case when dev.dnc_county_voter_id is not null then dev.dnc_county_voter_id end) as dev_distinct_dnc_county_voter_id
,count(distinct case when dev.county_fips is not null then dev.county_fips end) as dev_distinct_county_fips
,count(distinct case when dev.pdiid is not null then dev.pdiid end) as dev_distinct_pdiid
,count(distinct case when dev.pdi_sos_id is not null then dev.pdi_sos_id end) as dev_distinct_pdi_sos_id
,count(distinct case when dev.pdi_precinct is not null then dev.pdi_precinct end) as dev_distinct_pdi_precinct
,count(distinct case when dev.actionkit_id is not null then dev.actionkit_id end) as dev_distinct_actionkit_id
,count(distinct case when dev.ak_updated_at is not null then dev.ak_updated_at end) as dev_distinct_ak_updated_at
,count(distinct case when dev.bern_id is not null then dev.bern_id end) as dev_distinct_bern_id
,count(distinct case when dev.bern_canvasser_id is not null then dev.bern_canvasser_id end) as dev_distinct_bern_canvasser_id
,count(distinct case when dev.myc_van_id is not null then dev.myc_van_id end) as dev_distinct_myc_van_id
,count(distinct case when dev.st_myc_van_id is not null then dev.st_myc_van_id end) as dev_distinct_st_myc_van_id
,count(distinct case when dev.myc_dupe is not null then dev.myc_dupe end) as dev_distinct_myc_dupe
,count(distinct case when dev.myc_voter_type_id is not null then dev.myc_voter_type_id end) as dev_distinct_myc_voter_type_id
,count(distinct case when dev.myc_myv_linked is not null then dev.myc_myv_linked end) as dev_distinct_myc_myv_linked
,count(distinct case when dev.myv_van_id is not null then dev.myv_van_id end) as dev_distinct_myv_van_id
,count(distinct case when dev.st_myv_van_id is not null then dev.st_myv_van_id end) as dev_distinct_st_myv_van_id
,count(distinct case when dev.email is not null then dev.email end) as dev_distinct_email
,count(distinct case when dev.aux_myc_email is not null then dev.aux_myc_email end) as dev_distinct_aux_myc_email
,count(distinct case when dev.student_hash is not null then dev.student_hash end) as dev_distinct_student_hash
,count(distinct case when dev.student_rn is not null then dev.student_rn end) as dev_distinct_student_rn
,count(distinct case when dev.state_code is not null then dev.state_code end) as dev_distinct_state_code

,count(*)
from 
bernie_data_commons.master_xwalk_dnc prod
inner join 
bernie_nmarchio2.master_xwalk_dnc_20200224 dev
using(person_id)
group by 1,2)
union all
(select 
 'lalvoterid' as id_type
,'inner join' as join_type 
,count(distinct case when prod.lalvoterid = dev.lalvoterid then prod.lalvoterid end) as matched_lalvoterid
,count(distinct case when prod.voters_statevoterid = dev.voters_statevoterid then prod.voters_statevoterid end) as matched_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 = dev.sos_join_key_l2 then prod.sos_join_key_l2 end) as matched_sos_join_key_l2
,count(distinct case when prod.l2_precinct = dev.l2_precinct then prod.l2_precinct end) as matched_l2_precinct
,count(distinct case when prod.person_id = dev.person_id then prod.person_id end) as matched_person_id
,count(distinct case when prod.voterbase_id = dev.voterbase_id then prod.voterbase_id end) as matched_voterbase_id
,count(distinct case when prod.dnc_precinct_id = dev.dnc_precinct_id then prod.dnc_precinct_id end) as matched_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id = dev.dnc_sos_id then prod.dnc_sos_id end) as matched_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc = dev.sos_join_key_dnc then prod.sos_join_key_dnc end) as matched_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id = dev.dnc_county_voter_id then prod.dnc_county_voter_id end) as matched_dnc_county_voter_id
,count(distinct case when prod.county_fips = dev.county_fips then prod.county_fips end) as matched_county_fips
,count(distinct case when prod.pdiid = dev.pdiid then prod.pdiid end) as matched_pdiid
,count(distinct case when prod.pdi_sos_id = dev.pdi_sos_id then prod.pdi_sos_id end) as matched_pdi_sos_id
,count(distinct case when prod.pdi_precinct = dev.pdi_precinct then prod.pdi_precinct end) as matched_pdi_precinct
,count(distinct case when prod.actionkit_id = dev.actionkit_id then prod.actionkit_id end) as matched_actionkit_id
,count(distinct case when prod.ak_updated_at = dev.ak_updated_at then prod.ak_updated_at end) as matched_ak_updated_at
,count(distinct case when prod.bern_id = dev.bern_id then prod.bern_id end) as matched_bern_id
,count(distinct case when prod.bern_canvasser_id = dev.bern_canvasser_id then prod.bern_canvasser_id end) as matched_bern_canvasser_id
,count(distinct case when prod.myc_van_id = dev.myc_van_id then prod.myc_van_id end) as matched_myc_van_id
,count(distinct case when prod.st_myc_van_id = dev.st_myc_van_id then prod.st_myc_van_id end) as matched_st_myc_van_id
,count(distinct case when prod.myc_dupe = dev.myc_dupe then prod.myc_dupe end) as matched_myc_dupe
,count(distinct case when prod.myc_voter_type_id = dev.myc_voter_type_id then prod.myc_voter_type_id end) as matched_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked = dev.myc_myv_linked then prod.myc_myv_linked end) as matched_myc_myv_linked
,count(distinct case when prod.myv_van_id = dev.myv_van_id then prod.myv_van_id end) as matched_myv_van_id
,count(distinct case when prod.st_myv_van_id = dev.st_myv_van_id then prod.st_myv_van_id end) as matched_st_myv_van_id
,count(distinct case when prod.email = dev.email then prod.email end) as matched_email
,count(distinct case when prod.aux_myc_email = dev.aux_myc_email then prod.aux_myc_email end) as matched_aux_myc_email
,count(distinct case when prod.student_hash = dev.student_hash then prod.student_hash end) as matched_student_hash
,count(distinct case when prod.student_rn = dev.student_rn then prod.student_rn end) as matched_student_rn
,count(distinct case when prod.state_code = dev.state_code then prod.state_code end) as matched_state_code

,count(distinct case when prod.lalvoterid is not null then prod.lalvoterid end) as prod_distinct_lalvoterid
,count(distinct case when prod.voters_statevoterid is not null then prod.voters_statevoterid end) as prod_distinct_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 is not null then prod.sos_join_key_l2 end) as prod_distinct_sos_join_key_l2
,count(distinct case when prod.l2_precinct is not null then prod.l2_precinct end) as prod_distinct_l2_precinct
,count(distinct case when prod.person_id is not null then prod.person_id end) as prod_distinct_person_id
,count(distinct case when prod.voterbase_id is not null then prod.voterbase_id end) as prod_distinct_voterbase_id
,count(distinct case when prod.dnc_precinct_id is not null then prod.dnc_precinct_id end) as prod_distinct_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id is not null then prod.dnc_sos_id end) as prod_distinct_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc is not null then prod.sos_join_key_dnc end) as prod_distinct_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id is not null then prod.dnc_county_voter_id end) as prod_distinct_dnc_county_voter_id
,count(distinct case when prod.county_fips is not null then prod.county_fips end) as prod_distinct_county_fips
,count(distinct case when prod.pdiid is not null then prod.pdiid end) as prod_distinct_pdiid
,count(distinct case when prod.pdi_sos_id is not null then prod.pdi_sos_id end) as prod_distinct_pdi_sos_id
,count(distinct case when prod.pdi_precinct is not null then prod.pdi_precinct end) as prod_distinct_pdi_precinct
,count(distinct case when prod.actionkit_id is not null then prod.actionkit_id end) as prod_distinct_actionkit_id
,count(distinct case when prod.ak_updated_at is not null then prod.ak_updated_at end) as prod_distinct_ak_updated_at
,count(distinct case when prod.bern_id is not null then prod.bern_id end) as prod_distinct_bern_id
,count(distinct case when prod.bern_canvasser_id is not null then prod.bern_canvasser_id end) as prod_distinct_bern_canvasser_id
,count(distinct case when prod.myc_van_id is not null then prod.myc_van_id end) as prod_distinct_myc_van_id
,count(distinct case when prod.st_myc_van_id is not null then prod.st_myc_van_id end) as prod_distinct_st_myc_van_id
,count(distinct case when prod.myc_dupe is not null then prod.myc_dupe end) as prod_distinct_myc_dupe
,count(distinct case when prod.myc_voter_type_id is not null then prod.myc_voter_type_id end) as prod_distinct_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked is not null then prod.myc_myv_linked end) as prod_distinct_myc_myv_linked
,count(distinct case when prod.myv_van_id is not null then prod.myv_van_id end) as prod_distinct_myv_van_id
,count(distinct case when prod.st_myv_van_id is not null then prod.st_myv_van_id end) as prod_distinct_st_myv_van_id
,count(distinct case when prod.email is not null then prod.email end) as prod_distinct_email
,count(distinct case when prod.aux_myc_email is not null then prod.aux_myc_email end) as prod_distinct_aux_myc_email
,count(distinct case when prod.student_hash is not null then prod.student_hash end) as prod_distinct_student_hash
,count(distinct case when prod.student_rn is not null then prod.student_rn end) as prod_distinct_student_rn
,count(distinct case when prod.state_code is not null then prod.state_code end) as prod_distinct_state_code

,count(distinct case when dev.lalvoterid is not null then dev.lalvoterid end) as dev_distinct_lalvoterid
,count(distinct case when dev.voters_statevoterid is not null then dev.voters_statevoterid end) as dev_distinct_voters_statevoterid
,count(distinct case when dev.sos_join_key_l2 is not null then dev.sos_join_key_l2 end) as dev_distinct_sos_join_key_l2
,count(distinct case when dev.l2_precinct is not null then dev.l2_precinct end) as dev_distinct_l2_precinct
,count(distinct case when dev.person_id is not null then dev.person_id end) as dev_distinct_person_id
,count(distinct case when dev.voterbase_id is not null then dev.voterbase_id end) as dev_distinct_voterbase_id
,count(distinct case when dev.dnc_precinct_id is not null then dev.dnc_precinct_id end) as dev_distinct_dnc_precinct_id
,count(distinct case when dev.dnc_sos_id is not null then dev.dnc_sos_id end) as dev_distinct_dnc_sos_id
,count(distinct case when dev.sos_join_key_dnc is not null then dev.sos_join_key_dnc end) as dev_distinct_sos_join_key_dnc
,count(distinct case when dev.dnc_county_voter_id is not null then dev.dnc_county_voter_id end) as dev_distinct_dnc_county_voter_id
,count(distinct case when dev.county_fips is not null then dev.county_fips end) as dev_distinct_county_fips
,count(distinct case when dev.pdiid is not null then dev.pdiid end) as dev_distinct_pdiid
,count(distinct case when dev.pdi_sos_id is not null then dev.pdi_sos_id end) as dev_distinct_pdi_sos_id
,count(distinct case when dev.pdi_precinct is not null then dev.pdi_precinct end) as dev_distinct_pdi_precinct
,count(distinct case when dev.actionkit_id is not null then dev.actionkit_id end) as dev_distinct_actionkit_id
,count(distinct case when dev.ak_updated_at is not null then dev.ak_updated_at end) as dev_distinct_ak_updated_at
,count(distinct case when dev.bern_id is not null then dev.bern_id end) as dev_distinct_bern_id
,count(distinct case when dev.bern_canvasser_id is not null then dev.bern_canvasser_id end) as dev_distinct_bern_canvasser_id
,count(distinct case when dev.myc_van_id is not null then dev.myc_van_id end) as dev_distinct_myc_van_id
,count(distinct case when dev.st_myc_van_id is not null then dev.st_myc_van_id end) as dev_distinct_st_myc_van_id
,count(distinct case when dev.myc_dupe is not null then dev.myc_dupe end) as dev_distinct_myc_dupe
,count(distinct case when dev.myc_voter_type_id is not null then dev.myc_voter_type_id end) as dev_distinct_myc_voter_type_id
,count(distinct case when dev.myc_myv_linked is not null then dev.myc_myv_linked end) as dev_distinct_myc_myv_linked
,count(distinct case when dev.myv_van_id is not null then dev.myv_van_id end) as dev_distinct_myv_van_id
,count(distinct case when dev.st_myv_van_id is not null then dev.st_myv_van_id end) as dev_distinct_st_myv_van_id
,count(distinct case when dev.email is not null then dev.email end) as dev_distinct_email
,count(distinct case when dev.aux_myc_email is not null then dev.aux_myc_email end) as dev_distinct_aux_myc_email
,count(distinct case when dev.student_hash is not null then dev.student_hash end) as dev_distinct_student_hash
,count(distinct case when dev.student_rn is not null then dev.student_rn end) as dev_distinct_student_rn
,count(distinct case when dev.state_code is not null then dev.state_code end) as dev_distinct_state_code

,count(*)
from 
bernie_data_commons.master_xwalk_lal prod
inner join 
bernie_nmarchio2.master_xwalk_lal_20200224 dev
using(lalvoterid)
group by 1,2)
union all
(select 
 'pdiid' as id_type
,'inner join' as join_type 
,count(distinct case when prod.lalvoterid = dev.lalvoterid then prod.lalvoterid end) as matched_lalvoterid
,count(distinct case when prod.voters_statevoterid = dev.voters_statevoterid then prod.voters_statevoterid end) as matched_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 = dev.sos_join_key_l2 then prod.sos_join_key_l2 end) as matched_sos_join_key_l2
,count(distinct case when prod.l2_precinct = dev.l2_precinct then prod.l2_precinct end) as matched_l2_precinct
,count(distinct case when prod.person_id = dev.person_id then prod.person_id end) as matched_person_id
,count(distinct case when prod.voterbase_id = dev.voterbase_id then prod.voterbase_id end) as matched_voterbase_id
,count(distinct case when prod.dnc_precinct_id = dev.dnc_precinct_id then prod.dnc_precinct_id end) as matched_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id = dev.dnc_sos_id then prod.dnc_sos_id end) as matched_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc = dev.sos_join_key_dnc then prod.sos_join_key_dnc end) as matched_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id = dev.dnc_county_voter_id then prod.dnc_county_voter_id end) as matched_dnc_county_voter_id
,count(distinct case when prod.county_fips = dev.county_fips then prod.county_fips end) as matched_county_fips
,count(distinct case when prod.pdiid = dev.pdiid then prod.pdiid end) as matched_pdiid
,count(distinct case when prod.pdi_sos_id = dev.pdi_sos_id then prod.pdi_sos_id end) as matched_pdi_sos_id
,count(distinct case when prod.pdi_precinct = dev.pdi_precinct then prod.pdi_precinct end) as matched_pdi_precinct
,count(distinct case when prod.actionkit_id = dev.actionkit_id then prod.actionkit_id end) as matched_actionkit_id
,count(distinct case when prod.ak_updated_at = dev.ak_updated_at then prod.ak_updated_at end) as matched_ak_updated_at
,count(distinct case when prod.bern_id = dev.bern_id then prod.bern_id end) as matched_bern_id
,count(distinct case when prod.bern_canvasser_id = dev.bern_canvasser_id then prod.bern_canvasser_id end) as matched_bern_canvasser_id
,count(distinct case when prod.myc_van_id = dev.myc_van_id then prod.myc_van_id end) as matched_myc_van_id
,count(distinct case when prod.st_myc_van_id = dev.st_myc_van_id then prod.st_myc_van_id end) as matched_st_myc_van_id
,count(distinct case when prod.myc_dupe = dev.myc_dupe then prod.myc_dupe end) as matched_myc_dupe
,count(distinct case when prod.myc_voter_type_id = dev.myc_voter_type_id then prod.myc_voter_type_id end) as matched_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked = dev.myc_myv_linked then prod.myc_myv_linked end) as matched_myc_myv_linked
,count(distinct case when prod.myv_van_id = dev.myv_van_id then prod.myv_van_id end) as matched_myv_van_id
,count(distinct case when prod.st_myv_van_id = dev.st_myv_van_id then prod.st_myv_van_id end) as matched_st_myv_van_id
,count(distinct case when prod.email = dev.email then prod.email end) as matched_email
,count(distinct case when prod.aux_myc_email = dev.aux_myc_email then prod.aux_myc_email end) as matched_aux_myc_email
,count(distinct case when prod.student_hash = dev.student_hash then prod.student_hash end) as matched_student_hash
,count(distinct case when prod.student_rn = dev.student_rn then prod.student_rn end) as matched_student_rn
,count(distinct case when prod.state_code = dev.state_code then prod.state_code end) as matched_state_code

,count(distinct case when prod.lalvoterid is not null then prod.lalvoterid end) as prod_distinct_lalvoterid
,count(distinct case when prod.voters_statevoterid is not null then prod.voters_statevoterid end) as prod_distinct_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 is not null then prod.sos_join_key_l2 end) as prod_distinct_sos_join_key_l2
,count(distinct case when prod.l2_precinct is not null then prod.l2_precinct end) as prod_distinct_l2_precinct
,count(distinct case when prod.person_id is not null then prod.person_id end) as prod_distinct_person_id
,count(distinct case when prod.voterbase_id is not null then prod.voterbase_id end) as prod_distinct_voterbase_id
,count(distinct case when prod.dnc_precinct_id is not null then prod.dnc_precinct_id end) as prod_distinct_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id is not null then prod.dnc_sos_id end) as prod_distinct_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc is not null then prod.sos_join_key_dnc end) as prod_distinct_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id is not null then prod.dnc_county_voter_id end) as prod_distinct_dnc_county_voter_id
,count(distinct case when prod.county_fips is not null then prod.county_fips end) as prod_distinct_county_fips
,count(distinct case when prod.pdiid is not null then prod.pdiid end) as prod_distinct_pdiid
,count(distinct case when prod.pdi_sos_id is not null then prod.pdi_sos_id end) as prod_distinct_pdi_sos_id
,count(distinct case when prod.pdi_precinct is not null then prod.pdi_precinct end) as prod_distinct_pdi_precinct
,count(distinct case when prod.actionkit_id is not null then prod.actionkit_id end) as prod_distinct_actionkit_id
,count(distinct case when prod.ak_updated_at is not null then prod.ak_updated_at end) as prod_distinct_ak_updated_at
,count(distinct case when prod.bern_id is not null then prod.bern_id end) as prod_distinct_bern_id
,count(distinct case when prod.bern_canvasser_id is not null then prod.bern_canvasser_id end) as prod_distinct_bern_canvasser_id
,count(distinct case when prod.myc_van_id is not null then prod.myc_van_id end) as prod_distinct_myc_van_id
,count(distinct case when prod.st_myc_van_id is not null then prod.st_myc_van_id end) as prod_distinct_st_myc_van_id
,count(distinct case when prod.myc_dupe is not null then prod.myc_dupe end) as prod_distinct_myc_dupe
,count(distinct case when prod.myc_voter_type_id is not null then prod.myc_voter_type_id end) as prod_distinct_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked is not null then prod.myc_myv_linked end) as prod_distinct_myc_myv_linked
,count(distinct case when prod.myv_van_id is not null then prod.myv_van_id end) as prod_distinct_myv_van_id
,count(distinct case when prod.st_myv_van_id is not null then prod.st_myv_van_id end) as prod_distinct_st_myv_van_id
,count(distinct case when prod.email is not null then prod.email end) as prod_distinct_email
,count(distinct case when prod.aux_myc_email is not null then prod.aux_myc_email end) as prod_distinct_aux_myc_email
,count(distinct case when prod.student_hash is not null then prod.student_hash end) as prod_distinct_student_hash
,count(distinct case when prod.student_rn is not null then prod.student_rn end) as prod_distinct_student_rn
,count(distinct case when prod.state_code is not null then prod.state_code end) as prod_distinct_state_code

,count(distinct case when dev.lalvoterid is not null then dev.lalvoterid end) as dev_distinct_lalvoterid
,count(distinct case when dev.voters_statevoterid is not null then dev.voters_statevoterid end) as dev_distinct_voters_statevoterid
,count(distinct case when dev.sos_join_key_l2 is not null then dev.sos_join_key_l2 end) as dev_distinct_sos_join_key_l2
,count(distinct case when dev.l2_precinct is not null then dev.l2_precinct end) as dev_distinct_l2_precinct
,count(distinct case when dev.person_id is not null then dev.person_id end) as dev_distinct_person_id
,count(distinct case when dev.voterbase_id is not null then dev.voterbase_id end) as dev_distinct_voterbase_id
,count(distinct case when dev.dnc_precinct_id is not null then dev.dnc_precinct_id end) as dev_distinct_dnc_precinct_id
,count(distinct case when dev.dnc_sos_id is not null then dev.dnc_sos_id end) as dev_distinct_dnc_sos_id
,count(distinct case when dev.sos_join_key_dnc is not null then dev.sos_join_key_dnc end) as dev_distinct_sos_join_key_dnc
,count(distinct case when dev.dnc_county_voter_id is not null then dev.dnc_county_voter_id end) as dev_distinct_dnc_county_voter_id
,count(distinct case when dev.county_fips is not null then dev.county_fips end) as dev_distinct_county_fips
,count(distinct case when dev.pdiid is not null then dev.pdiid end) as dev_distinct_pdiid
,count(distinct case when dev.pdi_sos_id is not null then dev.pdi_sos_id end) as dev_distinct_pdi_sos_id
,count(distinct case when dev.pdi_precinct is not null then dev.pdi_precinct end) as dev_distinct_pdi_precinct
,count(distinct case when dev.actionkit_id is not null then dev.actionkit_id end) as dev_distinct_actionkit_id
,count(distinct case when dev.ak_updated_at is not null then dev.ak_updated_at end) as dev_distinct_ak_updated_at
,count(distinct case when dev.bern_id is not null then dev.bern_id end) as dev_distinct_bern_id
,count(distinct case when dev.bern_canvasser_id is not null then dev.bern_canvasser_id end) as dev_distinct_bern_canvasser_id
,count(distinct case when dev.myc_van_id is not null then dev.myc_van_id end) as dev_distinct_myc_van_id
,count(distinct case when dev.st_myc_van_id is not null then dev.st_myc_van_id end) as dev_distinct_st_myc_van_id
,count(distinct case when dev.myc_dupe is not null then dev.myc_dupe end) as dev_distinct_myc_dupe
,count(distinct case when dev.myc_voter_type_id is not null then dev.myc_voter_type_id end) as dev_distinct_myc_voter_type_id
,count(distinct case when dev.myc_myv_linked is not null then dev.myc_myv_linked end) as dev_distinct_myc_myv_linked
,count(distinct case when dev.myv_van_id is not null then dev.myv_van_id end) as dev_distinct_myv_van_id
,count(distinct case when dev.st_myv_van_id is not null then dev.st_myv_van_id end) as dev_distinct_st_myv_van_id
,count(distinct case when dev.email is not null then dev.email end) as dev_distinct_email
,count(distinct case when dev.aux_myc_email is not null then dev.aux_myc_email end) as dev_distinct_aux_myc_email
,count(distinct case when dev.student_hash is not null then dev.student_hash end) as dev_distinct_student_hash
,count(distinct case when dev.student_rn is not null then dev.student_rn end) as dev_distinct_student_rn
,count(distinct case when dev.state_code is not null then dev.state_code end) as dev_distinct_state_code

,count(*)
from 
bernie_data_commons.master_xwalk_pdi prod
inner join 
bernie_nmarchio2.master_xwalk_pdi_20200224 dev
using(pdiid)
group by 1,2)
union all
(select 
 'st_myc_van_id' as id_type
,'inner join' as join_type 
,count(distinct case when prod.lalvoterid = dev.lalvoterid then prod.lalvoterid end) as matched_lalvoterid
,count(distinct case when prod.voters_statevoterid = dev.voters_statevoterid then prod.voters_statevoterid end) as matched_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 = dev.sos_join_key_l2 then prod.sos_join_key_l2 end) as matched_sos_join_key_l2
,count(distinct case when prod.l2_precinct = dev.l2_precinct then prod.l2_precinct end) as matched_l2_precinct
,count(distinct case when prod.person_id = dev.person_id then prod.person_id end) as matched_person_id
,count(distinct case when prod.voterbase_id = dev.voterbase_id then prod.voterbase_id end) as matched_voterbase_id
,count(distinct case when prod.dnc_precinct_id = dev.dnc_precinct_id then prod.dnc_precinct_id end) as matched_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id = dev.dnc_sos_id then prod.dnc_sos_id end) as matched_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc = dev.sos_join_key_dnc then prod.sos_join_key_dnc end) as matched_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id = dev.dnc_county_voter_id then prod.dnc_county_voter_id end) as matched_dnc_county_voter_id
,count(distinct case when prod.county_fips = dev.county_fips then prod.county_fips end) as matched_county_fips
,count(distinct case when prod.pdiid = dev.pdiid then prod.pdiid end) as matched_pdiid
,count(distinct case when prod.pdi_sos_id = dev.pdi_sos_id then prod.pdi_sos_id end) as matched_pdi_sos_id
,count(distinct case when prod.pdi_precinct = dev.pdi_precinct then prod.pdi_precinct end) as matched_pdi_precinct
,count(distinct case when prod.actionkit_id = dev.actionkit_id then prod.actionkit_id end) as matched_actionkit_id
,count(distinct case when prod.ak_updated_at = dev.ak_updated_at then prod.ak_updated_at end) as matched_ak_updated_at
,count(distinct case when prod.bern_id = dev.bern_id then prod.bern_id end) as matched_bern_id
,count(distinct case when prod.bern_canvasser_id = dev.bern_canvasser_id then prod.bern_canvasser_id end) as matched_bern_canvasser_id
,count(distinct case when prod.myc_van_id = dev.myc_van_id then prod.myc_van_id end) as matched_myc_van_id
,count(distinct case when prod.st_myc_van_id = dev.st_myc_van_id then prod.st_myc_van_id end) as matched_st_myc_van_id
,count(distinct case when prod.myc_dupe = dev.myc_dupe then prod.myc_dupe end) as matched_myc_dupe
,count(distinct case when prod.myc_voter_type_id = dev.myc_voter_type_id then prod.myc_voter_type_id end) as matched_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked = dev.myc_myv_linked then prod.myc_myv_linked end) as matched_myc_myv_linked
,count(distinct case when prod.myv_van_id = dev.myv_van_id then prod.myv_van_id end) as matched_myv_van_id
,count(distinct case when prod.st_myv_van_id = dev.st_myv_van_id then prod.st_myv_van_id end) as matched_st_myv_van_id
,count(distinct case when prod.email = dev.email then prod.email end) as matched_email
,count(distinct case when prod.aux_myc_email = dev.aux_myc_email then prod.aux_myc_email end) as matched_aux_myc_email
,count(distinct case when prod.student_hash = dev.student_hash then prod.student_hash end) as matched_student_hash
,count(distinct case when prod.student_rn = dev.student_rn then prod.student_rn end) as matched_student_rn
,count(distinct case when prod.state_code = dev.state_code then prod.state_code end) as matched_state_code

,count(distinct case when prod.lalvoterid is not null then prod.lalvoterid end) as prod_distinct_lalvoterid
,count(distinct case when prod.voters_statevoterid is not null then prod.voters_statevoterid end) as prod_distinct_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 is not null then prod.sos_join_key_l2 end) as prod_distinct_sos_join_key_l2
,count(distinct case when prod.l2_precinct is not null then prod.l2_precinct end) as prod_distinct_l2_precinct
,count(distinct case when prod.person_id is not null then prod.person_id end) as prod_distinct_person_id
,count(distinct case when prod.voterbase_id is not null then prod.voterbase_id end) as prod_distinct_voterbase_id
,count(distinct case when prod.dnc_precinct_id is not null then prod.dnc_precinct_id end) as prod_distinct_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id is not null then prod.dnc_sos_id end) as prod_distinct_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc is not null then prod.sos_join_key_dnc end) as prod_distinct_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id is not null then prod.dnc_county_voter_id end) as prod_distinct_dnc_county_voter_id
,count(distinct case when prod.county_fips is not null then prod.county_fips end) as prod_distinct_county_fips
,count(distinct case when prod.pdiid is not null then prod.pdiid end) as prod_distinct_pdiid
,count(distinct case when prod.pdi_sos_id is not null then prod.pdi_sos_id end) as prod_distinct_pdi_sos_id
,count(distinct case when prod.pdi_precinct is not null then prod.pdi_precinct end) as prod_distinct_pdi_precinct
,count(distinct case when prod.actionkit_id is not null then prod.actionkit_id end) as prod_distinct_actionkit_id
,count(distinct case when prod.ak_updated_at is not null then prod.ak_updated_at end) as prod_distinct_ak_updated_at
,count(distinct case when prod.bern_id is not null then prod.bern_id end) as prod_distinct_bern_id
,count(distinct case when prod.bern_canvasser_id is not null then prod.bern_canvasser_id end) as prod_distinct_bern_canvasser_id
,count(distinct case when prod.myc_van_id is not null then prod.myc_van_id end) as prod_distinct_myc_van_id
,count(distinct case when prod.st_myc_van_id is not null then prod.st_myc_van_id end) as prod_distinct_st_myc_van_id
,count(distinct case when prod.myc_dupe is not null then prod.myc_dupe end) as prod_distinct_myc_dupe
,count(distinct case when prod.myc_voter_type_id is not null then prod.myc_voter_type_id end) as prod_distinct_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked is not null then prod.myc_myv_linked end) as prod_distinct_myc_myv_linked
,count(distinct case when prod.myv_van_id is not null then prod.myv_van_id end) as prod_distinct_myv_van_id
,count(distinct case when prod.st_myv_van_id is not null then prod.st_myv_van_id end) as prod_distinct_st_myv_van_id
,count(distinct case when prod.email is not null then prod.email end) as prod_distinct_email
,count(distinct case when prod.aux_myc_email is not null then prod.aux_myc_email end) as prod_distinct_aux_myc_email
,count(distinct case when prod.student_hash is not null then prod.student_hash end) as prod_distinct_student_hash
,count(distinct case when prod.student_rn is not null then prod.student_rn end) as prod_distinct_student_rn
,count(distinct case when prod.state_code is not null then prod.state_code end) as prod_distinct_state_code

,count(distinct case when dev.lalvoterid is not null then dev.lalvoterid end) as dev_distinct_lalvoterid
,count(distinct case when dev.voters_statevoterid is not null then dev.voters_statevoterid end) as dev_distinct_voters_statevoterid
,count(distinct case when dev.sos_join_key_l2 is not null then dev.sos_join_key_l2 end) as dev_distinct_sos_join_key_l2
,count(distinct case when dev.l2_precinct is not null then dev.l2_precinct end) as dev_distinct_l2_precinct
,count(distinct case when dev.person_id is not null then dev.person_id end) as dev_distinct_person_id
,count(distinct case when dev.voterbase_id is not null then dev.voterbase_id end) as dev_distinct_voterbase_id
,count(distinct case when dev.dnc_precinct_id is not null then dev.dnc_precinct_id end) as dev_distinct_dnc_precinct_id
,count(distinct case when dev.dnc_sos_id is not null then dev.dnc_sos_id end) as dev_distinct_dnc_sos_id
,count(distinct case when dev.sos_join_key_dnc is not null then dev.sos_join_key_dnc end) as dev_distinct_sos_join_key_dnc
,count(distinct case when dev.dnc_county_voter_id is not null then dev.dnc_county_voter_id end) as dev_distinct_dnc_county_voter_id
,count(distinct case when dev.county_fips is not null then dev.county_fips end) as dev_distinct_county_fips
,count(distinct case when dev.pdiid is not null then dev.pdiid end) as dev_distinct_pdiid
,count(distinct case when dev.pdi_sos_id is not null then dev.pdi_sos_id end) as dev_distinct_pdi_sos_id
,count(distinct case when dev.pdi_precinct is not null then dev.pdi_precinct end) as dev_distinct_pdi_precinct
,count(distinct case when dev.actionkit_id is not null then dev.actionkit_id end) as dev_distinct_actionkit_id
,count(distinct case when dev.ak_updated_at is not null then dev.ak_updated_at end) as dev_distinct_ak_updated_at
,count(distinct case when dev.bern_id is not null then dev.bern_id end) as dev_distinct_bern_id
,count(distinct case when dev.bern_canvasser_id is not null then dev.bern_canvasser_id end) as dev_distinct_bern_canvasser_id
,count(distinct case when dev.myc_van_id is not null then dev.myc_van_id end) as dev_distinct_myc_van_id
,count(distinct case when dev.st_myc_van_id is not null then dev.st_myc_van_id end) as dev_distinct_st_myc_van_id
,count(distinct case when dev.myc_dupe is not null then dev.myc_dupe end) as dev_distinct_myc_dupe
,count(distinct case when dev.myc_voter_type_id is not null then dev.myc_voter_type_id end) as dev_distinct_myc_voter_type_id
,count(distinct case when dev.myc_myv_linked is not null then dev.myc_myv_linked end) as dev_distinct_myc_myv_linked
,count(distinct case when dev.myv_van_id is not null then dev.myv_van_id end) as dev_distinct_myv_van_id
,count(distinct case when dev.st_myv_van_id is not null then dev.st_myv_van_id end) as dev_distinct_st_myv_van_id
,count(distinct case when dev.email is not null then dev.email end) as dev_distinct_email
,count(distinct case when dev.aux_myc_email is not null then dev.aux_myc_email end) as dev_distinct_aux_myc_email
,count(distinct case when dev.student_hash is not null then dev.student_hash end) as dev_distinct_student_hash
,count(distinct case when dev.student_rn is not null then dev.student_rn end) as dev_distinct_student_rn
,count(distinct case when dev.state_code is not null then dev.state_code end) as dev_distinct_state_code

,count(*)
from 
bernie_data_commons.master_xwalk_st_myc prod
inner join 
bernie_nmarchio2.master_xwalk_st_myc_20200224 dev
using(st_myc_van_id)
group by 1,2)
union all
(select 
 'st_myv_van_id' as id_type
,'inner join' as join_type 
,count(distinct case when prod.lalvoterid = dev.lalvoterid then prod.lalvoterid end) as matched_lalvoterid
,count(distinct case when prod.voters_statevoterid = dev.voters_statevoterid then prod.voters_statevoterid end) as matched_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 = dev.sos_join_key_l2 then prod.sos_join_key_l2 end) as matched_sos_join_key_l2
,count(distinct case when prod.l2_precinct = dev.l2_precinct then prod.l2_precinct end) as matched_l2_precinct
,count(distinct case when prod.person_id = dev.person_id then prod.person_id end) as matched_person_id
,count(distinct case when prod.voterbase_id = dev.voterbase_id then prod.voterbase_id end) as matched_voterbase_id
,count(distinct case when prod.dnc_precinct_id = dev.dnc_precinct_id then prod.dnc_precinct_id end) as matched_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id = dev.dnc_sos_id then prod.dnc_sos_id end) as matched_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc = dev.sos_join_key_dnc then prod.sos_join_key_dnc end) as matched_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id = dev.dnc_county_voter_id then prod.dnc_county_voter_id end) as matched_dnc_county_voter_id
,count(distinct case when prod.county_fips = dev.county_fips then prod.county_fips end) as matched_county_fips
,count(distinct case when prod.pdiid = dev.pdiid then prod.pdiid end) as matched_pdiid
,count(distinct case when prod.pdi_sos_id = dev.pdi_sos_id then prod.pdi_sos_id end) as matched_pdi_sos_id
,count(distinct case when prod.pdi_precinct = dev.pdi_precinct then prod.pdi_precinct end) as matched_pdi_precinct
,count(distinct case when prod.actionkit_id = dev.actionkit_id then prod.actionkit_id end) as matched_actionkit_id
,count(distinct case when prod.ak_updated_at = dev.ak_updated_at then prod.ak_updated_at end) as matched_ak_updated_at
,count(distinct case when prod.bern_id = dev.bern_id then prod.bern_id end) as matched_bern_id
,count(distinct case when prod.bern_canvasser_id = dev.bern_canvasser_id then prod.bern_canvasser_id end) as matched_bern_canvasser_id
,count(distinct case when prod.myc_van_id = dev.myc_van_id then prod.myc_van_id end) as matched_myc_van_id
,count(distinct case when prod.st_myc_van_id = dev.st_myc_van_id then prod.st_myc_van_id end) as matched_st_myc_van_id
,count(distinct case when prod.myc_dupe = dev.myc_dupe then prod.myc_dupe end) as matched_myc_dupe
,count(distinct case when prod.myc_voter_type_id = dev.myc_voter_type_id then prod.myc_voter_type_id end) as matched_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked = dev.myc_myv_linked then prod.myc_myv_linked end) as matched_myc_myv_linked
,count(distinct case when prod.myv_van_id = dev.myv_van_id then prod.myv_van_id end) as matched_myv_van_id
,count(distinct case when prod.st_myv_van_id = dev.st_myv_van_id then prod.st_myv_van_id end) as matched_st_myv_van_id
,count(distinct case when prod.email = dev.email then prod.email end) as matched_email
,count(distinct case when prod.aux_myc_email = dev.aux_myc_email then prod.aux_myc_email end) as matched_aux_myc_email
,count(distinct case when prod.student_hash = dev.student_hash then prod.student_hash end) as matched_student_hash
,count(distinct case when prod.student_rn = dev.student_rn then prod.student_rn end) as matched_student_rn
,count(distinct case when prod.state_code = dev.state_code then prod.state_code end) as matched_state_code

,count(distinct case when prod.lalvoterid is not null then prod.lalvoterid end) as prod_distinct_lalvoterid
,count(distinct case when prod.voters_statevoterid is not null then prod.voters_statevoterid end) as prod_distinct_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 is not null then prod.sos_join_key_l2 end) as prod_distinct_sos_join_key_l2
,count(distinct case when prod.l2_precinct is not null then prod.l2_precinct end) as prod_distinct_l2_precinct
,count(distinct case when prod.person_id is not null then prod.person_id end) as prod_distinct_person_id
,count(distinct case when prod.voterbase_id is not null then prod.voterbase_id end) as prod_distinct_voterbase_id
,count(distinct case when prod.dnc_precinct_id is not null then prod.dnc_precinct_id end) as prod_distinct_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id is not null then prod.dnc_sos_id end) as prod_distinct_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc is not null then prod.sos_join_key_dnc end) as prod_distinct_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id is not null then prod.dnc_county_voter_id end) as prod_distinct_dnc_county_voter_id
,count(distinct case when prod.county_fips is not null then prod.county_fips end) as prod_distinct_county_fips
,count(distinct case when prod.pdiid is not null then prod.pdiid end) as prod_distinct_pdiid
,count(distinct case when prod.pdi_sos_id is not null then prod.pdi_sos_id end) as prod_distinct_pdi_sos_id
,count(distinct case when prod.pdi_precinct is not null then prod.pdi_precinct end) as prod_distinct_pdi_precinct
,count(distinct case when prod.actionkit_id is not null then prod.actionkit_id end) as prod_distinct_actionkit_id
,count(distinct case when prod.ak_updated_at is not null then prod.ak_updated_at end) as prod_distinct_ak_updated_at
,count(distinct case when prod.bern_id is not null then prod.bern_id end) as prod_distinct_bern_id
,count(distinct case when prod.bern_canvasser_id is not null then prod.bern_canvasser_id end) as prod_distinct_bern_canvasser_id
,count(distinct case when prod.myc_van_id is not null then prod.myc_van_id end) as prod_distinct_myc_van_id
,count(distinct case when prod.st_myc_van_id is not null then prod.st_myc_van_id end) as prod_distinct_st_myc_van_id
,count(distinct case when prod.myc_dupe is not null then prod.myc_dupe end) as prod_distinct_myc_dupe
,count(distinct case when prod.myc_voter_type_id is not null then prod.myc_voter_type_id end) as prod_distinct_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked is not null then prod.myc_myv_linked end) as prod_distinct_myc_myv_linked
,count(distinct case when prod.myv_van_id is not null then prod.myv_van_id end) as prod_distinct_myv_van_id
,count(distinct case when prod.st_myv_van_id is not null then prod.st_myv_van_id end) as prod_distinct_st_myv_van_id
,count(distinct case when prod.email is not null then prod.email end) as prod_distinct_email
,count(distinct case when prod.aux_myc_email is not null then prod.aux_myc_email end) as prod_distinct_aux_myc_email
,count(distinct case when prod.student_hash is not null then prod.student_hash end) as prod_distinct_student_hash
,count(distinct case when prod.student_rn is not null then prod.student_rn end) as prod_distinct_student_rn
,count(distinct case when prod.state_code is not null then prod.state_code end) as prod_distinct_state_code

,count(distinct case when dev.lalvoterid is not null then dev.lalvoterid end) as dev_distinct_lalvoterid
,count(distinct case when dev.voters_statevoterid is not null then dev.voters_statevoterid end) as dev_distinct_voters_statevoterid
,count(distinct case when dev.sos_join_key_l2 is not null then dev.sos_join_key_l2 end) as dev_distinct_sos_join_key_l2
,count(distinct case when dev.l2_precinct is not null then dev.l2_precinct end) as dev_distinct_l2_precinct
,count(distinct case when dev.person_id is not null then dev.person_id end) as dev_distinct_person_id
,count(distinct case when dev.voterbase_id is not null then dev.voterbase_id end) as dev_distinct_voterbase_id
,count(distinct case when dev.dnc_precinct_id is not null then dev.dnc_precinct_id end) as dev_distinct_dnc_precinct_id
,count(distinct case when dev.dnc_sos_id is not null then dev.dnc_sos_id end) as dev_distinct_dnc_sos_id
,count(distinct case when dev.sos_join_key_dnc is not null then dev.sos_join_key_dnc end) as dev_distinct_sos_join_key_dnc
,count(distinct case when dev.dnc_county_voter_id is not null then dev.dnc_county_voter_id end) as dev_distinct_dnc_county_voter_id
,count(distinct case when dev.county_fips is not null then dev.county_fips end) as dev_distinct_county_fips
,count(distinct case when dev.pdiid is not null then dev.pdiid end) as dev_distinct_pdiid
,count(distinct case when dev.pdi_sos_id is not null then dev.pdi_sos_id end) as dev_distinct_pdi_sos_id
,count(distinct case when dev.pdi_precinct is not null then dev.pdi_precinct end) as dev_distinct_pdi_precinct
,count(distinct case when dev.actionkit_id is not null then dev.actionkit_id end) as dev_distinct_actionkit_id
,count(distinct case when dev.ak_updated_at is not null then dev.ak_updated_at end) as dev_distinct_ak_updated_at
,count(distinct case when dev.bern_id is not null then dev.bern_id end) as dev_distinct_bern_id
,count(distinct case when dev.bern_canvasser_id is not null then dev.bern_canvasser_id end) as dev_distinct_bern_canvasser_id
,count(distinct case when dev.myc_van_id is not null then dev.myc_van_id end) as dev_distinct_myc_van_id
,count(distinct case when dev.st_myc_van_id is not null then dev.st_myc_van_id end) as dev_distinct_st_myc_van_id
,count(distinct case when dev.myc_dupe is not null then dev.myc_dupe end) as dev_distinct_myc_dupe
,count(distinct case when dev.myc_voter_type_id is not null then dev.myc_voter_type_id end) as dev_distinct_myc_voter_type_id
,count(distinct case when dev.myc_myv_linked is not null then dev.myc_myv_linked end) as dev_distinct_myc_myv_linked
,count(distinct case when dev.myv_van_id is not null then dev.myv_van_id end) as dev_distinct_myv_van_id
,count(distinct case when dev.st_myv_van_id is not null then dev.st_myv_van_id end) as dev_distinct_st_myv_van_id
,count(distinct case when dev.email is not null then dev.email end) as dev_distinct_email
,count(distinct case when dev.aux_myc_email is not null then dev.aux_myc_email end) as dev_distinct_aux_myc_email
,count(distinct case when dev.student_hash is not null then dev.student_hash end) as dev_distinct_student_hash
,count(distinct case when dev.student_rn is not null then dev.student_rn end) as dev_distinct_student_rn
,count(distinct case when dev.state_code is not null then dev.state_code end) as dev_distinct_state_code

,count(*)
from 
bernie_data_commons.master_xwalk_st_myv prod
inner join 
bernie_nmarchio2.master_xwalk_st_myv_20200224 dev
using(st_myv_van_id)
group by 1,2)

union all


(select 
 'actionkit_id' as id_type
,'left outer join' as join_type
,count(distinct case when prod.lalvoterid = dev.lalvoterid then prod.lalvoterid end) as matched_lalvoterid
,count(distinct case when prod.voters_statevoterid = dev.voters_statevoterid then prod.voters_statevoterid end) as matched_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 = dev.sos_join_key_l2 then prod.sos_join_key_l2 end) as matched_sos_join_key_l2
,count(distinct case when prod.l2_precinct = dev.l2_precinct then prod.l2_precinct end) as matched_l2_precinct
,count(distinct case when prod.person_id = dev.person_id then prod.person_id end) as matched_person_id
,count(distinct case when prod.voterbase_id = dev.voterbase_id then prod.voterbase_id end) as matched_voterbase_id
,count(distinct case when prod.dnc_precinct_id = dev.dnc_precinct_id then prod.dnc_precinct_id end) as matched_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id = dev.dnc_sos_id then prod.dnc_sos_id end) as matched_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc = dev.sos_join_key_dnc then prod.sos_join_key_dnc end) as matched_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id = dev.dnc_county_voter_id then prod.dnc_county_voter_id end) as matched_dnc_county_voter_id
,count(distinct case when prod.county_fips = dev.county_fips then prod.county_fips end) as matched_county_fips
,count(distinct case when prod.pdiid = dev.pdiid then prod.pdiid end) as matched_pdiid
,count(distinct case when prod.pdi_sos_id = dev.pdi_sos_id then prod.pdi_sos_id end) as matched_pdi_sos_id
,count(distinct case when prod.pdi_precinct = dev.pdi_precinct then prod.pdi_precinct end) as matched_pdi_precinct
,count(distinct case when prod.actionkit_id = dev.actionkit_id then prod.actionkit_id end) as matched_actionkit_id
,count(distinct case when prod.ak_updated_at = dev.ak_updated_at then prod.ak_updated_at end) as matched_ak_updated_at
,count(distinct case when prod.bern_id = dev.bern_id then prod.bern_id end) as matched_bern_id
,count(distinct case when prod.bern_canvasser_id = dev.bern_canvasser_id then prod.bern_canvasser_id end) as matched_bern_canvasser_id
,count(distinct case when prod.myc_van_id = dev.myc_van_id then prod.myc_van_id end) as matched_myc_van_id
,count(distinct case when prod.st_myc_van_id = dev.st_myc_van_id then prod.st_myc_van_id end) as matched_st_myc_van_id
,count(distinct case when prod.myc_dupe = dev.myc_dupe then prod.myc_dupe end) as matched_myc_dupe
,count(distinct case when prod.myc_voter_type_id = dev.myc_voter_type_id then prod.myc_voter_type_id end) as matched_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked = dev.myc_myv_linked then prod.myc_myv_linked end) as matched_myc_myv_linked
,count(distinct case when prod.myv_van_id = dev.myv_van_id then prod.myv_van_id end) as matched_myv_van_id
,count(distinct case when prod.st_myv_van_id = dev.st_myv_van_id then prod.st_myv_van_id end) as matched_st_myv_van_id
,count(distinct case when prod.email = dev.email then prod.email end) as matched_email
,count(distinct case when prod.aux_myc_email = dev.aux_myc_email then prod.aux_myc_email end) as matched_aux_myc_email
,count(distinct case when prod.student_hash = dev.student_hash then prod.student_hash end) as matched_student_hash
,count(distinct case when prod.student_rn = dev.student_rn then prod.student_rn end) as matched_student_rn
,count(distinct case when prod.state_code = dev.state_code then prod.state_code end) as matched_state_code

,count(distinct case when prod.lalvoterid is not null then prod.lalvoterid end) as prod_distinct_lalvoterid
,count(distinct case when prod.voters_statevoterid is not null then prod.voters_statevoterid end) as prod_distinct_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 is not null then prod.sos_join_key_l2 end) as prod_distinct_sos_join_key_l2
,count(distinct case when prod.l2_precinct is not null then prod.l2_precinct end) as prod_distinct_l2_precinct
,count(distinct case when prod.person_id is not null then prod.person_id end) as prod_distinct_person_id
,count(distinct case when prod.voterbase_id is not null then prod.voterbase_id end) as prod_distinct_voterbase_id
,count(distinct case when prod.dnc_precinct_id is not null then prod.dnc_precinct_id end) as prod_distinct_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id is not null then prod.dnc_sos_id end) as prod_distinct_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc is not null then prod.sos_join_key_dnc end) as prod_distinct_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id is not null then prod.dnc_county_voter_id end) as prod_distinct_dnc_county_voter_id
,count(distinct case when prod.county_fips is not null then prod.county_fips end) as prod_distinct_county_fips
,count(distinct case when prod.pdiid is not null then prod.pdiid end) as prod_distinct_pdiid
,count(distinct case when prod.pdi_sos_id is not null then prod.pdi_sos_id end) as prod_distinct_pdi_sos_id
,count(distinct case when prod.pdi_precinct is not null then prod.pdi_precinct end) as prod_distinct_pdi_precinct
,count(distinct case when prod.actionkit_id is not null then prod.actionkit_id end) as prod_distinct_actionkit_id
,count(distinct case when prod.ak_updated_at is not null then prod.ak_updated_at end) as prod_distinct_ak_updated_at
,count(distinct case when prod.bern_id is not null then prod.bern_id end) as prod_distinct_bern_id
,count(distinct case when prod.bern_canvasser_id is not null then prod.bern_canvasser_id end) as prod_distinct_bern_canvasser_id
,count(distinct case when prod.myc_van_id is not null then prod.myc_van_id end) as prod_distinct_myc_van_id
,count(distinct case when prod.st_myc_van_id is not null then prod.st_myc_van_id end) as prod_distinct_st_myc_van_id
,count(distinct case when prod.myc_dupe is not null then prod.myc_dupe end) as prod_distinct_myc_dupe
,count(distinct case when prod.myc_voter_type_id is not null then prod.myc_voter_type_id end) as prod_distinct_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked is not null then prod.myc_myv_linked end) as prod_distinct_myc_myv_linked
,count(distinct case when prod.myv_van_id is not null then prod.myv_van_id end) as prod_distinct_myv_van_id
,count(distinct case when prod.st_myv_van_id is not null then prod.st_myv_van_id end) as prod_distinct_st_myv_van_id
,count(distinct case when prod.email is not null then prod.email end) as prod_distinct_email
,count(distinct case when prod.aux_myc_email is not null then prod.aux_myc_email end) as prod_distinct_aux_myc_email
,count(distinct case when prod.student_hash is not null then prod.student_hash end) as prod_distinct_student_hash
,count(distinct case when prod.student_rn is not null then prod.student_rn end) as prod_distinct_student_rn
,count(distinct case when prod.state_code is not null then prod.state_code end) as prod_distinct_state_code

,count(distinct case when dev.lalvoterid is not null then dev.lalvoterid end) as dev_distinct_lalvoterid
,count(distinct case when dev.voters_statevoterid is not null then dev.voters_statevoterid end) as dev_distinct_voters_statevoterid
,count(distinct case when dev.sos_join_key_l2 is not null then dev.sos_join_key_l2 end) as dev_distinct_sos_join_key_l2
,count(distinct case when dev.l2_precinct is not null then dev.l2_precinct end) as dev_distinct_l2_precinct
,count(distinct case when dev.person_id is not null then dev.person_id end) as dev_distinct_person_id
,count(distinct case when dev.voterbase_id is not null then dev.voterbase_id end) as dev_distinct_voterbase_id
,count(distinct case when dev.dnc_precinct_id is not null then dev.dnc_precinct_id end) as dev_distinct_dnc_precinct_id
,count(distinct case when dev.dnc_sos_id is not null then dev.dnc_sos_id end) as dev_distinct_dnc_sos_id
,count(distinct case when dev.sos_join_key_dnc is not null then dev.sos_join_key_dnc end) as dev_distinct_sos_join_key_dnc
,count(distinct case when dev.dnc_county_voter_id is not null then dev.dnc_county_voter_id end) as dev_distinct_dnc_county_voter_id
,count(distinct case when dev.county_fips is not null then dev.county_fips end) as dev_distinct_county_fips
,count(distinct case when dev.pdiid is not null then dev.pdiid end) as dev_distinct_pdiid
,count(distinct case when dev.pdi_sos_id is not null then dev.pdi_sos_id end) as dev_distinct_pdi_sos_id
,count(distinct case when dev.pdi_precinct is not null then dev.pdi_precinct end) as dev_distinct_pdi_precinct
,count(distinct case when dev.actionkit_id is not null then dev.actionkit_id end) as dev_distinct_actionkit_id
,count(distinct case when dev.ak_updated_at is not null then dev.ak_updated_at end) as dev_distinct_ak_updated_at
,count(distinct case when dev.bern_id is not null then dev.bern_id end) as dev_distinct_bern_id
,count(distinct case when dev.bern_canvasser_id is not null then dev.bern_canvasser_id end) as dev_distinct_bern_canvasser_id
,count(distinct case when dev.myc_van_id is not null then dev.myc_van_id end) as dev_distinct_myc_van_id
,count(distinct case when dev.st_myc_van_id is not null then dev.st_myc_van_id end) as dev_distinct_st_myc_van_id
,count(distinct case when dev.myc_dupe is not null then dev.myc_dupe end) as dev_distinct_myc_dupe
,count(distinct case when dev.myc_voter_type_id is not null then dev.myc_voter_type_id end) as dev_distinct_myc_voter_type_id
,count(distinct case when dev.myc_myv_linked is not null then dev.myc_myv_linked end) as dev_distinct_myc_myv_linked
,count(distinct case when dev.myv_van_id is not null then dev.myv_van_id end) as dev_distinct_myv_van_id
,count(distinct case when dev.st_myv_van_id is not null then dev.st_myv_van_id end) as dev_distinct_st_myv_van_id
,count(distinct case when dev.email is not null then dev.email end) as dev_distinct_email
,count(distinct case when dev.aux_myc_email is not null then dev.aux_myc_email end) as dev_distinct_aux_myc_email
,count(distinct case when dev.student_hash is not null then dev.student_hash end) as dev_distinct_student_hash
,count(distinct case when dev.student_rn is not null then dev.student_rn end) as dev_distinct_student_rn
,count(distinct case when dev.state_code is not null then dev.state_code end) as dev_distinct_state_code

,count(*)
 from 
bernie_data_commons.master_xwalk_ak prod
left outer join 
bernie_nmarchio2.master_xwalk_ak_20200224 dev
using(actionkit_id)
where dev.actionkit_id is null
group by 1,2)
union all
(select 
 'person_id' as id_type
,'left outer join' as join_type
,count(distinct case when prod.lalvoterid = dev.lalvoterid then prod.lalvoterid end) as matched_lalvoterid
,count(distinct case when prod.voters_statevoterid = dev.voters_statevoterid then prod.voters_statevoterid end) as matched_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 = dev.sos_join_key_l2 then prod.sos_join_key_l2 end) as matched_sos_join_key_l2
,count(distinct case when prod.l2_precinct = dev.l2_precinct then prod.l2_precinct end) as matched_l2_precinct
,count(distinct case when prod.person_id = dev.person_id then prod.person_id end) as matched_person_id
,count(distinct case when prod.voterbase_id = dev.voterbase_id then prod.voterbase_id end) as matched_voterbase_id
,count(distinct case when prod.dnc_precinct_id = dev.dnc_precinct_id then prod.dnc_precinct_id end) as matched_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id = dev.dnc_sos_id then prod.dnc_sos_id end) as matched_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc = dev.sos_join_key_dnc then prod.sos_join_key_dnc end) as matched_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id = dev.dnc_county_voter_id then prod.dnc_county_voter_id end) as matched_dnc_county_voter_id
,count(distinct case when prod.county_fips = dev.county_fips then prod.county_fips end) as matched_county_fips
,count(distinct case when prod.pdiid = dev.pdiid then prod.pdiid end) as matched_pdiid
,count(distinct case when prod.pdi_sos_id = dev.pdi_sos_id then prod.pdi_sos_id end) as matched_pdi_sos_id
,count(distinct case when prod.pdi_precinct = dev.pdi_precinct then prod.pdi_precinct end) as matched_pdi_precinct
,count(distinct case when prod.actionkit_id = dev.actionkit_id then prod.actionkit_id end) as matched_actionkit_id
,count(distinct case when prod.ak_updated_at = dev.ak_updated_at then prod.ak_updated_at end) as matched_ak_updated_at
,count(distinct case when prod.bern_id = dev.bern_id then prod.bern_id end) as matched_bern_id
,count(distinct case when prod.bern_canvasser_id = dev.bern_canvasser_id then prod.bern_canvasser_id end) as matched_bern_canvasser_id
,count(distinct case when prod.myc_van_id = dev.myc_van_id then prod.myc_van_id end) as matched_myc_van_id
,count(distinct case when prod.st_myc_van_id = dev.st_myc_van_id then prod.st_myc_van_id end) as matched_st_myc_van_id
,count(distinct case when prod.myc_dupe = dev.myc_dupe then prod.myc_dupe end) as matched_myc_dupe
,count(distinct case when prod.myc_voter_type_id = dev.myc_voter_type_id then prod.myc_voter_type_id end) as matched_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked = dev.myc_myv_linked then prod.myc_myv_linked end) as matched_myc_myv_linked
,count(distinct case when prod.myv_van_id = dev.myv_van_id then prod.myv_van_id end) as matched_myv_van_id
,count(distinct case when prod.st_myv_van_id = dev.st_myv_van_id then prod.st_myv_van_id end) as matched_st_myv_van_id
,count(distinct case when prod.email = dev.email then prod.email end) as matched_email
,count(distinct case when prod.aux_myc_email = dev.aux_myc_email then prod.aux_myc_email end) as matched_aux_myc_email
,count(distinct case when prod.student_hash = dev.student_hash then prod.student_hash end) as matched_student_hash
,count(distinct case when prod.student_rn = dev.student_rn then prod.student_rn end) as matched_student_rn
,count(distinct case when prod.state_code = dev.state_code then prod.state_code end) as matched_state_code

,count(distinct case when prod.lalvoterid is not null then prod.lalvoterid end) as prod_distinct_lalvoterid
,count(distinct case when prod.voters_statevoterid is not null then prod.voters_statevoterid end) as prod_distinct_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 is not null then prod.sos_join_key_l2 end) as prod_distinct_sos_join_key_l2
,count(distinct case when prod.l2_precinct is not null then prod.l2_precinct end) as prod_distinct_l2_precinct
,count(distinct case when prod.person_id is not null then prod.person_id end) as prod_distinct_person_id
,count(distinct case when prod.voterbase_id is not null then prod.voterbase_id end) as prod_distinct_voterbase_id
,count(distinct case when prod.dnc_precinct_id is not null then prod.dnc_precinct_id end) as prod_distinct_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id is not null then prod.dnc_sos_id end) as prod_distinct_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc is not null then prod.sos_join_key_dnc end) as prod_distinct_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id is not null then prod.dnc_county_voter_id end) as prod_distinct_dnc_county_voter_id
,count(distinct case when prod.county_fips is not null then prod.county_fips end) as prod_distinct_county_fips
,count(distinct case when prod.pdiid is not null then prod.pdiid end) as prod_distinct_pdiid
,count(distinct case when prod.pdi_sos_id is not null then prod.pdi_sos_id end) as prod_distinct_pdi_sos_id
,count(distinct case when prod.pdi_precinct is not null then prod.pdi_precinct end) as prod_distinct_pdi_precinct
,count(distinct case when prod.actionkit_id is not null then prod.actionkit_id end) as prod_distinct_actionkit_id
,count(distinct case when prod.ak_updated_at is not null then prod.ak_updated_at end) as prod_distinct_ak_updated_at
,count(distinct case when prod.bern_id is not null then prod.bern_id end) as prod_distinct_bern_id
,count(distinct case when prod.bern_canvasser_id is not null then prod.bern_canvasser_id end) as prod_distinct_bern_canvasser_id
,count(distinct case when prod.myc_van_id is not null then prod.myc_van_id end) as prod_distinct_myc_van_id
,count(distinct case when prod.st_myc_van_id is not null then prod.st_myc_van_id end) as prod_distinct_st_myc_van_id
,count(distinct case when prod.myc_dupe is not null then prod.myc_dupe end) as prod_distinct_myc_dupe
,count(distinct case when prod.myc_voter_type_id is not null then prod.myc_voter_type_id end) as prod_distinct_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked is not null then prod.myc_myv_linked end) as prod_distinct_myc_myv_linked
,count(distinct case when prod.myv_van_id is not null then prod.myv_van_id end) as prod_distinct_myv_van_id
,count(distinct case when prod.st_myv_van_id is not null then prod.st_myv_van_id end) as prod_distinct_st_myv_van_id
,count(distinct case when prod.email is not null then prod.email end) as prod_distinct_email
,count(distinct case when prod.aux_myc_email is not null then prod.aux_myc_email end) as prod_distinct_aux_myc_email
,count(distinct case when prod.student_hash is not null then prod.student_hash end) as prod_distinct_student_hash
,count(distinct case when prod.student_rn is not null then prod.student_rn end) as prod_distinct_student_rn
,count(distinct case when prod.state_code is not null then prod.state_code end) as prod_distinct_state_code

,count(distinct case when dev.lalvoterid is not null then dev.lalvoterid end) as dev_distinct_lalvoterid
,count(distinct case when dev.voters_statevoterid is not null then dev.voters_statevoterid end) as dev_distinct_voters_statevoterid
,count(distinct case when dev.sos_join_key_l2 is not null then dev.sos_join_key_l2 end) as dev_distinct_sos_join_key_l2
,count(distinct case when dev.l2_precinct is not null then dev.l2_precinct end) as dev_distinct_l2_precinct
,count(distinct case when dev.person_id is not null then dev.person_id end) as dev_distinct_person_id
,count(distinct case when dev.voterbase_id is not null then dev.voterbase_id end) as dev_distinct_voterbase_id
,count(distinct case when dev.dnc_precinct_id is not null then dev.dnc_precinct_id end) as dev_distinct_dnc_precinct_id
,count(distinct case when dev.dnc_sos_id is not null then dev.dnc_sos_id end) as dev_distinct_dnc_sos_id
,count(distinct case when dev.sos_join_key_dnc is not null then dev.sos_join_key_dnc end) as dev_distinct_sos_join_key_dnc
,count(distinct case when dev.dnc_county_voter_id is not null then dev.dnc_county_voter_id end) as dev_distinct_dnc_county_voter_id
,count(distinct case when dev.county_fips is not null then dev.county_fips end) as dev_distinct_county_fips
,count(distinct case when dev.pdiid is not null then dev.pdiid end) as dev_distinct_pdiid
,count(distinct case when dev.pdi_sos_id is not null then dev.pdi_sos_id end) as dev_distinct_pdi_sos_id
,count(distinct case when dev.pdi_precinct is not null then dev.pdi_precinct end) as dev_distinct_pdi_precinct
,count(distinct case when dev.actionkit_id is not null then dev.actionkit_id end) as dev_distinct_actionkit_id
,count(distinct case when dev.ak_updated_at is not null then dev.ak_updated_at end) as dev_distinct_ak_updated_at
,count(distinct case when dev.bern_id is not null then dev.bern_id end) as dev_distinct_bern_id
,count(distinct case when dev.bern_canvasser_id is not null then dev.bern_canvasser_id end) as dev_distinct_bern_canvasser_id
,count(distinct case when dev.myc_van_id is not null then dev.myc_van_id end) as dev_distinct_myc_van_id
,count(distinct case when dev.st_myc_van_id is not null then dev.st_myc_van_id end) as dev_distinct_st_myc_van_id
,count(distinct case when dev.myc_dupe is not null then dev.myc_dupe end) as dev_distinct_myc_dupe
,count(distinct case when dev.myc_voter_type_id is not null then dev.myc_voter_type_id end) as dev_distinct_myc_voter_type_id
,count(distinct case when dev.myc_myv_linked is not null then dev.myc_myv_linked end) as dev_distinct_myc_myv_linked
,count(distinct case when dev.myv_van_id is not null then dev.myv_van_id end) as dev_distinct_myv_van_id
,count(distinct case when dev.st_myv_van_id is not null then dev.st_myv_van_id end) as dev_distinct_st_myv_van_id
,count(distinct case when dev.email is not null then dev.email end) as dev_distinct_email
,count(distinct case when dev.aux_myc_email is not null then dev.aux_myc_email end) as dev_distinct_aux_myc_email
,count(distinct case when dev.student_hash is not null then dev.student_hash end) as dev_distinct_student_hash
,count(distinct case when dev.student_rn is not null then dev.student_rn end) as dev_distinct_student_rn
,count(distinct case when dev.state_code is not null then dev.state_code end) as dev_distinct_state_code

,count(*)
from 
bernie_data_commons.master_xwalk_dnc prod
left outer join 
bernie_nmarchio2.master_xwalk_dnc_20200224 dev
using(person_id)
where dev.person_id is null
group by 1,2)
union all
(select 
 'lalvoterid' as id_type
,'left outer join' as join_type 
,count(distinct case when prod.lalvoterid = dev.lalvoterid then prod.lalvoterid end) as matched_lalvoterid
,count(distinct case when prod.voters_statevoterid = dev.voters_statevoterid then prod.voters_statevoterid end) as matched_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 = dev.sos_join_key_l2 then prod.sos_join_key_l2 end) as matched_sos_join_key_l2
,count(distinct case when prod.l2_precinct = dev.l2_precinct then prod.l2_precinct end) as matched_l2_precinct
,count(distinct case when prod.person_id = dev.person_id then prod.person_id end) as matched_person_id
,count(distinct case when prod.voterbase_id = dev.voterbase_id then prod.voterbase_id end) as matched_voterbase_id
,count(distinct case when prod.dnc_precinct_id = dev.dnc_precinct_id then prod.dnc_precinct_id end) as matched_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id = dev.dnc_sos_id then prod.dnc_sos_id end) as matched_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc = dev.sos_join_key_dnc then prod.sos_join_key_dnc end) as matched_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id = dev.dnc_county_voter_id then prod.dnc_county_voter_id end) as matched_dnc_county_voter_id
,count(distinct case when prod.county_fips = dev.county_fips then prod.county_fips end) as matched_county_fips
,count(distinct case when prod.pdiid = dev.pdiid then prod.pdiid end) as matched_pdiid
,count(distinct case when prod.pdi_sos_id = dev.pdi_sos_id then prod.pdi_sos_id end) as matched_pdi_sos_id
,count(distinct case when prod.pdi_precinct = dev.pdi_precinct then prod.pdi_precinct end) as matched_pdi_precinct
,count(distinct case when prod.actionkit_id = dev.actionkit_id then prod.actionkit_id end) as matched_actionkit_id
,count(distinct case when prod.ak_updated_at = dev.ak_updated_at then prod.ak_updated_at end) as matched_ak_updated_at
,count(distinct case when prod.bern_id = dev.bern_id then prod.bern_id end) as matched_bern_id
,count(distinct case when prod.bern_canvasser_id = dev.bern_canvasser_id then prod.bern_canvasser_id end) as matched_bern_canvasser_id
,count(distinct case when prod.myc_van_id = dev.myc_van_id then prod.myc_van_id end) as matched_myc_van_id
,count(distinct case when prod.st_myc_van_id = dev.st_myc_van_id then prod.st_myc_van_id end) as matched_st_myc_van_id
,count(distinct case when prod.myc_dupe = dev.myc_dupe then prod.myc_dupe end) as matched_myc_dupe
,count(distinct case when prod.myc_voter_type_id = dev.myc_voter_type_id then prod.myc_voter_type_id end) as matched_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked = dev.myc_myv_linked then prod.myc_myv_linked end) as matched_myc_myv_linked
,count(distinct case when prod.myv_van_id = dev.myv_van_id then prod.myv_van_id end) as matched_myv_van_id
,count(distinct case when prod.st_myv_van_id = dev.st_myv_van_id then prod.st_myv_van_id end) as matched_st_myv_van_id
,count(distinct case when prod.email = dev.email then prod.email end) as matched_email
,count(distinct case when prod.aux_myc_email = dev.aux_myc_email then prod.aux_myc_email end) as matched_aux_myc_email
,count(distinct case when prod.student_hash = dev.student_hash then prod.student_hash end) as matched_student_hash
,count(distinct case when prod.student_rn = dev.student_rn then prod.student_rn end) as matched_student_rn
,count(distinct case when prod.state_code = dev.state_code then prod.state_code end) as matched_state_code

,count(distinct case when prod.lalvoterid is not null then prod.lalvoterid end) as prod_distinct_lalvoterid
,count(distinct case when prod.voters_statevoterid is not null then prod.voters_statevoterid end) as prod_distinct_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 is not null then prod.sos_join_key_l2 end) as prod_distinct_sos_join_key_l2
,count(distinct case when prod.l2_precinct is not null then prod.l2_precinct end) as prod_distinct_l2_precinct
,count(distinct case when prod.person_id is not null then prod.person_id end) as prod_distinct_person_id
,count(distinct case when prod.voterbase_id is not null then prod.voterbase_id end) as prod_distinct_voterbase_id
,count(distinct case when prod.dnc_precinct_id is not null then prod.dnc_precinct_id end) as prod_distinct_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id is not null then prod.dnc_sos_id end) as prod_distinct_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc is not null then prod.sos_join_key_dnc end) as prod_distinct_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id is not null then prod.dnc_county_voter_id end) as prod_distinct_dnc_county_voter_id
,count(distinct case when prod.county_fips is not null then prod.county_fips end) as prod_distinct_county_fips
,count(distinct case when prod.pdiid is not null then prod.pdiid end) as prod_distinct_pdiid
,count(distinct case when prod.pdi_sos_id is not null then prod.pdi_sos_id end) as prod_distinct_pdi_sos_id
,count(distinct case when prod.pdi_precinct is not null then prod.pdi_precinct end) as prod_distinct_pdi_precinct
,count(distinct case when prod.actionkit_id is not null then prod.actionkit_id end) as prod_distinct_actionkit_id
,count(distinct case when prod.ak_updated_at is not null then prod.ak_updated_at end) as prod_distinct_ak_updated_at
,count(distinct case when prod.bern_id is not null then prod.bern_id end) as prod_distinct_bern_id
,count(distinct case when prod.bern_canvasser_id is not null then prod.bern_canvasser_id end) as prod_distinct_bern_canvasser_id
,count(distinct case when prod.myc_van_id is not null then prod.myc_van_id end) as prod_distinct_myc_van_id
,count(distinct case when prod.st_myc_van_id is not null then prod.st_myc_van_id end) as prod_distinct_st_myc_van_id
,count(distinct case when prod.myc_dupe is not null then prod.myc_dupe end) as prod_distinct_myc_dupe
,count(distinct case when prod.myc_voter_type_id is not null then prod.myc_voter_type_id end) as prod_distinct_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked is not null then prod.myc_myv_linked end) as prod_distinct_myc_myv_linked
,count(distinct case when prod.myv_van_id is not null then prod.myv_van_id end) as prod_distinct_myv_van_id
,count(distinct case when prod.st_myv_van_id is not null then prod.st_myv_van_id end) as prod_distinct_st_myv_van_id
,count(distinct case when prod.email is not null then prod.email end) as prod_distinct_email
,count(distinct case when prod.aux_myc_email is not null then prod.aux_myc_email end) as prod_distinct_aux_myc_email
,count(distinct case when prod.student_hash is not null then prod.student_hash end) as prod_distinct_student_hash
,count(distinct case when prod.student_rn is not null then prod.student_rn end) as prod_distinct_student_rn
,count(distinct case when prod.state_code is not null then prod.state_code end) as prod_distinct_state_code

,count(distinct case when dev.lalvoterid is not null then dev.lalvoterid end) as dev_distinct_lalvoterid
,count(distinct case when dev.voters_statevoterid is not null then dev.voters_statevoterid end) as dev_distinct_voters_statevoterid
,count(distinct case when dev.sos_join_key_l2 is not null then dev.sos_join_key_l2 end) as dev_distinct_sos_join_key_l2
,count(distinct case when dev.l2_precinct is not null then dev.l2_precinct end) as dev_distinct_l2_precinct
,count(distinct case when dev.person_id is not null then dev.person_id end) as dev_distinct_person_id
,count(distinct case when dev.voterbase_id is not null then dev.voterbase_id end) as dev_distinct_voterbase_id
,count(distinct case when dev.dnc_precinct_id is not null then dev.dnc_precinct_id end) as dev_distinct_dnc_precinct_id
,count(distinct case when dev.dnc_sos_id is not null then dev.dnc_sos_id end) as dev_distinct_dnc_sos_id
,count(distinct case when dev.sos_join_key_dnc is not null then dev.sos_join_key_dnc end) as dev_distinct_sos_join_key_dnc
,count(distinct case when dev.dnc_county_voter_id is not null then dev.dnc_county_voter_id end) as dev_distinct_dnc_county_voter_id
,count(distinct case when dev.county_fips is not null then dev.county_fips end) as dev_distinct_county_fips
,count(distinct case when dev.pdiid is not null then dev.pdiid end) as dev_distinct_pdiid
,count(distinct case when dev.pdi_sos_id is not null then dev.pdi_sos_id end) as dev_distinct_pdi_sos_id
,count(distinct case when dev.pdi_precinct is not null then dev.pdi_precinct end) as dev_distinct_pdi_precinct
,count(distinct case when dev.actionkit_id is not null then dev.actionkit_id end) as dev_distinct_actionkit_id
,count(distinct case when dev.ak_updated_at is not null then dev.ak_updated_at end) as dev_distinct_ak_updated_at
,count(distinct case when dev.bern_id is not null then dev.bern_id end) as dev_distinct_bern_id
,count(distinct case when dev.bern_canvasser_id is not null then dev.bern_canvasser_id end) as dev_distinct_bern_canvasser_id
,count(distinct case when dev.myc_van_id is not null then dev.myc_van_id end) as dev_distinct_myc_van_id
,count(distinct case when dev.st_myc_van_id is not null then dev.st_myc_van_id end) as dev_distinct_st_myc_van_id
,count(distinct case when dev.myc_dupe is not null then dev.myc_dupe end) as dev_distinct_myc_dupe
,count(distinct case when dev.myc_voter_type_id is not null then dev.myc_voter_type_id end) as dev_distinct_myc_voter_type_id
,count(distinct case when dev.myc_myv_linked is not null then dev.myc_myv_linked end) as dev_distinct_myc_myv_linked
,count(distinct case when dev.myv_van_id is not null then dev.myv_van_id end) as dev_distinct_myv_van_id
,count(distinct case when dev.st_myv_van_id is not null then dev.st_myv_van_id end) as dev_distinct_st_myv_van_id
,count(distinct case when dev.email is not null then dev.email end) as dev_distinct_email
,count(distinct case when dev.aux_myc_email is not null then dev.aux_myc_email end) as dev_distinct_aux_myc_email
,count(distinct case when dev.student_hash is not null then dev.student_hash end) as dev_distinct_student_hash
,count(distinct case when dev.student_rn is not null then dev.student_rn end) as dev_distinct_student_rn
,count(distinct case when dev.state_code is not null then dev.state_code end) as dev_distinct_state_code

,count(*)
from 
bernie_data_commons.master_xwalk_lal prod
left outer join 
bernie_nmarchio2.master_xwalk_lal_20200224 dev
using(lalvoterid)
where dev.lalvoterid is null
group by 1,2)
union all
(select 
 'pdiid' as id_type
,'left outer join' as join_type 
,count(distinct case when prod.lalvoterid = dev.lalvoterid then prod.lalvoterid end) as matched_lalvoterid
,count(distinct case when prod.voters_statevoterid = dev.voters_statevoterid then prod.voters_statevoterid end) as matched_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 = dev.sos_join_key_l2 then prod.sos_join_key_l2 end) as matched_sos_join_key_l2
,count(distinct case when prod.l2_precinct = dev.l2_precinct then prod.l2_precinct end) as matched_l2_precinct
,count(distinct case when prod.person_id = dev.person_id then prod.person_id end) as matched_person_id
,count(distinct case when prod.voterbase_id = dev.voterbase_id then prod.voterbase_id end) as matched_voterbase_id
,count(distinct case when prod.dnc_precinct_id = dev.dnc_precinct_id then prod.dnc_precinct_id end) as matched_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id = dev.dnc_sos_id then prod.dnc_sos_id end) as matched_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc = dev.sos_join_key_dnc then prod.sos_join_key_dnc end) as matched_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id = dev.dnc_county_voter_id then prod.dnc_county_voter_id end) as matched_dnc_county_voter_id
,count(distinct case when prod.county_fips = dev.county_fips then prod.county_fips end) as matched_county_fips
,count(distinct case when prod.pdiid = dev.pdiid then prod.pdiid end) as matched_pdiid
,count(distinct case when prod.pdi_sos_id = dev.pdi_sos_id then prod.pdi_sos_id end) as matched_pdi_sos_id
,count(distinct case when prod.pdi_precinct = dev.pdi_precinct then prod.pdi_precinct end) as matched_pdi_precinct
,count(distinct case when prod.actionkit_id = dev.actionkit_id then prod.actionkit_id end) as matched_actionkit_id
,count(distinct case when prod.ak_updated_at = dev.ak_updated_at then prod.ak_updated_at end) as matched_ak_updated_at
,count(distinct case when prod.bern_id = dev.bern_id then prod.bern_id end) as matched_bern_id
,count(distinct case when prod.bern_canvasser_id = dev.bern_canvasser_id then prod.bern_canvasser_id end) as matched_bern_canvasser_id
,count(distinct case when prod.myc_van_id = dev.myc_van_id then prod.myc_van_id end) as matched_myc_van_id
,count(distinct case when prod.st_myc_van_id = dev.st_myc_van_id then prod.st_myc_van_id end) as matched_st_myc_van_id
,count(distinct case when prod.myc_dupe = dev.myc_dupe then prod.myc_dupe end) as matched_myc_dupe
,count(distinct case when prod.myc_voter_type_id = dev.myc_voter_type_id then prod.myc_voter_type_id end) as matched_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked = dev.myc_myv_linked then prod.myc_myv_linked end) as matched_myc_myv_linked
,count(distinct case when prod.myv_van_id = dev.myv_van_id then prod.myv_van_id end) as matched_myv_van_id
,count(distinct case when prod.st_myv_van_id = dev.st_myv_van_id then prod.st_myv_van_id end) as matched_st_myv_van_id
,count(distinct case when prod.email = dev.email then prod.email end) as matched_email
,count(distinct case when prod.aux_myc_email = dev.aux_myc_email then prod.aux_myc_email end) as matched_aux_myc_email
,count(distinct case when prod.student_hash = dev.student_hash then prod.student_hash end) as matched_student_hash
,count(distinct case when prod.student_rn = dev.student_rn then prod.student_rn end) as matched_student_rn
,count(distinct case when prod.state_code = dev.state_code then prod.state_code end) as matched_state_code

,count(distinct case when prod.lalvoterid is not null then prod.lalvoterid end) as prod_distinct_lalvoterid
,count(distinct case when prod.voters_statevoterid is not null then prod.voters_statevoterid end) as prod_distinct_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 is not null then prod.sos_join_key_l2 end) as prod_distinct_sos_join_key_l2
,count(distinct case when prod.l2_precinct is not null then prod.l2_precinct end) as prod_distinct_l2_precinct
,count(distinct case when prod.person_id is not null then prod.person_id end) as prod_distinct_person_id
,count(distinct case when prod.voterbase_id is not null then prod.voterbase_id end) as prod_distinct_voterbase_id
,count(distinct case when prod.dnc_precinct_id is not null then prod.dnc_precinct_id end) as prod_distinct_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id is not null then prod.dnc_sos_id end) as prod_distinct_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc is not null then prod.sos_join_key_dnc end) as prod_distinct_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id is not null then prod.dnc_county_voter_id end) as prod_distinct_dnc_county_voter_id
,count(distinct case when prod.county_fips is not null then prod.county_fips end) as prod_distinct_county_fips
,count(distinct case when prod.pdiid is not null then prod.pdiid end) as prod_distinct_pdiid
,count(distinct case when prod.pdi_sos_id is not null then prod.pdi_sos_id end) as prod_distinct_pdi_sos_id
,count(distinct case when prod.pdi_precinct is not null then prod.pdi_precinct end) as prod_distinct_pdi_precinct
,count(distinct case when prod.actionkit_id is not null then prod.actionkit_id end) as prod_distinct_actionkit_id
,count(distinct case when prod.ak_updated_at is not null then prod.ak_updated_at end) as prod_distinct_ak_updated_at
,count(distinct case when prod.bern_id is not null then prod.bern_id end) as prod_distinct_bern_id
,count(distinct case when prod.bern_canvasser_id is not null then prod.bern_canvasser_id end) as prod_distinct_bern_canvasser_id
,count(distinct case when prod.myc_van_id is not null then prod.myc_van_id end) as prod_distinct_myc_van_id
,count(distinct case when prod.st_myc_van_id is not null then prod.st_myc_van_id end) as prod_distinct_st_myc_van_id
,count(distinct case when prod.myc_dupe is not null then prod.myc_dupe end) as prod_distinct_myc_dupe
,count(distinct case when prod.myc_voter_type_id is not null then prod.myc_voter_type_id end) as prod_distinct_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked is not null then prod.myc_myv_linked end) as prod_distinct_myc_myv_linked
,count(distinct case when prod.myv_van_id is not null then prod.myv_van_id end) as prod_distinct_myv_van_id
,count(distinct case when prod.st_myv_van_id is not null then prod.st_myv_van_id end) as prod_distinct_st_myv_van_id
,count(distinct case when prod.email is not null then prod.email end) as prod_distinct_email
,count(distinct case when prod.aux_myc_email is not null then prod.aux_myc_email end) as prod_distinct_aux_myc_email
,count(distinct case when prod.student_hash is not null then prod.student_hash end) as prod_distinct_student_hash
,count(distinct case when prod.student_rn is not null then prod.student_rn end) as prod_distinct_student_rn
,count(distinct case when prod.state_code is not null then prod.state_code end) as prod_distinct_state_code

,count(distinct case when dev.lalvoterid is not null then dev.lalvoterid end) as dev_distinct_lalvoterid
,count(distinct case when dev.voters_statevoterid is not null then dev.voters_statevoterid end) as dev_distinct_voters_statevoterid
,count(distinct case when dev.sos_join_key_l2 is not null then dev.sos_join_key_l2 end) as dev_distinct_sos_join_key_l2
,count(distinct case when dev.l2_precinct is not null then dev.l2_precinct end) as dev_distinct_l2_precinct
,count(distinct case when dev.person_id is not null then dev.person_id end) as dev_distinct_person_id
,count(distinct case when dev.voterbase_id is not null then dev.voterbase_id end) as dev_distinct_voterbase_id
,count(distinct case when dev.dnc_precinct_id is not null then dev.dnc_precinct_id end) as dev_distinct_dnc_precinct_id
,count(distinct case when dev.dnc_sos_id is not null then dev.dnc_sos_id end) as dev_distinct_dnc_sos_id
,count(distinct case when dev.sos_join_key_dnc is not null then dev.sos_join_key_dnc end) as dev_distinct_sos_join_key_dnc
,count(distinct case when dev.dnc_county_voter_id is not null then dev.dnc_county_voter_id end) as dev_distinct_dnc_county_voter_id
,count(distinct case when dev.county_fips is not null then dev.county_fips end) as dev_distinct_county_fips
,count(distinct case when dev.pdiid is not null then dev.pdiid end) as dev_distinct_pdiid
,count(distinct case when dev.pdi_sos_id is not null then dev.pdi_sos_id end) as dev_distinct_pdi_sos_id
,count(distinct case when dev.pdi_precinct is not null then dev.pdi_precinct end) as dev_distinct_pdi_precinct
,count(distinct case when dev.actionkit_id is not null then dev.actionkit_id end) as dev_distinct_actionkit_id
,count(distinct case when dev.ak_updated_at is not null then dev.ak_updated_at end) as dev_distinct_ak_updated_at
,count(distinct case when dev.bern_id is not null then dev.bern_id end) as dev_distinct_bern_id
,count(distinct case when dev.bern_canvasser_id is not null then dev.bern_canvasser_id end) as dev_distinct_bern_canvasser_id
,count(distinct case when dev.myc_van_id is not null then dev.myc_van_id end) as dev_distinct_myc_van_id
,count(distinct case when dev.st_myc_van_id is not null then dev.st_myc_van_id end) as dev_distinct_st_myc_van_id
,count(distinct case when dev.myc_dupe is not null then dev.myc_dupe end) as dev_distinct_myc_dupe
,count(distinct case when dev.myc_voter_type_id is not null then dev.myc_voter_type_id end) as dev_distinct_myc_voter_type_id
,count(distinct case when dev.myc_myv_linked is not null then dev.myc_myv_linked end) as dev_distinct_myc_myv_linked
,count(distinct case when dev.myv_van_id is not null then dev.myv_van_id end) as dev_distinct_myv_van_id
,count(distinct case when dev.st_myv_van_id is not null then dev.st_myv_van_id end) as dev_distinct_st_myv_van_id
,count(distinct case when dev.email is not null then dev.email end) as dev_distinct_email
,count(distinct case when dev.aux_myc_email is not null then dev.aux_myc_email end) as dev_distinct_aux_myc_email
,count(distinct case when dev.student_hash is not null then dev.student_hash end) as dev_distinct_student_hash
,count(distinct case when dev.student_rn is not null then dev.student_rn end) as dev_distinct_student_rn
,count(distinct case when dev.state_code is not null then dev.state_code end) as dev_distinct_state_code

,count(*)
from 
bernie_data_commons.master_xwalk_pdi prod
left outer join 
bernie_nmarchio2.master_xwalk_pdi_20200224 dev
using(pdiid)
where dev.pdiid is null
group by 1,2)
union all
(select 
 'st_myc_van_id' as id_type
,'left outer join' as join_type 
,count(distinct case when prod.lalvoterid = dev.lalvoterid then prod.lalvoterid end) as matched_lalvoterid
,count(distinct case when prod.voters_statevoterid = dev.voters_statevoterid then prod.voters_statevoterid end) as matched_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 = dev.sos_join_key_l2 then prod.sos_join_key_l2 end) as matched_sos_join_key_l2
,count(distinct case when prod.l2_precinct = dev.l2_precinct then prod.l2_precinct end) as matched_l2_precinct
,count(distinct case when prod.person_id = dev.person_id then prod.person_id end) as matched_person_id
,count(distinct case when prod.voterbase_id = dev.voterbase_id then prod.voterbase_id end) as matched_voterbase_id
,count(distinct case when prod.dnc_precinct_id = dev.dnc_precinct_id then prod.dnc_precinct_id end) as matched_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id = dev.dnc_sos_id then prod.dnc_sos_id end) as matched_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc = dev.sos_join_key_dnc then prod.sos_join_key_dnc end) as matched_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id = dev.dnc_county_voter_id then prod.dnc_county_voter_id end) as matched_dnc_county_voter_id
,count(distinct case when prod.county_fips = dev.county_fips then prod.county_fips end) as matched_county_fips
,count(distinct case when prod.pdiid = dev.pdiid then prod.pdiid end) as matched_pdiid
,count(distinct case when prod.pdi_sos_id = dev.pdi_sos_id then prod.pdi_sos_id end) as matched_pdi_sos_id
,count(distinct case when prod.pdi_precinct = dev.pdi_precinct then prod.pdi_precinct end) as matched_pdi_precinct
,count(distinct case when prod.actionkit_id = dev.actionkit_id then prod.actionkit_id end) as matched_actionkit_id
,count(distinct case when prod.ak_updated_at = dev.ak_updated_at then prod.ak_updated_at end) as matched_ak_updated_at
,count(distinct case when prod.bern_id = dev.bern_id then prod.bern_id end) as matched_bern_id
,count(distinct case when prod.bern_canvasser_id = dev.bern_canvasser_id then prod.bern_canvasser_id end) as matched_bern_canvasser_id
,count(distinct case when prod.myc_van_id = dev.myc_van_id then prod.myc_van_id end) as matched_myc_van_id
,count(distinct case when prod.st_myc_van_id = dev.st_myc_van_id then prod.st_myc_van_id end) as matched_st_myc_van_id
,count(distinct case when prod.myc_dupe = dev.myc_dupe then prod.myc_dupe end) as matched_myc_dupe
,count(distinct case when prod.myc_voter_type_id = dev.myc_voter_type_id then prod.myc_voter_type_id end) as matched_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked = dev.myc_myv_linked then prod.myc_myv_linked end) as matched_myc_myv_linked
,count(distinct case when prod.myv_van_id = dev.myv_van_id then prod.myv_van_id end) as matched_myv_van_id
,count(distinct case when prod.st_myv_van_id = dev.st_myv_van_id then prod.st_myv_van_id end) as matched_st_myv_van_id
,count(distinct case when prod.email = dev.email then prod.email end) as matched_email
,count(distinct case when prod.aux_myc_email = dev.aux_myc_email then prod.aux_myc_email end) as matched_aux_myc_email
,count(distinct case when prod.student_hash = dev.student_hash then prod.student_hash end) as matched_student_hash
,count(distinct case when prod.student_rn = dev.student_rn then prod.student_rn end) as matched_student_rn
,count(distinct case when prod.state_code = dev.state_code then prod.state_code end) as matched_state_code

,count(distinct case when prod.lalvoterid is not null then prod.lalvoterid end) as prod_distinct_lalvoterid
,count(distinct case when prod.voters_statevoterid is not null then prod.voters_statevoterid end) as prod_distinct_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 is not null then prod.sos_join_key_l2 end) as prod_distinct_sos_join_key_l2
,count(distinct case when prod.l2_precinct is not null then prod.l2_precinct end) as prod_distinct_l2_precinct
,count(distinct case when prod.person_id is not null then prod.person_id end) as prod_distinct_person_id
,count(distinct case when prod.voterbase_id is not null then prod.voterbase_id end) as prod_distinct_voterbase_id
,count(distinct case when prod.dnc_precinct_id is not null then prod.dnc_precinct_id end) as prod_distinct_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id is not null then prod.dnc_sos_id end) as prod_distinct_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc is not null then prod.sos_join_key_dnc end) as prod_distinct_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id is not null then prod.dnc_county_voter_id end) as prod_distinct_dnc_county_voter_id
,count(distinct case when prod.county_fips is not null then prod.county_fips end) as prod_distinct_county_fips
,count(distinct case when prod.pdiid is not null then prod.pdiid end) as prod_distinct_pdiid
,count(distinct case when prod.pdi_sos_id is not null then prod.pdi_sos_id end) as prod_distinct_pdi_sos_id
,count(distinct case when prod.pdi_precinct is not null then prod.pdi_precinct end) as prod_distinct_pdi_precinct
,count(distinct case when prod.actionkit_id is not null then prod.actionkit_id end) as prod_distinct_actionkit_id
,count(distinct case when prod.ak_updated_at is not null then prod.ak_updated_at end) as prod_distinct_ak_updated_at
,count(distinct case when prod.bern_id is not null then prod.bern_id end) as prod_distinct_bern_id
,count(distinct case when prod.bern_canvasser_id is not null then prod.bern_canvasser_id end) as prod_distinct_bern_canvasser_id
,count(distinct case when prod.myc_van_id is not null then prod.myc_van_id end) as prod_distinct_myc_van_id
,count(distinct case when prod.st_myc_van_id is not null then prod.st_myc_van_id end) as prod_distinct_st_myc_van_id
,count(distinct case when prod.myc_dupe is not null then prod.myc_dupe end) as prod_distinct_myc_dupe
,count(distinct case when prod.myc_voter_type_id is not null then prod.myc_voter_type_id end) as prod_distinct_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked is not null then prod.myc_myv_linked end) as prod_distinct_myc_myv_linked
,count(distinct case when prod.myv_van_id is not null then prod.myv_van_id end) as prod_distinct_myv_van_id
,count(distinct case when prod.st_myv_van_id is not null then prod.st_myv_van_id end) as prod_distinct_st_myv_van_id
,count(distinct case when prod.email is not null then prod.email end) as prod_distinct_email
,count(distinct case when prod.aux_myc_email is not null then prod.aux_myc_email end) as prod_distinct_aux_myc_email
,count(distinct case when prod.student_hash is not null then prod.student_hash end) as prod_distinct_student_hash
,count(distinct case when prod.student_rn is not null then prod.student_rn end) as prod_distinct_student_rn
,count(distinct case when prod.state_code is not null then prod.state_code end) as prod_distinct_state_code

,count(distinct case when dev.lalvoterid is not null then dev.lalvoterid end) as dev_distinct_lalvoterid
,count(distinct case when dev.voters_statevoterid is not null then dev.voters_statevoterid end) as dev_distinct_voters_statevoterid
,count(distinct case when dev.sos_join_key_l2 is not null then dev.sos_join_key_l2 end) as dev_distinct_sos_join_key_l2
,count(distinct case when dev.l2_precinct is not null then dev.l2_precinct end) as dev_distinct_l2_precinct
,count(distinct case when dev.person_id is not null then dev.person_id end) as dev_distinct_person_id
,count(distinct case when dev.voterbase_id is not null then dev.voterbase_id end) as dev_distinct_voterbase_id
,count(distinct case when dev.dnc_precinct_id is not null then dev.dnc_precinct_id end) as dev_distinct_dnc_precinct_id
,count(distinct case when dev.dnc_sos_id is not null then dev.dnc_sos_id end) as dev_distinct_dnc_sos_id
,count(distinct case when dev.sos_join_key_dnc is not null then dev.sos_join_key_dnc end) as dev_distinct_sos_join_key_dnc
,count(distinct case when dev.dnc_county_voter_id is not null then dev.dnc_county_voter_id end) as dev_distinct_dnc_county_voter_id
,count(distinct case when dev.county_fips is not null then dev.county_fips end) as dev_distinct_county_fips
,count(distinct case when dev.pdiid is not null then dev.pdiid end) as dev_distinct_pdiid
,count(distinct case when dev.pdi_sos_id is not null then dev.pdi_sos_id end) as dev_distinct_pdi_sos_id
,count(distinct case when dev.pdi_precinct is not null then dev.pdi_precinct end) as dev_distinct_pdi_precinct
,count(distinct case when dev.actionkit_id is not null then dev.actionkit_id end) as dev_distinct_actionkit_id
,count(distinct case when dev.ak_updated_at is not null then dev.ak_updated_at end) as dev_distinct_ak_updated_at
,count(distinct case when dev.bern_id is not null then dev.bern_id end) as dev_distinct_bern_id
,count(distinct case when dev.bern_canvasser_id is not null then dev.bern_canvasser_id end) as dev_distinct_bern_canvasser_id
,count(distinct case when dev.myc_van_id is not null then dev.myc_van_id end) as dev_distinct_myc_van_id
,count(distinct case when dev.st_myc_van_id is not null then dev.st_myc_van_id end) as dev_distinct_st_myc_van_id
,count(distinct case when dev.myc_dupe is not null then dev.myc_dupe end) as dev_distinct_myc_dupe
,count(distinct case when dev.myc_voter_type_id is not null then dev.myc_voter_type_id end) as dev_distinct_myc_voter_type_id
,count(distinct case when dev.myc_myv_linked is not null then dev.myc_myv_linked end) as dev_distinct_myc_myv_linked
,count(distinct case when dev.myv_van_id is not null then dev.myv_van_id end) as dev_distinct_myv_van_id
,count(distinct case when dev.st_myv_van_id is not null then dev.st_myv_van_id end) as dev_distinct_st_myv_van_id
,count(distinct case when dev.email is not null then dev.email end) as dev_distinct_email
,count(distinct case when dev.aux_myc_email is not null then dev.aux_myc_email end) as dev_distinct_aux_myc_email
,count(distinct case when dev.student_hash is not null then dev.student_hash end) as dev_distinct_student_hash
,count(distinct case when dev.student_rn is not null then dev.student_rn end) as dev_distinct_student_rn
,count(distinct case when dev.state_code is not null then dev.state_code end) as dev_distinct_state_code

,count(*)
from 
bernie_data_commons.master_xwalk_st_myc prod
left outer join 
bernie_nmarchio2.master_xwalk_st_myc_20200224 dev
using(st_myc_van_id)
where dev.st_myc_van_id is null
group by 1,2)
union all
(select 
 'st_myv_van_id' as id_type
,'left outer join' as join_type 
,count(distinct case when prod.lalvoterid = dev.lalvoterid then prod.lalvoterid end) as matched_lalvoterid
,count(distinct case when prod.voters_statevoterid = dev.voters_statevoterid then prod.voters_statevoterid end) as matched_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 = dev.sos_join_key_l2 then prod.sos_join_key_l2 end) as matched_sos_join_key_l2
,count(distinct case when prod.l2_precinct = dev.l2_precinct then prod.l2_precinct end) as matched_l2_precinct
,count(distinct case when prod.person_id = dev.person_id then prod.person_id end) as matched_person_id
,count(distinct case when prod.voterbase_id = dev.voterbase_id then prod.voterbase_id end) as matched_voterbase_id
,count(distinct case when prod.dnc_precinct_id = dev.dnc_precinct_id then prod.dnc_precinct_id end) as matched_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id = dev.dnc_sos_id then prod.dnc_sos_id end) as matched_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc = dev.sos_join_key_dnc then prod.sos_join_key_dnc end) as matched_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id = dev.dnc_county_voter_id then prod.dnc_county_voter_id end) as matched_dnc_county_voter_id
,count(distinct case when prod.county_fips = dev.county_fips then prod.county_fips end) as matched_county_fips
,count(distinct case when prod.pdiid = dev.pdiid then prod.pdiid end) as matched_pdiid
,count(distinct case when prod.pdi_sos_id = dev.pdi_sos_id then prod.pdi_sos_id end) as matched_pdi_sos_id
,count(distinct case when prod.pdi_precinct = dev.pdi_precinct then prod.pdi_precinct end) as matched_pdi_precinct
,count(distinct case when prod.actionkit_id = dev.actionkit_id then prod.actionkit_id end) as matched_actionkit_id
,count(distinct case when prod.ak_updated_at = dev.ak_updated_at then prod.ak_updated_at end) as matched_ak_updated_at
,count(distinct case when prod.bern_id = dev.bern_id then prod.bern_id end) as matched_bern_id
,count(distinct case when prod.bern_canvasser_id = dev.bern_canvasser_id then prod.bern_canvasser_id end) as matched_bern_canvasser_id
,count(distinct case when prod.myc_van_id = dev.myc_van_id then prod.myc_van_id end) as matched_myc_van_id
,count(distinct case when prod.st_myc_van_id = dev.st_myc_van_id then prod.st_myc_van_id end) as matched_st_myc_van_id
,count(distinct case when prod.myc_dupe = dev.myc_dupe then prod.myc_dupe end) as matched_myc_dupe
,count(distinct case when prod.myc_voter_type_id = dev.myc_voter_type_id then prod.myc_voter_type_id end) as matched_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked = dev.myc_myv_linked then prod.myc_myv_linked end) as matched_myc_myv_linked
,count(distinct case when prod.myv_van_id = dev.myv_van_id then prod.myv_van_id end) as matched_myv_van_id
,count(distinct case when prod.st_myv_van_id = dev.st_myv_van_id then prod.st_myv_van_id end) as matched_st_myv_van_id
,count(distinct case when prod.email = dev.email then prod.email end) as matched_email
,count(distinct case when prod.aux_myc_email = dev.aux_myc_email then prod.aux_myc_email end) as matched_aux_myc_email
,count(distinct case when prod.student_hash = dev.student_hash then prod.student_hash end) as matched_student_hash
,count(distinct case when prod.student_rn = dev.student_rn then prod.student_rn end) as matched_student_rn
,count(distinct case when prod.state_code = dev.state_code then prod.state_code end) as matched_state_code

,count(distinct case when prod.lalvoterid is not null then prod.lalvoterid end) as prod_distinct_lalvoterid
,count(distinct case when prod.voters_statevoterid is not null then prod.voters_statevoterid end) as prod_distinct_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 is not null then prod.sos_join_key_l2 end) as prod_distinct_sos_join_key_l2
,count(distinct case when prod.l2_precinct is not null then prod.l2_precinct end) as prod_distinct_l2_precinct
,count(distinct case when prod.person_id is not null then prod.person_id end) as prod_distinct_person_id
,count(distinct case when prod.voterbase_id is not null then prod.voterbase_id end) as prod_distinct_voterbase_id
,count(distinct case when prod.dnc_precinct_id is not null then prod.dnc_precinct_id end) as prod_distinct_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id is not null then prod.dnc_sos_id end) as prod_distinct_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc is not null then prod.sos_join_key_dnc end) as prod_distinct_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id is not null then prod.dnc_county_voter_id end) as prod_distinct_dnc_county_voter_id
,count(distinct case when prod.county_fips is not null then prod.county_fips end) as prod_distinct_county_fips
,count(distinct case when prod.pdiid is not null then prod.pdiid end) as prod_distinct_pdiid
,count(distinct case when prod.pdi_sos_id is not null then prod.pdi_sos_id end) as prod_distinct_pdi_sos_id
,count(distinct case when prod.pdi_precinct is not null then prod.pdi_precinct end) as prod_distinct_pdi_precinct
,count(distinct case when prod.actionkit_id is not null then prod.actionkit_id end) as prod_distinct_actionkit_id
,count(distinct case when prod.ak_updated_at is not null then prod.ak_updated_at end) as prod_distinct_ak_updated_at
,count(distinct case when prod.bern_id is not null then prod.bern_id end) as prod_distinct_bern_id
,count(distinct case when prod.bern_canvasser_id is not null then prod.bern_canvasser_id end) as prod_distinct_bern_canvasser_id
,count(distinct case when prod.myc_van_id is not null then prod.myc_van_id end) as prod_distinct_myc_van_id
,count(distinct case when prod.st_myc_van_id is not null then prod.st_myc_van_id end) as prod_distinct_st_myc_van_id
,count(distinct case when prod.myc_dupe is not null then prod.myc_dupe end) as prod_distinct_myc_dupe
,count(distinct case when prod.myc_voter_type_id is not null then prod.myc_voter_type_id end) as prod_distinct_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked is not null then prod.myc_myv_linked end) as prod_distinct_myc_myv_linked
,count(distinct case when prod.myv_van_id is not null then prod.myv_van_id end) as prod_distinct_myv_van_id
,count(distinct case when prod.st_myv_van_id is not null then prod.st_myv_van_id end) as prod_distinct_st_myv_van_id
,count(distinct case when prod.email is not null then prod.email end) as prod_distinct_email
,count(distinct case when prod.aux_myc_email is not null then prod.aux_myc_email end) as prod_distinct_aux_myc_email
,count(distinct case when prod.student_hash is not null then prod.student_hash end) as prod_distinct_student_hash
,count(distinct case when prod.student_rn is not null then prod.student_rn end) as prod_distinct_student_rn
,count(distinct case when prod.state_code is not null then prod.state_code end) as prod_distinct_state_code

,count(distinct case when dev.lalvoterid is not null then dev.lalvoterid end) as dev_distinct_lalvoterid
,count(distinct case when dev.voters_statevoterid is not null then dev.voters_statevoterid end) as dev_distinct_voters_statevoterid
,count(distinct case when dev.sos_join_key_l2 is not null then dev.sos_join_key_l2 end) as dev_distinct_sos_join_key_l2
,count(distinct case when dev.l2_precinct is not null then dev.l2_precinct end) as dev_distinct_l2_precinct
,count(distinct case when dev.person_id is not null then dev.person_id end) as dev_distinct_person_id
,count(distinct case when dev.voterbase_id is not null then dev.voterbase_id end) as dev_distinct_voterbase_id
,count(distinct case when dev.dnc_precinct_id is not null then dev.dnc_precinct_id end) as dev_distinct_dnc_precinct_id
,count(distinct case when dev.dnc_sos_id is not null then dev.dnc_sos_id end) as dev_distinct_dnc_sos_id
,count(distinct case when dev.sos_join_key_dnc is not null then dev.sos_join_key_dnc end) as dev_distinct_sos_join_key_dnc
,count(distinct case when dev.dnc_county_voter_id is not null then dev.dnc_county_voter_id end) as dev_distinct_dnc_county_voter_id
,count(distinct case when dev.county_fips is not null then dev.county_fips end) as dev_distinct_county_fips
,count(distinct case when dev.pdiid is not null then dev.pdiid end) as dev_distinct_pdiid
,count(distinct case when dev.pdi_sos_id is not null then dev.pdi_sos_id end) as dev_distinct_pdi_sos_id
,count(distinct case when dev.pdi_precinct is not null then dev.pdi_precinct end) as dev_distinct_pdi_precinct
,count(distinct case when dev.actionkit_id is not null then dev.actionkit_id end) as dev_distinct_actionkit_id
,count(distinct case when dev.ak_updated_at is not null then dev.ak_updated_at end) as dev_distinct_ak_updated_at
,count(distinct case when dev.bern_id is not null then dev.bern_id end) as dev_distinct_bern_id
,count(distinct case when dev.bern_canvasser_id is not null then dev.bern_canvasser_id end) as dev_distinct_bern_canvasser_id
,count(distinct case when dev.myc_van_id is not null then dev.myc_van_id end) as dev_distinct_myc_van_id
,count(distinct case when dev.st_myc_van_id is not null then dev.st_myc_van_id end) as dev_distinct_st_myc_van_id
,count(distinct case when dev.myc_dupe is not null then dev.myc_dupe end) as dev_distinct_myc_dupe
,count(distinct case when dev.myc_voter_type_id is not null then dev.myc_voter_type_id end) as dev_distinct_myc_voter_type_id
,count(distinct case when dev.myc_myv_linked is not null then dev.myc_myv_linked end) as dev_distinct_myc_myv_linked
,count(distinct case when dev.myv_van_id is not null then dev.myv_van_id end) as dev_distinct_myv_van_id
,count(distinct case when dev.st_myv_van_id is not null then dev.st_myv_van_id end) as dev_distinct_st_myv_van_id
,count(distinct case when dev.email is not null then dev.email end) as dev_distinct_email
,count(distinct case when dev.aux_myc_email is not null then dev.aux_myc_email end) as dev_distinct_aux_myc_email
,count(distinct case when dev.student_hash is not null then dev.student_hash end) as dev_distinct_student_hash
,count(distinct case when dev.student_rn is not null then dev.student_rn end) as dev_distinct_student_rn
,count(distinct case when dev.state_code is not null then dev.state_code end) as dev_distinct_state_code

,count(*)
from 
bernie_data_commons.master_xwalk_st_myv prod
left outer join 
bernie_nmarchio2.master_xwalk_st_myv_20200224 dev
using(st_myv_van_id)
where dev.st_myv_van_id is null
group by 1,2)

union all 


(select 
 'actionkit_id' as id_type
,'right outer join' as join_type
,count(distinct case when prod.lalvoterid = dev.lalvoterid then prod.lalvoterid end) as matched_lalvoterid
,count(distinct case when prod.voters_statevoterid = dev.voters_statevoterid then prod.voters_statevoterid end) as matched_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 = dev.sos_join_key_l2 then prod.sos_join_key_l2 end) as matched_sos_join_key_l2
,count(distinct case when prod.l2_precinct = dev.l2_precinct then prod.l2_precinct end) as matched_l2_precinct
,count(distinct case when prod.person_id = dev.person_id then prod.person_id end) as matched_person_id
,count(distinct case when prod.voterbase_id = dev.voterbase_id then prod.voterbase_id end) as matched_voterbase_id
,count(distinct case when prod.dnc_precinct_id = dev.dnc_precinct_id then prod.dnc_precinct_id end) as matched_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id = dev.dnc_sos_id then prod.dnc_sos_id end) as matched_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc = dev.sos_join_key_dnc then prod.sos_join_key_dnc end) as matched_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id = dev.dnc_county_voter_id then prod.dnc_county_voter_id end) as matched_dnc_county_voter_id
,count(distinct case when prod.county_fips = dev.county_fips then prod.county_fips end) as matched_county_fips
,count(distinct case when prod.pdiid = dev.pdiid then prod.pdiid end) as matched_pdiid
,count(distinct case when prod.pdi_sos_id = dev.pdi_sos_id then prod.pdi_sos_id end) as matched_pdi_sos_id
,count(distinct case when prod.pdi_precinct = dev.pdi_precinct then prod.pdi_precinct end) as matched_pdi_precinct
,count(distinct case when prod.actionkit_id = dev.actionkit_id then prod.actionkit_id end) as matched_actionkit_id
,count(distinct case when prod.ak_updated_at = dev.ak_updated_at then prod.ak_updated_at end) as matched_ak_updated_at
,count(distinct case when prod.bern_id = dev.bern_id then prod.bern_id end) as matched_bern_id
,count(distinct case when prod.bern_canvasser_id = dev.bern_canvasser_id then prod.bern_canvasser_id end) as matched_bern_canvasser_id
,count(distinct case when prod.myc_van_id = dev.myc_van_id then prod.myc_van_id end) as matched_myc_van_id
,count(distinct case when prod.st_myc_van_id = dev.st_myc_van_id then prod.st_myc_van_id end) as matched_st_myc_van_id
,count(distinct case when prod.myc_dupe = dev.myc_dupe then prod.myc_dupe end) as matched_myc_dupe
,count(distinct case when prod.myc_voter_type_id = dev.myc_voter_type_id then prod.myc_voter_type_id end) as matched_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked = dev.myc_myv_linked then prod.myc_myv_linked end) as matched_myc_myv_linked
,count(distinct case when prod.myv_van_id = dev.myv_van_id then prod.myv_van_id end) as matched_myv_van_id
,count(distinct case when prod.st_myv_van_id = dev.st_myv_van_id then prod.st_myv_van_id end) as matched_st_myv_van_id
,count(distinct case when prod.email = dev.email then prod.email end) as matched_email
,count(distinct case when prod.aux_myc_email = dev.aux_myc_email then prod.aux_myc_email end) as matched_aux_myc_email
,count(distinct case when prod.student_hash = dev.student_hash then prod.student_hash end) as matched_student_hash
,count(distinct case when prod.student_rn = dev.student_rn then prod.student_rn end) as matched_student_rn
,count(distinct case when prod.state_code = dev.state_code then prod.state_code end) as matched_state_code

,count(distinct case when prod.lalvoterid is not null then prod.lalvoterid end) as prod_distinct_lalvoterid
,count(distinct case when prod.voters_statevoterid is not null then prod.voters_statevoterid end) as prod_distinct_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 is not null then prod.sos_join_key_l2 end) as prod_distinct_sos_join_key_l2
,count(distinct case when prod.l2_precinct is not null then prod.l2_precinct end) as prod_distinct_l2_precinct
,count(distinct case when prod.person_id is not null then prod.person_id end) as prod_distinct_person_id
,count(distinct case when prod.voterbase_id is not null then prod.voterbase_id end) as prod_distinct_voterbase_id
,count(distinct case when prod.dnc_precinct_id is not null then prod.dnc_precinct_id end) as prod_distinct_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id is not null then prod.dnc_sos_id end) as prod_distinct_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc is not null then prod.sos_join_key_dnc end) as prod_distinct_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id is not null then prod.dnc_county_voter_id end) as prod_distinct_dnc_county_voter_id
,count(distinct case when prod.county_fips is not null then prod.county_fips end) as prod_distinct_county_fips
,count(distinct case when prod.pdiid is not null then prod.pdiid end) as prod_distinct_pdiid
,count(distinct case when prod.pdi_sos_id is not null then prod.pdi_sos_id end) as prod_distinct_pdi_sos_id
,count(distinct case when prod.pdi_precinct is not null then prod.pdi_precinct end) as prod_distinct_pdi_precinct
,count(distinct case when prod.actionkit_id is not null then prod.actionkit_id end) as prod_distinct_actionkit_id
,count(distinct case when prod.ak_updated_at is not null then prod.ak_updated_at end) as prod_distinct_ak_updated_at
,count(distinct case when prod.bern_id is not null then prod.bern_id end) as prod_distinct_bern_id
,count(distinct case when prod.bern_canvasser_id is not null then prod.bern_canvasser_id end) as prod_distinct_bern_canvasser_id
,count(distinct case when prod.myc_van_id is not null then prod.myc_van_id end) as prod_distinct_myc_van_id
,count(distinct case when prod.st_myc_van_id is not null then prod.st_myc_van_id end) as prod_distinct_st_myc_van_id
,count(distinct case when prod.myc_dupe is not null then prod.myc_dupe end) as prod_distinct_myc_dupe
,count(distinct case when prod.myc_voter_type_id is not null then prod.myc_voter_type_id end) as prod_distinct_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked is not null then prod.myc_myv_linked end) as prod_distinct_myc_myv_linked
,count(distinct case when prod.myv_van_id is not null then prod.myv_van_id end) as prod_distinct_myv_van_id
,count(distinct case when prod.st_myv_van_id is not null then prod.st_myv_van_id end) as prod_distinct_st_myv_van_id
,count(distinct case when prod.email is not null then prod.email end) as prod_distinct_email
,count(distinct case when prod.aux_myc_email is not null then prod.aux_myc_email end) as prod_distinct_aux_myc_email
,count(distinct case when prod.student_hash is not null then prod.student_hash end) as prod_distinct_student_hash
,count(distinct case when prod.student_rn is not null then prod.student_rn end) as prod_distinct_student_rn
,count(distinct case when prod.state_code is not null then prod.state_code end) as prod_distinct_state_code

,count(distinct case when dev.lalvoterid is not null then dev.lalvoterid end) as dev_distinct_lalvoterid
,count(distinct case when dev.voters_statevoterid is not null then dev.voters_statevoterid end) as dev_distinct_voters_statevoterid
,count(distinct case when dev.sos_join_key_l2 is not null then dev.sos_join_key_l2 end) as dev_distinct_sos_join_key_l2
,count(distinct case when dev.l2_precinct is not null then dev.l2_precinct end) as dev_distinct_l2_precinct
,count(distinct case when dev.person_id is not null then dev.person_id end) as dev_distinct_person_id
,count(distinct case when dev.voterbase_id is not null then dev.voterbase_id end) as dev_distinct_voterbase_id
,count(distinct case when dev.dnc_precinct_id is not null then dev.dnc_precinct_id end) as dev_distinct_dnc_precinct_id
,count(distinct case when dev.dnc_sos_id is not null then dev.dnc_sos_id end) as dev_distinct_dnc_sos_id
,count(distinct case when dev.sos_join_key_dnc is not null then dev.sos_join_key_dnc end) as dev_distinct_sos_join_key_dnc
,count(distinct case when dev.dnc_county_voter_id is not null then dev.dnc_county_voter_id end) as dev_distinct_dnc_county_voter_id
,count(distinct case when dev.county_fips is not null then dev.county_fips end) as dev_distinct_county_fips
,count(distinct case when dev.pdiid is not null then dev.pdiid end) as dev_distinct_pdiid
,count(distinct case when dev.pdi_sos_id is not null then dev.pdi_sos_id end) as dev_distinct_pdi_sos_id
,count(distinct case when dev.pdi_precinct is not null then dev.pdi_precinct end) as dev_distinct_pdi_precinct
,count(distinct case when dev.actionkit_id is not null then dev.actionkit_id end) as dev_distinct_actionkit_id
,count(distinct case when dev.ak_updated_at is not null then dev.ak_updated_at end) as dev_distinct_ak_updated_at
,count(distinct case when dev.bern_id is not null then dev.bern_id end) as dev_distinct_bern_id
,count(distinct case when dev.bern_canvasser_id is not null then dev.bern_canvasser_id end) as dev_distinct_bern_canvasser_id
,count(distinct case when dev.myc_van_id is not null then dev.myc_van_id end) as dev_distinct_myc_van_id
,count(distinct case when dev.st_myc_van_id is not null then dev.st_myc_van_id end) as dev_distinct_st_myc_van_id
,count(distinct case when dev.myc_dupe is not null then dev.myc_dupe end) as dev_distinct_myc_dupe
,count(distinct case when dev.myc_voter_type_id is not null then dev.myc_voter_type_id end) as dev_distinct_myc_voter_type_id
,count(distinct case when dev.myc_myv_linked is not null then dev.myc_myv_linked end) as dev_distinct_myc_myv_linked
,count(distinct case when dev.myv_van_id is not null then dev.myv_van_id end) as dev_distinct_myv_van_id
,count(distinct case when dev.st_myv_van_id is not null then dev.st_myv_van_id end) as dev_distinct_st_myv_van_id
,count(distinct case when dev.email is not null then dev.email end) as dev_distinct_email
,count(distinct case when dev.aux_myc_email is not null then dev.aux_myc_email end) as dev_distinct_aux_myc_email
,count(distinct case when dev.student_hash is not null then dev.student_hash end) as dev_distinct_student_hash
,count(distinct case when dev.student_rn is not null then dev.student_rn end) as dev_distinct_student_rn
,count(distinct case when dev.state_code is not null then dev.state_code end) as dev_distinct_state_code

,count(*)
 from 
bernie_data_commons.master_xwalk_ak prod
right outer join 
bernie_nmarchio2.master_xwalk_ak_20200224 dev
using(actionkit_id)
where prod.actionkit_id is null
group by 1,2)
union all
(select 
 'person_id' as id_type
,'right outer join' as join_type
,count(distinct case when prod.lalvoterid = dev.lalvoterid then prod.lalvoterid end) as matched_lalvoterid
,count(distinct case when prod.voters_statevoterid = dev.voters_statevoterid then prod.voters_statevoterid end) as matched_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 = dev.sos_join_key_l2 then prod.sos_join_key_l2 end) as matched_sos_join_key_l2
,count(distinct case when prod.l2_precinct = dev.l2_precinct then prod.l2_precinct end) as matched_l2_precinct
,count(distinct case when prod.person_id = dev.person_id then prod.person_id end) as matched_person_id
,count(distinct case when prod.voterbase_id = dev.voterbase_id then prod.voterbase_id end) as matched_voterbase_id
,count(distinct case when prod.dnc_precinct_id = dev.dnc_precinct_id then prod.dnc_precinct_id end) as matched_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id = dev.dnc_sos_id then prod.dnc_sos_id end) as matched_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc = dev.sos_join_key_dnc then prod.sos_join_key_dnc end) as matched_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id = dev.dnc_county_voter_id then prod.dnc_county_voter_id end) as matched_dnc_county_voter_id
,count(distinct case when prod.county_fips = dev.county_fips then prod.county_fips end) as matched_county_fips
,count(distinct case when prod.pdiid = dev.pdiid then prod.pdiid end) as matched_pdiid
,count(distinct case when prod.pdi_sos_id = dev.pdi_sos_id then prod.pdi_sos_id end) as matched_pdi_sos_id
,count(distinct case when prod.pdi_precinct = dev.pdi_precinct then prod.pdi_precinct end) as matched_pdi_precinct
,count(distinct case when prod.actionkit_id = dev.actionkit_id then prod.actionkit_id end) as matched_actionkit_id
,count(distinct case when prod.ak_updated_at = dev.ak_updated_at then prod.ak_updated_at end) as matched_ak_updated_at
,count(distinct case when prod.bern_id = dev.bern_id then prod.bern_id end) as matched_bern_id
,count(distinct case when prod.bern_canvasser_id = dev.bern_canvasser_id then prod.bern_canvasser_id end) as matched_bern_canvasser_id
,count(distinct case when prod.myc_van_id = dev.myc_van_id then prod.myc_van_id end) as matched_myc_van_id
,count(distinct case when prod.st_myc_van_id = dev.st_myc_van_id then prod.st_myc_van_id end) as matched_st_myc_van_id
,count(distinct case when prod.myc_dupe = dev.myc_dupe then prod.myc_dupe end) as matched_myc_dupe
,count(distinct case when prod.myc_voter_type_id = dev.myc_voter_type_id then prod.myc_voter_type_id end) as matched_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked = dev.myc_myv_linked then prod.myc_myv_linked end) as matched_myc_myv_linked
,count(distinct case when prod.myv_van_id = dev.myv_van_id then prod.myv_van_id end) as matched_myv_van_id
,count(distinct case when prod.st_myv_van_id = dev.st_myv_van_id then prod.st_myv_van_id end) as matched_st_myv_van_id
,count(distinct case when prod.email = dev.email then prod.email end) as matched_email
,count(distinct case when prod.aux_myc_email = dev.aux_myc_email then prod.aux_myc_email end) as matched_aux_myc_email
,count(distinct case when prod.student_hash = dev.student_hash then prod.student_hash end) as matched_student_hash
,count(distinct case when prod.student_rn = dev.student_rn then prod.student_rn end) as matched_student_rn
,count(distinct case when prod.state_code = dev.state_code then prod.state_code end) as matched_state_code

,count(distinct case when prod.lalvoterid is not null then prod.lalvoterid end) as prod_distinct_lalvoterid
,count(distinct case when prod.voters_statevoterid is not null then prod.voters_statevoterid end) as prod_distinct_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 is not null then prod.sos_join_key_l2 end) as prod_distinct_sos_join_key_l2
,count(distinct case when prod.l2_precinct is not null then prod.l2_precinct end) as prod_distinct_l2_precinct
,count(distinct case when prod.person_id is not null then prod.person_id end) as prod_distinct_person_id
,count(distinct case when prod.voterbase_id is not null then prod.voterbase_id end) as prod_distinct_voterbase_id
,count(distinct case when prod.dnc_precinct_id is not null then prod.dnc_precinct_id end) as prod_distinct_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id is not null then prod.dnc_sos_id end) as prod_distinct_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc is not null then prod.sos_join_key_dnc end) as prod_distinct_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id is not null then prod.dnc_county_voter_id end) as prod_distinct_dnc_county_voter_id
,count(distinct case when prod.county_fips is not null then prod.county_fips end) as prod_distinct_county_fips
,count(distinct case when prod.pdiid is not null then prod.pdiid end) as prod_distinct_pdiid
,count(distinct case when prod.pdi_sos_id is not null then prod.pdi_sos_id end) as prod_distinct_pdi_sos_id
,count(distinct case when prod.pdi_precinct is not null then prod.pdi_precinct end) as prod_distinct_pdi_precinct
,count(distinct case when prod.actionkit_id is not null then prod.actionkit_id end) as prod_distinct_actionkit_id
,count(distinct case when prod.ak_updated_at is not null then prod.ak_updated_at end) as prod_distinct_ak_updated_at
,count(distinct case when prod.bern_id is not null then prod.bern_id end) as prod_distinct_bern_id
,count(distinct case when prod.bern_canvasser_id is not null then prod.bern_canvasser_id end) as prod_distinct_bern_canvasser_id
,count(distinct case when prod.myc_van_id is not null then prod.myc_van_id end) as prod_distinct_myc_van_id
,count(distinct case when prod.st_myc_van_id is not null then prod.st_myc_van_id end) as prod_distinct_st_myc_van_id
,count(distinct case when prod.myc_dupe is not null then prod.myc_dupe end) as prod_distinct_myc_dupe
,count(distinct case when prod.myc_voter_type_id is not null then prod.myc_voter_type_id end) as prod_distinct_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked is not null then prod.myc_myv_linked end) as prod_distinct_myc_myv_linked
,count(distinct case when prod.myv_van_id is not null then prod.myv_van_id end) as prod_distinct_myv_van_id
,count(distinct case when prod.st_myv_van_id is not null then prod.st_myv_van_id end) as prod_distinct_st_myv_van_id
,count(distinct case when prod.email is not null then prod.email end) as prod_distinct_email
,count(distinct case when prod.aux_myc_email is not null then prod.aux_myc_email end) as prod_distinct_aux_myc_email
,count(distinct case when prod.student_hash is not null then prod.student_hash end) as prod_distinct_student_hash
,count(distinct case when prod.student_rn is not null then prod.student_rn end) as prod_distinct_student_rn
,count(distinct case when prod.state_code is not null then prod.state_code end) as prod_distinct_state_code

,count(distinct case when dev.lalvoterid is not null then dev.lalvoterid end) as dev_distinct_lalvoterid
,count(distinct case when dev.voters_statevoterid is not null then dev.voters_statevoterid end) as dev_distinct_voters_statevoterid
,count(distinct case when dev.sos_join_key_l2 is not null then dev.sos_join_key_l2 end) as dev_distinct_sos_join_key_l2
,count(distinct case when dev.l2_precinct is not null then dev.l2_precinct end) as dev_distinct_l2_precinct
,count(distinct case when dev.person_id is not null then dev.person_id end) as dev_distinct_person_id
,count(distinct case when dev.voterbase_id is not null then dev.voterbase_id end) as dev_distinct_voterbase_id
,count(distinct case when dev.dnc_precinct_id is not null then dev.dnc_precinct_id end) as dev_distinct_dnc_precinct_id
,count(distinct case when dev.dnc_sos_id is not null then dev.dnc_sos_id end) as dev_distinct_dnc_sos_id
,count(distinct case when dev.sos_join_key_dnc is not null then dev.sos_join_key_dnc end) as dev_distinct_sos_join_key_dnc
,count(distinct case when dev.dnc_county_voter_id is not null then dev.dnc_county_voter_id end) as dev_distinct_dnc_county_voter_id
,count(distinct case when dev.county_fips is not null then dev.county_fips end) as dev_distinct_county_fips
,count(distinct case when dev.pdiid is not null then dev.pdiid end) as dev_distinct_pdiid
,count(distinct case when dev.pdi_sos_id is not null then dev.pdi_sos_id end) as dev_distinct_pdi_sos_id
,count(distinct case when dev.pdi_precinct is not null then dev.pdi_precinct end) as dev_distinct_pdi_precinct
,count(distinct case when dev.actionkit_id is not null then dev.actionkit_id end) as dev_distinct_actionkit_id
,count(distinct case when dev.ak_updated_at is not null then dev.ak_updated_at end) as dev_distinct_ak_updated_at
,count(distinct case when dev.bern_id is not null then dev.bern_id end) as dev_distinct_bern_id
,count(distinct case when dev.bern_canvasser_id is not null then dev.bern_canvasser_id end) as dev_distinct_bern_canvasser_id
,count(distinct case when dev.myc_van_id is not null then dev.myc_van_id end) as dev_distinct_myc_van_id
,count(distinct case when dev.st_myc_van_id is not null then dev.st_myc_van_id end) as dev_distinct_st_myc_van_id
,count(distinct case when dev.myc_dupe is not null then dev.myc_dupe end) as dev_distinct_myc_dupe
,count(distinct case when dev.myc_voter_type_id is not null then dev.myc_voter_type_id end) as dev_distinct_myc_voter_type_id
,count(distinct case when dev.myc_myv_linked is not null then dev.myc_myv_linked end) as dev_distinct_myc_myv_linked
,count(distinct case when dev.myv_van_id is not null then dev.myv_van_id end) as dev_distinct_myv_van_id
,count(distinct case when dev.st_myv_van_id is not null then dev.st_myv_van_id end) as dev_distinct_st_myv_van_id
,count(distinct case when dev.email is not null then dev.email end) as dev_distinct_email
,count(distinct case when dev.aux_myc_email is not null then dev.aux_myc_email end) as dev_distinct_aux_myc_email
,count(distinct case when dev.student_hash is not null then dev.student_hash end) as dev_distinct_student_hash
,count(distinct case when dev.student_rn is not null then dev.student_rn end) as dev_distinct_student_rn
,count(distinct case when dev.state_code is not null then dev.state_code end) as dev_distinct_state_code

,count(*)
from 
bernie_data_commons.master_xwalk_dnc prod
right outer join 
bernie_nmarchio2.master_xwalk_dnc_20200224 dev
using(person_id)
where prod.person_id is null
group by 1,2)
union all
(select 
 'lalvoterid' as id_type
,'right outer join' as join_type 
,count(distinct case when prod.lalvoterid = dev.lalvoterid then prod.lalvoterid end) as matched_lalvoterid
,count(distinct case when prod.voters_statevoterid = dev.voters_statevoterid then prod.voters_statevoterid end) as matched_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 = dev.sos_join_key_l2 then prod.sos_join_key_l2 end) as matched_sos_join_key_l2
,count(distinct case when prod.l2_precinct = dev.l2_precinct then prod.l2_precinct end) as matched_l2_precinct
,count(distinct case when prod.person_id = dev.person_id then prod.person_id end) as matched_person_id
,count(distinct case when prod.voterbase_id = dev.voterbase_id then prod.voterbase_id end) as matched_voterbase_id
,count(distinct case when prod.dnc_precinct_id = dev.dnc_precinct_id then prod.dnc_precinct_id end) as matched_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id = dev.dnc_sos_id then prod.dnc_sos_id end) as matched_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc = dev.sos_join_key_dnc then prod.sos_join_key_dnc end) as matched_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id = dev.dnc_county_voter_id then prod.dnc_county_voter_id end) as matched_dnc_county_voter_id
,count(distinct case when prod.county_fips = dev.county_fips then prod.county_fips end) as matched_county_fips
,count(distinct case when prod.pdiid = dev.pdiid then prod.pdiid end) as matched_pdiid
,count(distinct case when prod.pdi_sos_id = dev.pdi_sos_id then prod.pdi_sos_id end) as matched_pdi_sos_id
,count(distinct case when prod.pdi_precinct = dev.pdi_precinct then prod.pdi_precinct end) as matched_pdi_precinct
,count(distinct case when prod.actionkit_id = dev.actionkit_id then prod.actionkit_id end) as matched_actionkit_id
,count(distinct case when prod.ak_updated_at = dev.ak_updated_at then prod.ak_updated_at end) as matched_ak_updated_at
,count(distinct case when prod.bern_id = dev.bern_id then prod.bern_id end) as matched_bern_id
,count(distinct case when prod.bern_canvasser_id = dev.bern_canvasser_id then prod.bern_canvasser_id end) as matched_bern_canvasser_id
,count(distinct case when prod.myc_van_id = dev.myc_van_id then prod.myc_van_id end) as matched_myc_van_id
,count(distinct case when prod.st_myc_van_id = dev.st_myc_van_id then prod.st_myc_van_id end) as matched_st_myc_van_id
,count(distinct case when prod.myc_dupe = dev.myc_dupe then prod.myc_dupe end) as matched_myc_dupe
,count(distinct case when prod.myc_voter_type_id = dev.myc_voter_type_id then prod.myc_voter_type_id end) as matched_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked = dev.myc_myv_linked then prod.myc_myv_linked end) as matched_myc_myv_linked
,count(distinct case when prod.myv_van_id = dev.myv_van_id then prod.myv_van_id end) as matched_myv_van_id
,count(distinct case when prod.st_myv_van_id = dev.st_myv_van_id then prod.st_myv_van_id end) as matched_st_myv_van_id
,count(distinct case when prod.email = dev.email then prod.email end) as matched_email
,count(distinct case when prod.aux_myc_email = dev.aux_myc_email then prod.aux_myc_email end) as matched_aux_myc_email
,count(distinct case when prod.student_hash = dev.student_hash then prod.student_hash end) as matched_student_hash
,count(distinct case when prod.student_rn = dev.student_rn then prod.student_rn end) as matched_student_rn
,count(distinct case when prod.state_code = dev.state_code then prod.state_code end) as matched_state_code

,count(distinct case when prod.lalvoterid is not null then prod.lalvoterid end) as prod_distinct_lalvoterid
,count(distinct case when prod.voters_statevoterid is not null then prod.voters_statevoterid end) as prod_distinct_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 is not null then prod.sos_join_key_l2 end) as prod_distinct_sos_join_key_l2
,count(distinct case when prod.l2_precinct is not null then prod.l2_precinct end) as prod_distinct_l2_precinct
,count(distinct case when prod.person_id is not null then prod.person_id end) as prod_distinct_person_id
,count(distinct case when prod.voterbase_id is not null then prod.voterbase_id end) as prod_distinct_voterbase_id
,count(distinct case when prod.dnc_precinct_id is not null then prod.dnc_precinct_id end) as prod_distinct_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id is not null then prod.dnc_sos_id end) as prod_distinct_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc is not null then prod.sos_join_key_dnc end) as prod_distinct_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id is not null then prod.dnc_county_voter_id end) as prod_distinct_dnc_county_voter_id
,count(distinct case when prod.county_fips is not null then prod.county_fips end) as prod_distinct_county_fips
,count(distinct case when prod.pdiid is not null then prod.pdiid end) as prod_distinct_pdiid
,count(distinct case when prod.pdi_sos_id is not null then prod.pdi_sos_id end) as prod_distinct_pdi_sos_id
,count(distinct case when prod.pdi_precinct is not null then prod.pdi_precinct end) as prod_distinct_pdi_precinct
,count(distinct case when prod.actionkit_id is not null then prod.actionkit_id end) as prod_distinct_actionkit_id
,count(distinct case when prod.ak_updated_at is not null then prod.ak_updated_at end) as prod_distinct_ak_updated_at
,count(distinct case when prod.bern_id is not null then prod.bern_id end) as prod_distinct_bern_id
,count(distinct case when prod.bern_canvasser_id is not null then prod.bern_canvasser_id end) as prod_distinct_bern_canvasser_id
,count(distinct case when prod.myc_van_id is not null then prod.myc_van_id end) as prod_distinct_myc_van_id
,count(distinct case when prod.st_myc_van_id is not null then prod.st_myc_van_id end) as prod_distinct_st_myc_van_id
,count(distinct case when prod.myc_dupe is not null then prod.myc_dupe end) as prod_distinct_myc_dupe
,count(distinct case when prod.myc_voter_type_id is not null then prod.myc_voter_type_id end) as prod_distinct_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked is not null then prod.myc_myv_linked end) as prod_distinct_myc_myv_linked
,count(distinct case when prod.myv_van_id is not null then prod.myv_van_id end) as prod_distinct_myv_van_id
,count(distinct case when prod.st_myv_van_id is not null then prod.st_myv_van_id end) as prod_distinct_st_myv_van_id
,count(distinct case when prod.email is not null then prod.email end) as prod_distinct_email
,count(distinct case when prod.aux_myc_email is not null then prod.aux_myc_email end) as prod_distinct_aux_myc_email
,count(distinct case when prod.student_hash is not null then prod.student_hash end) as prod_distinct_student_hash
,count(distinct case when prod.student_rn is not null then prod.student_rn end) as prod_distinct_student_rn
,count(distinct case when prod.state_code is not null then prod.state_code end) as prod_distinct_state_code

,count(distinct case when dev.lalvoterid is not null then dev.lalvoterid end) as dev_distinct_lalvoterid
,count(distinct case when dev.voters_statevoterid is not null then dev.voters_statevoterid end) as dev_distinct_voters_statevoterid
,count(distinct case when dev.sos_join_key_l2 is not null then dev.sos_join_key_l2 end) as dev_distinct_sos_join_key_l2
,count(distinct case when dev.l2_precinct is not null then dev.l2_precinct end) as dev_distinct_l2_precinct
,count(distinct case when dev.person_id is not null then dev.person_id end) as dev_distinct_person_id
,count(distinct case when dev.voterbase_id is not null then dev.voterbase_id end) as dev_distinct_voterbase_id
,count(distinct case when dev.dnc_precinct_id is not null then dev.dnc_precinct_id end) as dev_distinct_dnc_precinct_id
,count(distinct case when dev.dnc_sos_id is not null then dev.dnc_sos_id end) as dev_distinct_dnc_sos_id
,count(distinct case when dev.sos_join_key_dnc is not null then dev.sos_join_key_dnc end) as dev_distinct_sos_join_key_dnc
,count(distinct case when dev.dnc_county_voter_id is not null then dev.dnc_county_voter_id end) as dev_distinct_dnc_county_voter_id
,count(distinct case when dev.county_fips is not null then dev.county_fips end) as dev_distinct_county_fips
,count(distinct case when dev.pdiid is not null then dev.pdiid end) as dev_distinct_pdiid
,count(distinct case when dev.pdi_sos_id is not null then dev.pdi_sos_id end) as dev_distinct_pdi_sos_id
,count(distinct case when dev.pdi_precinct is not null then dev.pdi_precinct end) as dev_distinct_pdi_precinct
,count(distinct case when dev.actionkit_id is not null then dev.actionkit_id end) as dev_distinct_actionkit_id
,count(distinct case when dev.ak_updated_at is not null then dev.ak_updated_at end) as dev_distinct_ak_updated_at
,count(distinct case when dev.bern_id is not null then dev.bern_id end) as dev_distinct_bern_id
,count(distinct case when dev.bern_canvasser_id is not null then dev.bern_canvasser_id end) as dev_distinct_bern_canvasser_id
,count(distinct case when dev.myc_van_id is not null then dev.myc_van_id end) as dev_distinct_myc_van_id
,count(distinct case when dev.st_myc_van_id is not null then dev.st_myc_van_id end) as dev_distinct_st_myc_van_id
,count(distinct case when dev.myc_dupe is not null then dev.myc_dupe end) as dev_distinct_myc_dupe
,count(distinct case when dev.myc_voter_type_id is not null then dev.myc_voter_type_id end) as dev_distinct_myc_voter_type_id
,count(distinct case when dev.myc_myv_linked is not null then dev.myc_myv_linked end) as dev_distinct_myc_myv_linked
,count(distinct case when dev.myv_van_id is not null then dev.myv_van_id end) as dev_distinct_myv_van_id
,count(distinct case when dev.st_myv_van_id is not null then dev.st_myv_van_id end) as dev_distinct_st_myv_van_id
,count(distinct case when dev.email is not null then dev.email end) as dev_distinct_email
,count(distinct case when dev.aux_myc_email is not null then dev.aux_myc_email end) as dev_distinct_aux_myc_email
,count(distinct case when dev.student_hash is not null then dev.student_hash end) as dev_distinct_student_hash
,count(distinct case when dev.student_rn is not null then dev.student_rn end) as dev_distinct_student_rn
,count(distinct case when dev.state_code is not null then dev.state_code end) as dev_distinct_state_code

,count(*)
from 
bernie_data_commons.master_xwalk_lal prod
right outer join 
bernie_nmarchio2.master_xwalk_lal_20200224 dev
using(lalvoterid)
where prod.lalvoterid is null
group by 1,2)
union all
(select 
 'pdiid' as id_type
,'right outer join' as join_type 
,count(distinct case when prod.lalvoterid = dev.lalvoterid then prod.lalvoterid end) as matched_lalvoterid
,count(distinct case when prod.voters_statevoterid = dev.voters_statevoterid then prod.voters_statevoterid end) as matched_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 = dev.sos_join_key_l2 then prod.sos_join_key_l2 end) as matched_sos_join_key_l2
,count(distinct case when prod.l2_precinct = dev.l2_precinct then prod.l2_precinct end) as matched_l2_precinct
,count(distinct case when prod.person_id = dev.person_id then prod.person_id end) as matched_person_id
,count(distinct case when prod.voterbase_id = dev.voterbase_id then prod.voterbase_id end) as matched_voterbase_id
,count(distinct case when prod.dnc_precinct_id = dev.dnc_precinct_id then prod.dnc_precinct_id end) as matched_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id = dev.dnc_sos_id then prod.dnc_sos_id end) as matched_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc = dev.sos_join_key_dnc then prod.sos_join_key_dnc end) as matched_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id = dev.dnc_county_voter_id then prod.dnc_county_voter_id end) as matched_dnc_county_voter_id
,count(distinct case when prod.county_fips = dev.county_fips then prod.county_fips end) as matched_county_fips
,count(distinct case when prod.pdiid = dev.pdiid then prod.pdiid end) as matched_pdiid
,count(distinct case when prod.pdi_sos_id = dev.pdi_sos_id then prod.pdi_sos_id end) as matched_pdi_sos_id
,count(distinct case when prod.pdi_precinct = dev.pdi_precinct then prod.pdi_precinct end) as matched_pdi_precinct
,count(distinct case when prod.actionkit_id = dev.actionkit_id then prod.actionkit_id end) as matched_actionkit_id
,count(distinct case when prod.ak_updated_at = dev.ak_updated_at then prod.ak_updated_at end) as matched_ak_updated_at
,count(distinct case when prod.bern_id = dev.bern_id then prod.bern_id end) as matched_bern_id
,count(distinct case when prod.bern_canvasser_id = dev.bern_canvasser_id then prod.bern_canvasser_id end) as matched_bern_canvasser_id
,count(distinct case when prod.myc_van_id = dev.myc_van_id then prod.myc_van_id end) as matched_myc_van_id
,count(distinct case when prod.st_myc_van_id = dev.st_myc_van_id then prod.st_myc_van_id end) as matched_st_myc_van_id
,count(distinct case when prod.myc_dupe = dev.myc_dupe then prod.myc_dupe end) as matched_myc_dupe
,count(distinct case when prod.myc_voter_type_id = dev.myc_voter_type_id then prod.myc_voter_type_id end) as matched_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked = dev.myc_myv_linked then prod.myc_myv_linked end) as matched_myc_myv_linked
,count(distinct case when prod.myv_van_id = dev.myv_van_id then prod.myv_van_id end) as matched_myv_van_id
,count(distinct case when prod.st_myv_van_id = dev.st_myv_van_id then prod.st_myv_van_id end) as matched_st_myv_van_id
,count(distinct case when prod.email = dev.email then prod.email end) as matched_email
,count(distinct case when prod.aux_myc_email = dev.aux_myc_email then prod.aux_myc_email end) as matched_aux_myc_email
,count(distinct case when prod.student_hash = dev.student_hash then prod.student_hash end) as matched_student_hash
,count(distinct case when prod.student_rn = dev.student_rn then prod.student_rn end) as matched_student_rn
,count(distinct case when prod.state_code = dev.state_code then prod.state_code end) as matched_state_code

,count(distinct case when prod.lalvoterid is not null then prod.lalvoterid end) as prod_distinct_lalvoterid
,count(distinct case when prod.voters_statevoterid is not null then prod.voters_statevoterid end) as prod_distinct_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 is not null then prod.sos_join_key_l2 end) as prod_distinct_sos_join_key_l2
,count(distinct case when prod.l2_precinct is not null then prod.l2_precinct end) as prod_distinct_l2_precinct
,count(distinct case when prod.person_id is not null then prod.person_id end) as prod_distinct_person_id
,count(distinct case when prod.voterbase_id is not null then prod.voterbase_id end) as prod_distinct_voterbase_id
,count(distinct case when prod.dnc_precinct_id is not null then prod.dnc_precinct_id end) as prod_distinct_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id is not null then prod.dnc_sos_id end) as prod_distinct_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc is not null then prod.sos_join_key_dnc end) as prod_distinct_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id is not null then prod.dnc_county_voter_id end) as prod_distinct_dnc_county_voter_id
,count(distinct case when prod.county_fips is not null then prod.county_fips end) as prod_distinct_county_fips
,count(distinct case when prod.pdiid is not null then prod.pdiid end) as prod_distinct_pdiid
,count(distinct case when prod.pdi_sos_id is not null then prod.pdi_sos_id end) as prod_distinct_pdi_sos_id
,count(distinct case when prod.pdi_precinct is not null then prod.pdi_precinct end) as prod_distinct_pdi_precinct
,count(distinct case when prod.actionkit_id is not null then prod.actionkit_id end) as prod_distinct_actionkit_id
,count(distinct case when prod.ak_updated_at is not null then prod.ak_updated_at end) as prod_distinct_ak_updated_at
,count(distinct case when prod.bern_id is not null then prod.bern_id end) as prod_distinct_bern_id
,count(distinct case when prod.bern_canvasser_id is not null then prod.bern_canvasser_id end) as prod_distinct_bern_canvasser_id
,count(distinct case when prod.myc_van_id is not null then prod.myc_van_id end) as prod_distinct_myc_van_id
,count(distinct case when prod.st_myc_van_id is not null then prod.st_myc_van_id end) as prod_distinct_st_myc_van_id
,count(distinct case when prod.myc_dupe is not null then prod.myc_dupe end) as prod_distinct_myc_dupe
,count(distinct case when prod.myc_voter_type_id is not null then prod.myc_voter_type_id end) as prod_distinct_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked is not null then prod.myc_myv_linked end) as prod_distinct_myc_myv_linked
,count(distinct case when prod.myv_van_id is not null then prod.myv_van_id end) as prod_distinct_myv_van_id
,count(distinct case when prod.st_myv_van_id is not null then prod.st_myv_van_id end) as prod_distinct_st_myv_van_id
,count(distinct case when prod.email is not null then prod.email end) as prod_distinct_email
,count(distinct case when prod.aux_myc_email is not null then prod.aux_myc_email end) as prod_distinct_aux_myc_email
,count(distinct case when prod.student_hash is not null then prod.student_hash end) as prod_distinct_student_hash
,count(distinct case when prod.student_rn is not null then prod.student_rn end) as prod_distinct_student_rn
,count(distinct case when prod.state_code is not null then prod.state_code end) as prod_distinct_state_code

,count(distinct case when dev.lalvoterid is not null then dev.lalvoterid end) as dev_distinct_lalvoterid
,count(distinct case when dev.voters_statevoterid is not null then dev.voters_statevoterid end) as dev_distinct_voters_statevoterid
,count(distinct case when dev.sos_join_key_l2 is not null then dev.sos_join_key_l2 end) as dev_distinct_sos_join_key_l2
,count(distinct case when dev.l2_precinct is not null then dev.l2_precinct end) as dev_distinct_l2_precinct
,count(distinct case when dev.person_id is not null then dev.person_id end) as dev_distinct_person_id
,count(distinct case when dev.voterbase_id is not null then dev.voterbase_id end) as dev_distinct_voterbase_id
,count(distinct case when dev.dnc_precinct_id is not null then dev.dnc_precinct_id end) as dev_distinct_dnc_precinct_id
,count(distinct case when dev.dnc_sos_id is not null then dev.dnc_sos_id end) as dev_distinct_dnc_sos_id
,count(distinct case when dev.sos_join_key_dnc is not null then dev.sos_join_key_dnc end) as dev_distinct_sos_join_key_dnc
,count(distinct case when dev.dnc_county_voter_id is not null then dev.dnc_county_voter_id end) as dev_distinct_dnc_county_voter_id
,count(distinct case when dev.county_fips is not null then dev.county_fips end) as dev_distinct_county_fips
,count(distinct case when dev.pdiid is not null then dev.pdiid end) as dev_distinct_pdiid
,count(distinct case when dev.pdi_sos_id is not null then dev.pdi_sos_id end) as dev_distinct_pdi_sos_id
,count(distinct case when dev.pdi_precinct is not null then dev.pdi_precinct end) as dev_distinct_pdi_precinct
,count(distinct case when dev.actionkit_id is not null then dev.actionkit_id end) as dev_distinct_actionkit_id
,count(distinct case when dev.ak_updated_at is not null then dev.ak_updated_at end) as dev_distinct_ak_updated_at
,count(distinct case when dev.bern_id is not null then dev.bern_id end) as dev_distinct_bern_id
,count(distinct case when dev.bern_canvasser_id is not null then dev.bern_canvasser_id end) as dev_distinct_bern_canvasser_id
,count(distinct case when dev.myc_van_id is not null then dev.myc_van_id end) as dev_distinct_myc_van_id
,count(distinct case when dev.st_myc_van_id is not null then dev.st_myc_van_id end) as dev_distinct_st_myc_van_id
,count(distinct case when dev.myc_dupe is not null then dev.myc_dupe end) as dev_distinct_myc_dupe
,count(distinct case when dev.myc_voter_type_id is not null then dev.myc_voter_type_id end) as dev_distinct_myc_voter_type_id
,count(distinct case when dev.myc_myv_linked is not null then dev.myc_myv_linked end) as dev_distinct_myc_myv_linked
,count(distinct case when dev.myv_van_id is not null then dev.myv_van_id end) as dev_distinct_myv_van_id
,count(distinct case when dev.st_myv_van_id is not null then dev.st_myv_van_id end) as dev_distinct_st_myv_van_id
,count(distinct case when dev.email is not null then dev.email end) as dev_distinct_email
,count(distinct case when dev.aux_myc_email is not null then dev.aux_myc_email end) as dev_distinct_aux_myc_email
,count(distinct case when dev.student_hash is not null then dev.student_hash end) as dev_distinct_student_hash
,count(distinct case when dev.student_rn is not null then dev.student_rn end) as dev_distinct_student_rn
,count(distinct case when dev.state_code is not null then dev.state_code end) as dev_distinct_state_code

,count(*)
from 
bernie_data_commons.master_xwalk_pdi prod
right outer join 
bernie_nmarchio2.master_xwalk_pdi_20200224 dev
using(pdiid)
where prod.pdiid is null
group by 1,2)
union all
(select 
 'st_myc_van_id' as id_type
,'right outer join' as join_type 
,count(distinct case when prod.lalvoterid = dev.lalvoterid then prod.lalvoterid end) as matched_lalvoterid
,count(distinct case when prod.voters_statevoterid = dev.voters_statevoterid then prod.voters_statevoterid end) as matched_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 = dev.sos_join_key_l2 then prod.sos_join_key_l2 end) as matched_sos_join_key_l2
,count(distinct case when prod.l2_precinct = dev.l2_precinct then prod.l2_precinct end) as matched_l2_precinct
,count(distinct case when prod.person_id = dev.person_id then prod.person_id end) as matched_person_id
,count(distinct case when prod.voterbase_id = dev.voterbase_id then prod.voterbase_id end) as matched_voterbase_id
,count(distinct case when prod.dnc_precinct_id = dev.dnc_precinct_id then prod.dnc_precinct_id end) as matched_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id = dev.dnc_sos_id then prod.dnc_sos_id end) as matched_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc = dev.sos_join_key_dnc then prod.sos_join_key_dnc end) as matched_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id = dev.dnc_county_voter_id then prod.dnc_county_voter_id end) as matched_dnc_county_voter_id
,count(distinct case when prod.county_fips = dev.county_fips then prod.county_fips end) as matched_county_fips
,count(distinct case when prod.pdiid = dev.pdiid then prod.pdiid end) as matched_pdiid
,count(distinct case when prod.pdi_sos_id = dev.pdi_sos_id then prod.pdi_sos_id end) as matched_pdi_sos_id
,count(distinct case when prod.pdi_precinct = dev.pdi_precinct then prod.pdi_precinct end) as matched_pdi_precinct
,count(distinct case when prod.actionkit_id = dev.actionkit_id then prod.actionkit_id end) as matched_actionkit_id
,count(distinct case when prod.ak_updated_at = dev.ak_updated_at then prod.ak_updated_at end) as matched_ak_updated_at
,count(distinct case when prod.bern_id = dev.bern_id then prod.bern_id end) as matched_bern_id
,count(distinct case when prod.bern_canvasser_id = dev.bern_canvasser_id then prod.bern_canvasser_id end) as matched_bern_canvasser_id
,count(distinct case when prod.myc_van_id = dev.myc_van_id then prod.myc_van_id end) as matched_myc_van_id
,count(distinct case when prod.st_myc_van_id = dev.st_myc_van_id then prod.st_myc_van_id end) as matched_st_myc_van_id
,count(distinct case when prod.myc_dupe = dev.myc_dupe then prod.myc_dupe end) as matched_myc_dupe
,count(distinct case when prod.myc_voter_type_id = dev.myc_voter_type_id then prod.myc_voter_type_id end) as matched_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked = dev.myc_myv_linked then prod.myc_myv_linked end) as matched_myc_myv_linked
,count(distinct case when prod.myv_van_id = dev.myv_van_id then prod.myv_van_id end) as matched_myv_van_id
,count(distinct case when prod.st_myv_van_id = dev.st_myv_van_id then prod.st_myv_van_id end) as matched_st_myv_van_id
,count(distinct case when prod.email = dev.email then prod.email end) as matched_email
,count(distinct case when prod.aux_myc_email = dev.aux_myc_email then prod.aux_myc_email end) as matched_aux_myc_email
,count(distinct case when prod.student_hash = dev.student_hash then prod.student_hash end) as matched_student_hash
,count(distinct case when prod.student_rn = dev.student_rn then prod.student_rn end) as matched_student_rn
,count(distinct case when prod.state_code = dev.state_code then prod.state_code end) as matched_state_code

,count(distinct case when prod.lalvoterid is not null then prod.lalvoterid end) as prod_distinct_lalvoterid
,count(distinct case when prod.voters_statevoterid is not null then prod.voters_statevoterid end) as prod_distinct_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 is not null then prod.sos_join_key_l2 end) as prod_distinct_sos_join_key_l2
,count(distinct case when prod.l2_precinct is not null then prod.l2_precinct end) as prod_distinct_l2_precinct
,count(distinct case when prod.person_id is not null then prod.person_id end) as prod_distinct_person_id
,count(distinct case when prod.voterbase_id is not null then prod.voterbase_id end) as prod_distinct_voterbase_id
,count(distinct case when prod.dnc_precinct_id is not null then prod.dnc_precinct_id end) as prod_distinct_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id is not null then prod.dnc_sos_id end) as prod_distinct_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc is not null then prod.sos_join_key_dnc end) as prod_distinct_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id is not null then prod.dnc_county_voter_id end) as prod_distinct_dnc_county_voter_id
,count(distinct case when prod.county_fips is not null then prod.county_fips end) as prod_distinct_county_fips
,count(distinct case when prod.pdiid is not null then prod.pdiid end) as prod_distinct_pdiid
,count(distinct case when prod.pdi_sos_id is not null then prod.pdi_sos_id end) as prod_distinct_pdi_sos_id
,count(distinct case when prod.pdi_precinct is not null then prod.pdi_precinct end) as prod_distinct_pdi_precinct
,count(distinct case when prod.actionkit_id is not null then prod.actionkit_id end) as prod_distinct_actionkit_id
,count(distinct case when prod.ak_updated_at is not null then prod.ak_updated_at end) as prod_distinct_ak_updated_at
,count(distinct case when prod.bern_id is not null then prod.bern_id end) as prod_distinct_bern_id
,count(distinct case when prod.bern_canvasser_id is not null then prod.bern_canvasser_id end) as prod_distinct_bern_canvasser_id
,count(distinct case when prod.myc_van_id is not null then prod.myc_van_id end) as prod_distinct_myc_van_id
,count(distinct case when prod.st_myc_van_id is not null then prod.st_myc_van_id end) as prod_distinct_st_myc_van_id
,count(distinct case when prod.myc_dupe is not null then prod.myc_dupe end) as prod_distinct_myc_dupe
,count(distinct case when prod.myc_voter_type_id is not null then prod.myc_voter_type_id end) as prod_distinct_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked is not null then prod.myc_myv_linked end) as prod_distinct_myc_myv_linked
,count(distinct case when prod.myv_van_id is not null then prod.myv_van_id end) as prod_distinct_myv_van_id
,count(distinct case when prod.st_myv_van_id is not null then prod.st_myv_van_id end) as prod_distinct_st_myv_van_id
,count(distinct case when prod.email is not null then prod.email end) as prod_distinct_email
,count(distinct case when prod.aux_myc_email is not null then prod.aux_myc_email end) as prod_distinct_aux_myc_email
,count(distinct case when prod.student_hash is not null then prod.student_hash end) as prod_distinct_student_hash
,count(distinct case when prod.student_rn is not null then prod.student_rn end) as prod_distinct_student_rn
,count(distinct case when prod.state_code is not null then prod.state_code end) as prod_distinct_state_code

,count(distinct case when dev.lalvoterid is not null then dev.lalvoterid end) as dev_distinct_lalvoterid
,count(distinct case when dev.voters_statevoterid is not null then dev.voters_statevoterid end) as dev_distinct_voters_statevoterid
,count(distinct case when dev.sos_join_key_l2 is not null then dev.sos_join_key_l2 end) as dev_distinct_sos_join_key_l2
,count(distinct case when dev.l2_precinct is not null then dev.l2_precinct end) as dev_distinct_l2_precinct
,count(distinct case when dev.person_id is not null then dev.person_id end) as dev_distinct_person_id
,count(distinct case when dev.voterbase_id is not null then dev.voterbase_id end) as dev_distinct_voterbase_id
,count(distinct case when dev.dnc_precinct_id is not null then dev.dnc_precinct_id end) as dev_distinct_dnc_precinct_id
,count(distinct case when dev.dnc_sos_id is not null then dev.dnc_sos_id end) as dev_distinct_dnc_sos_id
,count(distinct case when dev.sos_join_key_dnc is not null then dev.sos_join_key_dnc end) as dev_distinct_sos_join_key_dnc
,count(distinct case when dev.dnc_county_voter_id is not null then dev.dnc_county_voter_id end) as dev_distinct_dnc_county_voter_id
,count(distinct case when dev.county_fips is not null then dev.county_fips end) as dev_distinct_county_fips
,count(distinct case when dev.pdiid is not null then dev.pdiid end) as dev_distinct_pdiid
,count(distinct case when dev.pdi_sos_id is not null then dev.pdi_sos_id end) as dev_distinct_pdi_sos_id
,count(distinct case when dev.pdi_precinct is not null then dev.pdi_precinct end) as dev_distinct_pdi_precinct
,count(distinct case when dev.actionkit_id is not null then dev.actionkit_id end) as dev_distinct_actionkit_id
,count(distinct case when dev.ak_updated_at is not null then dev.ak_updated_at end) as dev_distinct_ak_updated_at
,count(distinct case when dev.bern_id is not null then dev.bern_id end) as dev_distinct_bern_id
,count(distinct case when dev.bern_canvasser_id is not null then dev.bern_canvasser_id end) as dev_distinct_bern_canvasser_id
,count(distinct case when dev.myc_van_id is not null then dev.myc_van_id end) as dev_distinct_myc_van_id
,count(distinct case when dev.st_myc_van_id is not null then dev.st_myc_van_id end) as dev_distinct_st_myc_van_id
,count(distinct case when dev.myc_dupe is not null then dev.myc_dupe end) as dev_distinct_myc_dupe
,count(distinct case when dev.myc_voter_type_id is not null then dev.myc_voter_type_id end) as dev_distinct_myc_voter_type_id
,count(distinct case when dev.myc_myv_linked is not null then dev.myc_myv_linked end) as dev_distinct_myc_myv_linked
,count(distinct case when dev.myv_van_id is not null then dev.myv_van_id end) as dev_distinct_myv_van_id
,count(distinct case when dev.st_myv_van_id is not null then dev.st_myv_van_id end) as dev_distinct_st_myv_van_id
,count(distinct case when dev.email is not null then dev.email end) as dev_distinct_email
,count(distinct case when dev.aux_myc_email is not null then dev.aux_myc_email end) as dev_distinct_aux_myc_email
,count(distinct case when dev.student_hash is not null then dev.student_hash end) as dev_distinct_student_hash
,count(distinct case when dev.student_rn is not null then dev.student_rn end) as dev_distinct_student_rn
,count(distinct case when dev.state_code is not null then dev.state_code end) as dev_distinct_state_code

,count(*)
from 
bernie_data_commons.master_xwalk_st_myc prod
right outer join 
bernie_nmarchio2.master_xwalk_st_myc_20200224 dev
using(st_myc_van_id)
where prod.st_myc_van_id is null
group by 1,2)
union all
(select 
 'st_myv_van_id' as id_type
,'right outer join' as join_type 
,count(distinct case when prod.lalvoterid = dev.lalvoterid then prod.lalvoterid end) as matched_lalvoterid
,count(distinct case when prod.voters_statevoterid = dev.voters_statevoterid then prod.voters_statevoterid end) as matched_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 = dev.sos_join_key_l2 then prod.sos_join_key_l2 end) as matched_sos_join_key_l2
,count(distinct case when prod.l2_precinct = dev.l2_precinct then prod.l2_precinct end) as matched_l2_precinct
,count(distinct case when prod.person_id = dev.person_id then prod.person_id end) as matched_person_id
,count(distinct case when prod.voterbase_id = dev.voterbase_id then prod.voterbase_id end) as matched_voterbase_id
,count(distinct case when prod.dnc_precinct_id = dev.dnc_precinct_id then prod.dnc_precinct_id end) as matched_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id = dev.dnc_sos_id then prod.dnc_sos_id end) as matched_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc = dev.sos_join_key_dnc then prod.sos_join_key_dnc end) as matched_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id = dev.dnc_county_voter_id then prod.dnc_county_voter_id end) as matched_dnc_county_voter_id
,count(distinct case when prod.county_fips = dev.county_fips then prod.county_fips end) as matched_county_fips
,count(distinct case when prod.pdiid = dev.pdiid then prod.pdiid end) as matched_pdiid
,count(distinct case when prod.pdi_sos_id = dev.pdi_sos_id then prod.pdi_sos_id end) as matched_pdi_sos_id
,count(distinct case when prod.pdi_precinct = dev.pdi_precinct then prod.pdi_precinct end) as matched_pdi_precinct
,count(distinct case when prod.actionkit_id = dev.actionkit_id then prod.actionkit_id end) as matched_actionkit_id
,count(distinct case when prod.ak_updated_at = dev.ak_updated_at then prod.ak_updated_at end) as matched_ak_updated_at
,count(distinct case when prod.bern_id = dev.bern_id then prod.bern_id end) as matched_bern_id
,count(distinct case when prod.bern_canvasser_id = dev.bern_canvasser_id then prod.bern_canvasser_id end) as matched_bern_canvasser_id
,count(distinct case when prod.myc_van_id = dev.myc_van_id then prod.myc_van_id end) as matched_myc_van_id
,count(distinct case when prod.st_myc_van_id = dev.st_myc_van_id then prod.st_myc_van_id end) as matched_st_myc_van_id
,count(distinct case when prod.myc_dupe = dev.myc_dupe then prod.myc_dupe end) as matched_myc_dupe
,count(distinct case when prod.myc_voter_type_id = dev.myc_voter_type_id then prod.myc_voter_type_id end) as matched_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked = dev.myc_myv_linked then prod.myc_myv_linked end) as matched_myc_myv_linked
,count(distinct case when prod.myv_van_id = dev.myv_van_id then prod.myv_van_id end) as matched_myv_van_id
,count(distinct case when prod.st_myv_van_id = dev.st_myv_van_id then prod.st_myv_van_id end) as matched_st_myv_van_id
,count(distinct case when prod.email = dev.email then prod.email end) as matched_email
,count(distinct case when prod.aux_myc_email = dev.aux_myc_email then prod.aux_myc_email end) as matched_aux_myc_email
,count(distinct case when prod.student_hash = dev.student_hash then prod.student_hash end) as matched_student_hash
,count(distinct case when prod.student_rn = dev.student_rn then prod.student_rn end) as matched_student_rn
,count(distinct case when prod.state_code = dev.state_code then prod.state_code end) as matched_state_code

,count(distinct case when prod.lalvoterid is not null then prod.lalvoterid end) as prod_distinct_lalvoterid
,count(distinct case when prod.voters_statevoterid is not null then prod.voters_statevoterid end) as prod_distinct_voters_statevoterid
,count(distinct case when prod.sos_join_key_l2 is not null then prod.sos_join_key_l2 end) as prod_distinct_sos_join_key_l2
,count(distinct case when prod.l2_precinct is not null then prod.l2_precinct end) as prod_distinct_l2_precinct
,count(distinct case when prod.person_id is not null then prod.person_id end) as prod_distinct_person_id
,count(distinct case when prod.voterbase_id is not null then prod.voterbase_id end) as prod_distinct_voterbase_id
,count(distinct case when prod.dnc_precinct_id is not null then prod.dnc_precinct_id end) as prod_distinct_dnc_precinct_id
,count(distinct case when prod.dnc_sos_id is not null then prod.dnc_sos_id end) as prod_distinct_dnc_sos_id
,count(distinct case when prod.sos_join_key_dnc is not null then prod.sos_join_key_dnc end) as prod_distinct_sos_join_key_dnc
,count(distinct case when prod.dnc_county_voter_id is not null then prod.dnc_county_voter_id end) as prod_distinct_dnc_county_voter_id
,count(distinct case when prod.county_fips is not null then prod.county_fips end) as prod_distinct_county_fips
,count(distinct case when prod.pdiid is not null then prod.pdiid end) as prod_distinct_pdiid
,count(distinct case when prod.pdi_sos_id is not null then prod.pdi_sos_id end) as prod_distinct_pdi_sos_id
,count(distinct case when prod.pdi_precinct is not null then prod.pdi_precinct end) as prod_distinct_pdi_precinct
,count(distinct case when prod.actionkit_id is not null then prod.actionkit_id end) as prod_distinct_actionkit_id
,count(distinct case when prod.ak_updated_at is not null then prod.ak_updated_at end) as prod_distinct_ak_updated_at
,count(distinct case when prod.bern_id is not null then prod.bern_id end) as prod_distinct_bern_id
,count(distinct case when prod.bern_canvasser_id is not null then prod.bern_canvasser_id end) as prod_distinct_bern_canvasser_id
,count(distinct case when prod.myc_van_id is not null then prod.myc_van_id end) as prod_distinct_myc_van_id
,count(distinct case when prod.st_myc_van_id is not null then prod.st_myc_van_id end) as prod_distinct_st_myc_van_id
,count(distinct case when prod.myc_dupe is not null then prod.myc_dupe end) as prod_distinct_myc_dupe
,count(distinct case when prod.myc_voter_type_id is not null then prod.myc_voter_type_id end) as prod_distinct_myc_voter_type_id
,count(distinct case when prod.myc_myv_linked is not null then prod.myc_myv_linked end) as prod_distinct_myc_myv_linked
,count(distinct case when prod.myv_van_id is not null then prod.myv_van_id end) as prod_distinct_myv_van_id
,count(distinct case when prod.st_myv_van_id is not null then prod.st_myv_van_id end) as prod_distinct_st_myv_van_id
,count(distinct case when prod.email is not null then prod.email end) as prod_distinct_email
,count(distinct case when prod.aux_myc_email is not null then prod.aux_myc_email end) as prod_distinct_aux_myc_email
,count(distinct case when prod.student_hash is not null then prod.student_hash end) as prod_distinct_student_hash
,count(distinct case when prod.student_rn is not null then prod.student_rn end) as prod_distinct_student_rn
,count(distinct case when prod.state_code is not null then prod.state_code end) as prod_distinct_state_code

,count(distinct case when dev.lalvoterid is not null then dev.lalvoterid end) as dev_distinct_lalvoterid
,count(distinct case when dev.voters_statevoterid is not null then dev.voters_statevoterid end) as dev_distinct_voters_statevoterid
,count(distinct case when dev.sos_join_key_l2 is not null then dev.sos_join_key_l2 end) as dev_distinct_sos_join_key_l2
,count(distinct case when dev.l2_precinct is not null then dev.l2_precinct end) as dev_distinct_l2_precinct
,count(distinct case when dev.person_id is not null then dev.person_id end) as dev_distinct_person_id
,count(distinct case when dev.voterbase_id is not null then dev.voterbase_id end) as dev_distinct_voterbase_id
,count(distinct case when dev.dnc_precinct_id is not null then dev.dnc_precinct_id end) as dev_distinct_dnc_precinct_id
,count(distinct case when dev.dnc_sos_id is not null then dev.dnc_sos_id end) as dev_distinct_dnc_sos_id
,count(distinct case when dev.sos_join_key_dnc is not null then dev.sos_join_key_dnc end) as dev_distinct_sos_join_key_dnc
,count(distinct case when dev.dnc_county_voter_id is not null then dev.dnc_county_voter_id end) as dev_distinct_dnc_county_voter_id
,count(distinct case when dev.county_fips is not null then dev.county_fips end) as dev_distinct_county_fips
,count(distinct case when dev.pdiid is not null then dev.pdiid end) as dev_distinct_pdiid
,count(distinct case when dev.pdi_sos_id is not null then dev.pdi_sos_id end) as dev_distinct_pdi_sos_id
,count(distinct case when dev.pdi_precinct is not null then dev.pdi_precinct end) as dev_distinct_pdi_precinct
,count(distinct case when dev.actionkit_id is not null then dev.actionkit_id end) as dev_distinct_actionkit_id
,count(distinct case when dev.ak_updated_at is not null then dev.ak_updated_at end) as dev_distinct_ak_updated_at
,count(distinct case when dev.bern_id is not null then dev.bern_id end) as dev_distinct_bern_id
,count(distinct case when dev.bern_canvasser_id is not null then dev.bern_canvasser_id end) as dev_distinct_bern_canvasser_id
,count(distinct case when dev.myc_van_id is not null then dev.myc_van_id end) as dev_distinct_myc_van_id
,count(distinct case when dev.st_myc_van_id is not null then dev.st_myc_van_id end) as dev_distinct_st_myc_van_id
,count(distinct case when dev.myc_dupe is not null then dev.myc_dupe end) as dev_distinct_myc_dupe
,count(distinct case when dev.myc_voter_type_id is not null then dev.myc_voter_type_id end) as dev_distinct_myc_voter_type_id
,count(distinct case when dev.myc_myv_linked is not null then dev.myc_myv_linked end) as dev_distinct_myc_myv_linked
,count(distinct case when dev.myv_van_id is not null then dev.myv_van_id end) as dev_distinct_myv_van_id
,count(distinct case when dev.st_myv_van_id is not null then dev.st_myv_van_id end) as dev_distinct_st_myv_van_id
,count(distinct case when dev.email is not null then dev.email end) as dev_distinct_email
,count(distinct case when dev.aux_myc_email is not null then dev.aux_myc_email end) as dev_distinct_aux_myc_email
,count(distinct case when dev.student_hash is not null then dev.student_hash end) as dev_distinct_student_hash
,count(distinct case when dev.student_rn is not null then dev.student_rn end) as dev_distinct_student_rn
,count(distinct case when dev.state_code is not null then dev.state_code end) as dev_distinct_state_code

,count(*)
from 
bernie_data_commons.master_xwalk_st_myv prod
left outer join 
bernie_nmarchio2.master_xwalk_st_myv_20200224 dev
using(st_myv_van_id)
where prod.st_myv_van_id is null
group by 1,2)
)
)
