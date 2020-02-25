drop table if exists bernie_nmarchio2.xwalk_qc;
create table bernie_nmarchio2.xwalk_qc as (
select * from 
(
(select 
 'actionkit_id' as id_type
,'inner join' as join_type
,sum(case when prod.lalvoterid = dev.lalvoterid and prod.lalvoterid is not null then 1 else 0 end) as matched_lalvoterid
,sum(case when prod.voters_statevoterid = dev.voters_statevoterid and prod.voters_statevoterid is not null then 1 else 0 end) as matched_voters_statevoterid
,sum(case when prod.sos_join_key_l2 = dev.sos_join_key_l2 and prod.sos_join_key_l2 is not null then 1 else 0 end) as matched_sos_join_key_l2
,sum(case when prod.l2_precinct = dev.l2_precinct and prod.l2_precinct is not null then 1 else 0 end) as matched_l2_precinct
,sum(case when prod.person_id = dev.person_id and prod.person_id is not null then 1 else 0 end) as matched_person_id
,sum(case when prod.voterbase_id = dev.voterbase_id and prod.voterbase_id is not null then 1 else 0 end) as matched_voterbase_id
,sum(case when prod.dnc_precinct_id = dev.dnc_precinct_id and prod.dnc_precinct_id is not null then 1 else 0 end) as matched_dnc_precinct_id
,sum(case when prod.dnc_sos_id = dev.dnc_sos_id and prod.dnc_sos_id is not null then 1 else 0 end) as matched_dnc_sos_id
,sum(case when prod.sos_join_key_dnc = dev.sos_join_key_dnc and prod.sos_join_key_dnc is not null then 1 else 0 end) as matched_sos_join_key_dnc
,sum(case when prod.dnc_county_voter_id = dev.dnc_county_voter_id and prod.dnc_county_voter_id is not null then 1 else 0 end) as matched_dnc_county_voter_id
,sum(case when prod.county_fips = dev.county_fips and prod.county_fips is not null then 1 else 0 end) as matched_county_fips
,sum(case when prod.pdiid = dev.pdiid and prod.pdiid is not null then 1 else 0 end) as matched_pdiid
,sum(case when prod.pdi_sos_id = dev.pdi_sos_id and prod.pdi_sos_id is not null then 1 else 0 end) as matched_pdi_sos_id
,sum(case when prod.pdi_precinct = dev.pdi_precinct and prod.pdi_precinct is not null then 1 else 0 end) as matched_pdi_precinct
,sum(case when prod.actionkit_id = dev.actionkit_id and prod.actionkit_id is not null then 1 else 0 end) as matched_actionkit_id
,sum(case when prod.ak_updated_at = dev.ak_updated_at and prod.ak_updated_at is not null then 1 else 0 end) as matched_ak_updated_at
,sum(case when prod.bern_id = dev.bern_id and prod.bern_id is not null then 1 else 0 end) as matched_bern_id
,sum(case when prod.bern_canvasser_id = dev.bern_canvasser_id and prod.bern_canvasser_id is not null then 1 else 0 end) as matched_bern_canvasser_id
,sum(case when prod.myc_van_id = dev.myc_van_id and prod.myc_van_id is not null then 1 else 0 end) as matched_myc_van_id
,sum(case when prod.st_myc_van_id = dev.st_myc_van_id and prod.st_myc_van_id is not null then 1 else 0 end) as matched_st_myc_van_id
,sum(case when prod.myc_dupe = dev.myc_dupe and prod.myc_dupe is not null then 1 else 0 end) as matched_myc_dupe
,sum(case when prod.myc_voter_type_id = dev.myc_voter_type_id and prod.myc_voter_type_id is not null then 1 else 0 end) as matched_myc_voter_type_id
,sum(case when prod.myc_myv_linked = dev.myc_myv_linked and prod.myc_myv_linked is not null then 1 else 0 end) as matched_myc_myv_linked
,sum(case when prod.myv_van_id = dev.myv_van_id and prod.myv_van_id is not null then 1 else 0 end) as matched_myv_van_id
,sum(case when prod.st_myv_van_id = dev.st_myv_van_id and prod.st_myv_van_id is not null then 1 else 0 end) as matched_st_myv_van_id
,sum(case when prod.email = dev.email and prod.email is not null then 1 else 0 end) as matched_email
,sum(case when prod.aux_myc_email = dev.aux_myc_email and prod.aux_myc_email is not null then 1 else 0 end) as matched_aux_myc_email
,sum(case when prod.student_hash = dev.student_hash and prod.student_hash is not null then 1 else 0 end) as matched_student_hash
,sum(case when prod.student_rn = dev.student_rn and prod.student_rn is not null then 1 else 0 end) as matched_student_rn
,sum(case when prod.state_code = dev.state_code and prod.state_code is not null then 1 else 0 end) as matched_state_code

,sum(case when prod.lalvoterid is not null then 1 else 0 end) as prodids_lalvoterid
,sum(case when prod.voters_statevoterid is not null then 1 else 0 end) as prodids_voters_statevoterid
,sum(case when prod.sos_join_key_l2 is not null then 1 else 0 end) as prodids_sos_join_key_l2
,sum(case when prod.l2_precinct is not null then 1 else 0 end) as prodids_l2_precinct
,sum(case when prod.person_id is not null then 1 else 0 end) as prodids_person_id
,sum(case when prod.voterbase_id is not null then 1 else 0 end) as prodids_voterbase_id
,sum(case when prod.dnc_precinct_id is not null then 1 else 0 end) as prodids_dnc_precinct_id
,sum(case when prod.dnc_sos_id is not null then 1 else 0 end) as prodids_dnc_sos_id
,sum(case when prod.sos_join_key_dnc is not null then 1 else 0 end) as prodids_sos_join_key_dnc
,sum(case when prod.dnc_county_voter_id is not null then 1 else 0 end) as prodids_dnc_county_voter_id
,sum(case when prod.county_fips is not null then 1 else 0 end) as prodids_county_fips
,sum(case when prod.pdiid is not null then 1 else 0 end) as prodids_pdiid
,sum(case when prod.pdi_sos_id is not null then 1 else 0 end) as prodids_pdi_sos_id
,sum(case when prod.pdi_precinct is not null then 1 else 0 end) as prodids_pdi_precinct
,sum(case when prod.actionkit_id is not null then 1 else 0 end) as prodids_actionkit_id
,sum(case when prod.ak_updated_at is not null then 1 else 0 end) as prodids_ak_updated_at
,sum(case when prod.bern_id is not null then 1 else 0 end) as prodids_bern_id
,sum(case when prod.bern_canvasser_id is not null then 1 else 0 end) as prodids_bern_canvasser_id
,sum(case when prod.myc_van_id is not null then 1 else 0 end) as prodids_myc_van_id
,sum(case when prod.st_myc_van_id is not null then 1 else 0 end) as prodids_st_myc_van_id
,sum(case when prod.myc_dupe is not null then 1 else 0 end) as prodids_myc_dupe
,sum(case when prod.myc_voter_type_id is not null then 1 else 0 end) as prodids_myc_voter_type_id
,sum(case when prod.myc_myv_linked is not null then 1 else 0 end) as prodids_myc_myv_linked
,sum(case when prod.myv_van_id is not null then 1 else 0 end) as prodids_myv_van_id
,sum(case when prod.st_myv_van_id is not null then 1 else 0 end) as prodids_st_myv_van_id
,sum(case when prod.email is not null then 1 else 0 end) as prodids_email
,sum(case when prod.aux_myc_email is not null then 1 else 0 end) as prodids_aux_myc_email
,sum(case when prod.student_hash is not null then 1 else 0 end) as prodids_student_hash
,sum(case when prod.student_rn is not null then 1 else 0 end) as prodids_student_rn
,sum(case when prod.state_code is not null then 1 else 0 end) as prodids_state_code
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
,sum(case when prod.lalvoterid = dev.lalvoterid and prod.lalvoterid is not null then 1 else 0 end) as matched_lalvoterid
,sum(case when prod.voters_statevoterid = dev.voters_statevoterid and prod.voters_statevoterid is not null then 1 else 0 end) as matched_voters_statevoterid
,sum(case when prod.sos_join_key_l2 = dev.sos_join_key_l2 and prod.sos_join_key_l2 is not null then 1 else 0 end) as matched_sos_join_key_l2
,sum(case when prod.l2_precinct = dev.l2_precinct and prod.l2_precinct is not null then 1 else 0 end) as matched_l2_precinct
,sum(case when prod.person_id = dev.person_id and prod.person_id is not null then 1 else 0 end) as matched_person_id
,sum(case when prod.voterbase_id = dev.voterbase_id and prod.voterbase_id is not null then 1 else 0 end) as matched_voterbase_id
,sum(case when prod.dnc_precinct_id = dev.dnc_precinct_id and prod.dnc_precinct_id is not null then 1 else 0 end) as matched_dnc_precinct_id
,sum(case when prod.dnc_sos_id = dev.dnc_sos_id and prod.dnc_sos_id is not null then 1 else 0 end) as matched_dnc_sos_id
,sum(case when prod.sos_join_key_dnc = dev.sos_join_key_dnc and prod.sos_join_key_dnc is not null then 1 else 0 end) as matched_sos_join_key_dnc
,sum(case when prod.dnc_county_voter_id = dev.dnc_county_voter_id and prod.dnc_county_voter_id is not null then 1 else 0 end) as matched_dnc_county_voter_id
,sum(case when prod.county_fips = dev.county_fips and prod.county_fips is not null then 1 else 0 end) as matched_county_fips
,sum(case when prod.pdiid = dev.pdiid and prod.pdiid is not null then 1 else 0 end) as matched_pdiid
,sum(case when prod.pdi_sos_id = dev.pdi_sos_id and prod.pdi_sos_id is not null then 1 else 0 end) as matched_pdi_sos_id
,sum(case when prod.pdi_precinct = dev.pdi_precinct and prod.pdi_precinct is not null then 1 else 0 end) as matched_pdi_precinct
,sum(case when prod.actionkit_id = dev.actionkit_id and prod.actionkit_id is not null then 1 else 0 end) as matched_actionkit_id
,sum(case when prod.ak_updated_at = dev.ak_updated_at and prod.ak_updated_at is not null then 1 else 0 end) as matched_ak_updated_at
,sum(case when prod.bern_id = dev.bern_id and prod.bern_id is not null then 1 else 0 end) as matched_bern_id
,sum(case when prod.bern_canvasser_id = dev.bern_canvasser_id and prod.bern_canvasser_id is not null then 1 else 0 end) as matched_bern_canvasser_id
,sum(case when prod.myc_van_id = dev.myc_van_id and prod.myc_van_id is not null then 1 else 0 end) as matched_myc_van_id
,sum(case when prod.st_myc_van_id = dev.st_myc_van_id and prod.st_myc_van_id is not null then 1 else 0 end) as matched_st_myc_van_id
,sum(case when prod.myc_dupe = dev.myc_dupe and prod.myc_dupe is not null then 1 else 0 end) as matched_myc_dupe
,sum(case when prod.myc_voter_type_id = dev.myc_voter_type_id and prod.myc_voter_type_id is not null then 1 else 0 end) as matched_myc_voter_type_id
,sum(case when prod.myc_myv_linked = dev.myc_myv_linked and prod.myc_myv_linked is not null then 1 else 0 end) as matched_myc_myv_linked
,sum(case when prod.myv_van_id = dev.myv_van_id and prod.myv_van_id is not null then 1 else 0 end) as matched_myv_van_id
,sum(case when prod.st_myv_van_id = dev.st_myv_van_id and prod.st_myv_van_id is not null then 1 else 0 end) as matched_st_myv_van_id
,sum(case when prod.email = dev.email and prod.email is not null then 1 else 0 end) as matched_email
,sum(case when prod.aux_myc_email = dev.aux_myc_email and prod.aux_myc_email is not null then 1 else 0 end) as matched_aux_myc_email
,sum(case when prod.student_hash = dev.student_hash and prod.student_hash is not null then 1 else 0 end) as matched_student_hash
,sum(case when prod.student_rn = dev.student_rn and prod.student_rn is not null then 1 else 0 end) as matched_student_rn
,sum(case when prod.state_code = dev.state_code and prod.state_code is not null then 1 else 0 end) as matched_state_code

,sum(case when prod.lalvoterid is not null then 1 else 0 end) as prodids_lalvoterid
,sum(case when prod.voters_statevoterid is not null then 1 else 0 end) as prodids_voters_statevoterid
,sum(case when prod.sos_join_key_l2 is not null then 1 else 0 end) as prodids_sos_join_key_l2
,sum(case when prod.l2_precinct is not null then 1 else 0 end) as prodids_l2_precinct
,sum(case when prod.person_id is not null then 1 else 0 end) as prodids_person_id
,sum(case when prod.voterbase_id is not null then 1 else 0 end) as prodids_voterbase_id
,sum(case when prod.dnc_precinct_id is not null then 1 else 0 end) as prodids_dnc_precinct_id
,sum(case when prod.dnc_sos_id is not null then 1 else 0 end) as prodids_dnc_sos_id
,sum(case when prod.sos_join_key_dnc is not null then 1 else 0 end) as prodids_sos_join_key_dnc
,sum(case when prod.dnc_county_voter_id is not null then 1 else 0 end) as prodids_dnc_county_voter_id
,sum(case when prod.county_fips is not null then 1 else 0 end) as prodids_county_fips
,sum(case when prod.pdiid is not null then 1 else 0 end) as prodids_pdiid
,sum(case when prod.pdi_sos_id is not null then 1 else 0 end) as prodids_pdi_sos_id
,sum(case when prod.pdi_precinct is not null then 1 else 0 end) as prodids_pdi_precinct
,sum(case when prod.actionkit_id is not null then 1 else 0 end) as prodids_actionkit_id
,sum(case when prod.ak_updated_at is not null then 1 else 0 end) as prodids_ak_updated_at
,sum(case when prod.bern_id is not null then 1 else 0 end) as prodids_bern_id
,sum(case when prod.bern_canvasser_id is not null then 1 else 0 end) as prodids_bern_canvasser_id
,sum(case when prod.myc_van_id is not null then 1 else 0 end) as prodids_myc_van_id
,sum(case when prod.st_myc_van_id is not null then 1 else 0 end) as prodids_st_myc_van_id
,sum(case when prod.myc_dupe is not null then 1 else 0 end) as prodids_myc_dupe
,sum(case when prod.myc_voter_type_id is not null then 1 else 0 end) as prodids_myc_voter_type_id
,sum(case when prod.myc_myv_linked is not null then 1 else 0 end) as prodids_myc_myv_linked
,sum(case when prod.myv_van_id is not null then 1 else 0 end) as prodids_myv_van_id
,sum(case when prod.st_myv_van_id is not null then 1 else 0 end) as prodids_st_myv_van_id
,sum(case when prod.email is not null then 1 else 0 end) as prodids_email
,sum(case when prod.aux_myc_email is not null then 1 else 0 end) as prodids_aux_myc_email
,sum(case when prod.student_hash is not null then 1 else 0 end) as prodids_student_hash
,sum(case when prod.student_rn is not null then 1 else 0 end) as prodids_student_rn
,sum(case when prod.state_code is not null then 1 else 0 end) as prodids_state_code
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
,sum(case when prod.lalvoterid = dev.lalvoterid and prod.lalvoterid is not null then 1 else 0 end) as matched_lalvoterid
,sum(case when prod.voters_statevoterid = dev.voters_statevoterid and prod.voters_statevoterid is not null then 1 else 0 end) as matched_voters_statevoterid
,sum(case when prod.sos_join_key_l2 = dev.sos_join_key_l2 and prod.sos_join_key_l2 is not null then 1 else 0 end) as matched_sos_join_key_l2
,sum(case when prod.l2_precinct = dev.l2_precinct and prod.l2_precinct is not null then 1 else 0 end) as matched_l2_precinct
,sum(case when prod.person_id = dev.person_id and prod.person_id is not null then 1 else 0 end) as matched_person_id
,sum(case when prod.voterbase_id = dev.voterbase_id and prod.voterbase_id is not null then 1 else 0 end) as matched_voterbase_id
,sum(case when prod.dnc_precinct_id = dev.dnc_precinct_id and prod.dnc_precinct_id is not null then 1 else 0 end) as matched_dnc_precinct_id
,sum(case when prod.dnc_sos_id = dev.dnc_sos_id and prod.dnc_sos_id is not null then 1 else 0 end) as matched_dnc_sos_id
,sum(case when prod.sos_join_key_dnc = dev.sos_join_key_dnc and prod.sos_join_key_dnc is not null then 1 else 0 end) as matched_sos_join_key_dnc
,sum(case when prod.dnc_county_voter_id = dev.dnc_county_voter_id and prod.dnc_county_voter_id is not null then 1 else 0 end) as matched_dnc_county_voter_id
,sum(case when prod.county_fips = dev.county_fips and prod.county_fips is not null then 1 else 0 end) as matched_county_fips
,sum(case when prod.pdiid = dev.pdiid and prod.pdiid is not null then 1 else 0 end) as matched_pdiid
,sum(case when prod.pdi_sos_id = dev.pdi_sos_id and prod.pdi_sos_id is not null then 1 else 0 end) as matched_pdi_sos_id
,sum(case when prod.pdi_precinct = dev.pdi_precinct and prod.pdi_precinct is not null then 1 else 0 end) as matched_pdi_precinct
,sum(case when prod.actionkit_id = dev.actionkit_id and prod.actionkit_id is not null then 1 else 0 end) as matched_actionkit_id
,sum(case when prod.ak_updated_at = dev.ak_updated_at and prod.ak_updated_at is not null then 1 else 0 end) as matched_ak_updated_at
,sum(case when prod.bern_id = dev.bern_id and prod.bern_id is not null then 1 else 0 end) as matched_bern_id
,sum(case when prod.bern_canvasser_id = dev.bern_canvasser_id and prod.bern_canvasser_id is not null then 1 else 0 end) as matched_bern_canvasser_id
,sum(case when prod.myc_van_id = dev.myc_van_id and prod.myc_van_id is not null then 1 else 0 end) as matched_myc_van_id
,sum(case when prod.st_myc_van_id = dev.st_myc_van_id and prod.st_myc_van_id is not null then 1 else 0 end) as matched_st_myc_van_id
,sum(case when prod.myc_dupe = dev.myc_dupe and prod.myc_dupe is not null then 1 else 0 end) as matched_myc_dupe
,sum(case when prod.myc_voter_type_id = dev.myc_voter_type_id and prod.myc_voter_type_id is not null then 1 else 0 end) as matched_myc_voter_type_id
,sum(case when prod.myc_myv_linked = dev.myc_myv_linked and prod.myc_myv_linked is not null then 1 else 0 end) as matched_myc_myv_linked
,sum(case when prod.myv_van_id = dev.myv_van_id and prod.myv_van_id is not null then 1 else 0 end) as matched_myv_van_id
,sum(case when prod.st_myv_van_id = dev.st_myv_van_id and prod.st_myv_van_id is not null then 1 else 0 end) as matched_st_myv_van_id
,sum(case when prod.email = dev.email and prod.email is not null then 1 else 0 end) as matched_email
,sum(case when prod.aux_myc_email = dev.aux_myc_email and prod.aux_myc_email is not null then 1 else 0 end) as matched_aux_myc_email
,sum(case when prod.student_hash = dev.student_hash and prod.student_hash is not null then 1 else 0 end) as matched_student_hash
,sum(case when prod.student_rn = dev.student_rn and prod.student_rn is not null then 1 else 0 end) as matched_student_rn
,sum(case when prod.state_code = dev.state_code and prod.state_code is not null then 1 else 0 end) as matched_state_code

,sum(case when prod.lalvoterid is not null then 1 else 0 end) as prodids_lalvoterid
,sum(case when prod.voters_statevoterid is not null then 1 else 0 end) as prodids_voters_statevoterid
,sum(case when prod.sos_join_key_l2 is not null then 1 else 0 end) as prodids_sos_join_key_l2
,sum(case when prod.l2_precinct is not null then 1 else 0 end) as prodids_l2_precinct
,sum(case when prod.person_id is not null then 1 else 0 end) as prodids_person_id
,sum(case when prod.voterbase_id is not null then 1 else 0 end) as prodids_voterbase_id
,sum(case when prod.dnc_precinct_id is not null then 1 else 0 end) as prodids_dnc_precinct_id
,sum(case when prod.dnc_sos_id is not null then 1 else 0 end) as prodids_dnc_sos_id
,sum(case when prod.sos_join_key_dnc is not null then 1 else 0 end) as prodids_sos_join_key_dnc
,sum(case when prod.dnc_county_voter_id is not null then 1 else 0 end) as prodids_dnc_county_voter_id
,sum(case when prod.county_fips is not null then 1 else 0 end) as prodids_county_fips
,sum(case when prod.pdiid is not null then 1 else 0 end) as prodids_pdiid
,sum(case when prod.pdi_sos_id is not null then 1 else 0 end) as prodids_pdi_sos_id
,sum(case when prod.pdi_precinct is not null then 1 else 0 end) as prodids_pdi_precinct
,sum(case when prod.actionkit_id is not null then 1 else 0 end) as prodids_actionkit_id
,sum(case when prod.ak_updated_at is not null then 1 else 0 end) as prodids_ak_updated_at
,sum(case when prod.bern_id is not null then 1 else 0 end) as prodids_bern_id
,sum(case when prod.bern_canvasser_id is not null then 1 else 0 end) as prodids_bern_canvasser_id
,sum(case when prod.myc_van_id is not null then 1 else 0 end) as prodids_myc_van_id
,sum(case when prod.st_myc_van_id is not null then 1 else 0 end) as prodids_st_myc_van_id
,sum(case when prod.myc_dupe is not null then 1 else 0 end) as prodids_myc_dupe
,sum(case when prod.myc_voter_type_id is not null then 1 else 0 end) as prodids_myc_voter_type_id
,sum(case when prod.myc_myv_linked is not null then 1 else 0 end) as prodids_myc_myv_linked
,sum(case when prod.myv_van_id is not null then 1 else 0 end) as prodids_myv_van_id
,sum(case when prod.st_myv_van_id is not null then 1 else 0 end) as prodids_st_myv_van_id
,sum(case when prod.email is not null then 1 else 0 end) as prodids_email
,sum(case when prod.aux_myc_email is not null then 1 else 0 end) as prodids_aux_myc_email
,sum(case when prod.student_hash is not null then 1 else 0 end) as prodids_student_hash
,sum(case when prod.student_rn is not null then 1 else 0 end) as prodids_student_rn
,sum(case when prod.state_code is not null then 1 else 0 end) as prodids_state_code
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
,sum(case when prod.lalvoterid = dev.lalvoterid and prod.lalvoterid is not null then 1 else 0 end) as matched_lalvoterid
,sum(case when prod.voters_statevoterid = dev.voters_statevoterid and prod.voters_statevoterid is not null then 1 else 0 end) as matched_voters_statevoterid
,sum(case when prod.sos_join_key_l2 = dev.sos_join_key_l2 and prod.sos_join_key_l2 is not null then 1 else 0 end) as matched_sos_join_key_l2
,sum(case when prod.l2_precinct = dev.l2_precinct and prod.l2_precinct is not null then 1 else 0 end) as matched_l2_precinct
,sum(case when prod.person_id = dev.person_id and prod.person_id is not null then 1 else 0 end) as matched_person_id
,sum(case when prod.voterbase_id = dev.voterbase_id and prod.voterbase_id is not null then 1 else 0 end) as matched_voterbase_id
,sum(case when prod.dnc_precinct_id = dev.dnc_precinct_id and prod.dnc_precinct_id is not null then 1 else 0 end) as matched_dnc_precinct_id
,sum(case when prod.dnc_sos_id = dev.dnc_sos_id and prod.dnc_sos_id is not null then 1 else 0 end) as matched_dnc_sos_id
,sum(case when prod.sos_join_key_dnc = dev.sos_join_key_dnc and prod.sos_join_key_dnc is not null then 1 else 0 end) as matched_sos_join_key_dnc
,sum(case when prod.dnc_county_voter_id = dev.dnc_county_voter_id and prod.dnc_county_voter_id is not null then 1 else 0 end) as matched_dnc_county_voter_id
,sum(case when prod.county_fips = dev.county_fips and prod.county_fips is not null then 1 else 0 end) as matched_county_fips
,sum(case when prod.pdiid = dev.pdiid and prod.pdiid is not null then 1 else 0 end) as matched_pdiid
,sum(case when prod.pdi_sos_id = dev.pdi_sos_id and prod.pdi_sos_id is not null then 1 else 0 end) as matched_pdi_sos_id
,sum(case when prod.pdi_precinct = dev.pdi_precinct and prod.pdi_precinct is not null then 1 else 0 end) as matched_pdi_precinct
,sum(case when prod.actionkit_id = dev.actionkit_id and prod.actionkit_id is not null then 1 else 0 end) as matched_actionkit_id
,sum(case when prod.ak_updated_at = dev.ak_updated_at and prod.ak_updated_at is not null then 1 else 0 end) as matched_ak_updated_at
,sum(case when prod.bern_id = dev.bern_id and prod.bern_id is not null then 1 else 0 end) as matched_bern_id
,sum(case when prod.bern_canvasser_id = dev.bern_canvasser_id and prod.bern_canvasser_id is not null then 1 else 0 end) as matched_bern_canvasser_id
,sum(case when prod.myc_van_id = dev.myc_van_id and prod.myc_van_id is not null then 1 else 0 end) as matched_myc_van_id
,sum(case when prod.st_myc_van_id = dev.st_myc_van_id and prod.st_myc_van_id is not null then 1 else 0 end) as matched_st_myc_van_id
,sum(case when prod.myc_dupe = dev.myc_dupe and prod.myc_dupe is not null then 1 else 0 end) as matched_myc_dupe
,sum(case when prod.myc_voter_type_id = dev.myc_voter_type_id and prod.myc_voter_type_id is not null then 1 else 0 end) as matched_myc_voter_type_id
,sum(case when prod.myc_myv_linked = dev.myc_myv_linked and prod.myc_myv_linked is not null then 1 else 0 end) as matched_myc_myv_linked
,sum(case when prod.myv_van_id = dev.myv_van_id and prod.myv_van_id is not null then 1 else 0 end) as matched_myv_van_id
,sum(case when prod.st_myv_van_id = dev.st_myv_van_id and prod.st_myv_van_id is not null then 1 else 0 end) as matched_st_myv_van_id
,sum(case when prod.email = dev.email and prod.email is not null then 1 else 0 end) as matched_email
,sum(case when prod.aux_myc_email = dev.aux_myc_email and prod.aux_myc_email is not null then 1 else 0 end) as matched_aux_myc_email
,sum(case when prod.student_hash = dev.student_hash and prod.student_hash is not null then 1 else 0 end) as matched_student_hash
,sum(case when prod.student_rn = dev.student_rn and prod.student_rn is not null then 1 else 0 end) as matched_student_rn
,sum(case when prod.state_code = dev.state_code and prod.state_code is not null then 1 else 0 end) as matched_state_code

,sum(case when prod.lalvoterid is not null then 1 else 0 end) as prodids_lalvoterid
,sum(case when prod.voters_statevoterid is not null then 1 else 0 end) as prodids_voters_statevoterid
,sum(case when prod.sos_join_key_l2 is not null then 1 else 0 end) as prodids_sos_join_key_l2
,sum(case when prod.l2_precinct is not null then 1 else 0 end) as prodids_l2_precinct
,sum(case when prod.person_id is not null then 1 else 0 end) as prodids_person_id
,sum(case when prod.voterbase_id is not null then 1 else 0 end) as prodids_voterbase_id
,sum(case when prod.dnc_precinct_id is not null then 1 else 0 end) as prodids_dnc_precinct_id
,sum(case when prod.dnc_sos_id is not null then 1 else 0 end) as prodids_dnc_sos_id
,sum(case when prod.sos_join_key_dnc is not null then 1 else 0 end) as prodids_sos_join_key_dnc
,sum(case when prod.dnc_county_voter_id is not null then 1 else 0 end) as prodids_dnc_county_voter_id
,sum(case when prod.county_fips is not null then 1 else 0 end) as prodids_county_fips
,sum(case when prod.pdiid is not null then 1 else 0 end) as prodids_pdiid
,sum(case when prod.pdi_sos_id is not null then 1 else 0 end) as prodids_pdi_sos_id
,sum(case when prod.pdi_precinct is not null then 1 else 0 end) as prodids_pdi_precinct
,sum(case when prod.actionkit_id is not null then 1 else 0 end) as prodids_actionkit_id
,sum(case when prod.ak_updated_at is not null then 1 else 0 end) as prodids_ak_updated_at
,sum(case when prod.bern_id is not null then 1 else 0 end) as prodids_bern_id
,sum(case when prod.bern_canvasser_id is not null then 1 else 0 end) as prodids_bern_canvasser_id
,sum(case when prod.myc_van_id is not null then 1 else 0 end) as prodids_myc_van_id
,sum(case when prod.st_myc_van_id is not null then 1 else 0 end) as prodids_st_myc_van_id
,sum(case when prod.myc_dupe is not null then 1 else 0 end) as prodids_myc_dupe
,sum(case when prod.myc_voter_type_id is not null then 1 else 0 end) as prodids_myc_voter_type_id
,sum(case when prod.myc_myv_linked is not null then 1 else 0 end) as prodids_myc_myv_linked
,sum(case when prod.myv_van_id is not null then 1 else 0 end) as prodids_myv_van_id
,sum(case when prod.st_myv_van_id is not null then 1 else 0 end) as prodids_st_myv_van_id
,sum(case when prod.email is not null then 1 else 0 end) as prodids_email
,sum(case when prod.aux_myc_email is not null then 1 else 0 end) as prodids_aux_myc_email
,sum(case when prod.student_hash is not null then 1 else 0 end) as prodids_student_hash
,sum(case when prod.student_rn is not null then 1 else 0 end) as prodids_student_rn
,sum(case when prod.state_code is not null then 1 else 0 end) as prodids_state_code
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
,sum(case when prod.lalvoterid = dev.lalvoterid and prod.lalvoterid is not null then 1 else 0 end) as matched_lalvoterid
,sum(case when prod.voters_statevoterid = dev.voters_statevoterid and prod.voters_statevoterid is not null then 1 else 0 end) as matched_voters_statevoterid
,sum(case when prod.sos_join_key_l2 = dev.sos_join_key_l2 and prod.sos_join_key_l2 is not null then 1 else 0 end) as matched_sos_join_key_l2
,sum(case when prod.l2_precinct = dev.l2_precinct and prod.l2_precinct is not null then 1 else 0 end) as matched_l2_precinct
,sum(case when prod.person_id = dev.person_id and prod.person_id is not null then 1 else 0 end) as matched_person_id
,sum(case when prod.voterbase_id = dev.voterbase_id and prod.voterbase_id is not null then 1 else 0 end) as matched_voterbase_id
,sum(case when prod.dnc_precinct_id = dev.dnc_precinct_id and prod.dnc_precinct_id is not null then 1 else 0 end) as matched_dnc_precinct_id
,sum(case when prod.dnc_sos_id = dev.dnc_sos_id and prod.dnc_sos_id is not null then 1 else 0 end) as matched_dnc_sos_id
,sum(case when prod.sos_join_key_dnc = dev.sos_join_key_dnc and prod.sos_join_key_dnc is not null then 1 else 0 end) as matched_sos_join_key_dnc
,sum(case when prod.dnc_county_voter_id = dev.dnc_county_voter_id and prod.dnc_county_voter_id is not null then 1 else 0 end) as matched_dnc_county_voter_id
,sum(case when prod.county_fips = dev.county_fips and prod.county_fips is not null then 1 else 0 end) as matched_county_fips
,sum(case when prod.pdiid = dev.pdiid and prod.pdiid is not null then 1 else 0 end) as matched_pdiid
,sum(case when prod.pdi_sos_id = dev.pdi_sos_id and prod.pdi_sos_id is not null then 1 else 0 end) as matched_pdi_sos_id
,sum(case when prod.pdi_precinct = dev.pdi_precinct and prod.pdi_precinct is not null then 1 else 0 end) as matched_pdi_precinct
,sum(case when prod.actionkit_id = dev.actionkit_id and prod.actionkit_id is not null then 1 else 0 end) as matched_actionkit_id
,sum(case when prod.ak_updated_at = dev.ak_updated_at and prod.ak_updated_at is not null then 1 else 0 end) as matched_ak_updated_at
,sum(case when prod.bern_id = dev.bern_id and prod.bern_id is not null then 1 else 0 end) as matched_bern_id
,sum(case when prod.bern_canvasser_id = dev.bern_canvasser_id and prod.bern_canvasser_id is not null then 1 else 0 end) as matched_bern_canvasser_id
,sum(case when prod.myc_van_id = dev.myc_van_id and prod.myc_van_id is not null then 1 else 0 end) as matched_myc_van_id
,sum(case when prod.st_myc_van_id = dev.st_myc_van_id and prod.st_myc_van_id is not null then 1 else 0 end) as matched_st_myc_van_id
,sum(case when prod.myc_dupe = dev.myc_dupe and prod.myc_dupe is not null then 1 else 0 end) as matched_myc_dupe
,sum(case when prod.myc_voter_type_id = dev.myc_voter_type_id and prod.myc_voter_type_id is not null then 1 else 0 end) as matched_myc_voter_type_id
,sum(case when prod.myc_myv_linked = dev.myc_myv_linked and prod.myc_myv_linked is not null then 1 else 0 end) as matched_myc_myv_linked
,sum(case when prod.myv_van_id = dev.myv_van_id and prod.myv_van_id is not null then 1 else 0 end) as matched_myv_van_id
,sum(case when prod.st_myv_van_id = dev.st_myv_van_id and prod.st_myv_van_id is not null then 1 else 0 end) as matched_st_myv_van_id
,sum(case when prod.email = dev.email and prod.email is not null then 1 else 0 end) as matched_email
,sum(case when prod.aux_myc_email = dev.aux_myc_email and prod.aux_myc_email is not null then 1 else 0 end) as matched_aux_myc_email
,sum(case when prod.student_hash = dev.student_hash and prod.student_hash is not null then 1 else 0 end) as matched_student_hash
,sum(case when prod.student_rn = dev.student_rn and prod.student_rn is not null then 1 else 0 end) as matched_student_rn
,sum(case when prod.state_code = dev.state_code and prod.state_code is not null then 1 else 0 end) as matched_state_code

,sum(case when prod.lalvoterid is not null then 1 else 0 end) as prodids_lalvoterid
,sum(case when prod.voters_statevoterid is not null then 1 else 0 end) as prodids_voters_statevoterid
,sum(case when prod.sos_join_key_l2 is not null then 1 else 0 end) as prodids_sos_join_key_l2
,sum(case when prod.l2_precinct is not null then 1 else 0 end) as prodids_l2_precinct
,sum(case when prod.person_id is not null then 1 else 0 end) as prodids_person_id
,sum(case when prod.voterbase_id is not null then 1 else 0 end) as prodids_voterbase_id
,sum(case when prod.dnc_precinct_id is not null then 1 else 0 end) as prodids_dnc_precinct_id
,sum(case when prod.dnc_sos_id is not null then 1 else 0 end) as prodids_dnc_sos_id
,sum(case when prod.sos_join_key_dnc is not null then 1 else 0 end) as prodids_sos_join_key_dnc
,sum(case when prod.dnc_county_voter_id is not null then 1 else 0 end) as prodids_dnc_county_voter_id
,sum(case when prod.county_fips is not null then 1 else 0 end) as prodids_county_fips
,sum(case when prod.pdiid is not null then 1 else 0 end) as prodids_pdiid
,sum(case when prod.pdi_sos_id is not null then 1 else 0 end) as prodids_pdi_sos_id
,sum(case when prod.pdi_precinct is not null then 1 else 0 end) as prodids_pdi_precinct
,sum(case when prod.actionkit_id is not null then 1 else 0 end) as prodids_actionkit_id
,sum(case when prod.ak_updated_at is not null then 1 else 0 end) as prodids_ak_updated_at
,sum(case when prod.bern_id is not null then 1 else 0 end) as prodids_bern_id
,sum(case when prod.bern_canvasser_id is not null then 1 else 0 end) as prodids_bern_canvasser_id
,sum(case when prod.myc_van_id is not null then 1 else 0 end) as prodids_myc_van_id
,sum(case when prod.st_myc_van_id is not null then 1 else 0 end) as prodids_st_myc_van_id
,sum(case when prod.myc_dupe is not null then 1 else 0 end) as prodids_myc_dupe
,sum(case when prod.myc_voter_type_id is not null then 1 else 0 end) as prodids_myc_voter_type_id
,sum(case when prod.myc_myv_linked is not null then 1 else 0 end) as prodids_myc_myv_linked
,sum(case when prod.myv_van_id is not null then 1 else 0 end) as prodids_myv_van_id
,sum(case when prod.st_myv_van_id is not null then 1 else 0 end) as prodids_st_myv_van_id
,sum(case when prod.email is not null then 1 else 0 end) as prodids_email
,sum(case when prod.aux_myc_email is not null then 1 else 0 end) as prodids_aux_myc_email
,sum(case when prod.student_hash is not null then 1 else 0 end) as prodids_student_hash
,sum(case when prod.student_rn is not null then 1 else 0 end) as prodids_student_rn
,sum(case when prod.state_code is not null then 1 else 0 end) as prodids_state_code
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
,sum(case when prod.lalvoterid = dev.lalvoterid and prod.lalvoterid is not null then 1 else 0 end) as matched_lalvoterid
,sum(case when prod.voters_statevoterid = dev.voters_statevoterid and prod.voters_statevoterid is not null then 1 else 0 end) as matched_voters_statevoterid
,sum(case when prod.sos_join_key_l2 = dev.sos_join_key_l2 and prod.sos_join_key_l2 is not null then 1 else 0 end) as matched_sos_join_key_l2
,sum(case when prod.l2_precinct = dev.l2_precinct and prod.l2_precinct is not null then 1 else 0 end) as matched_l2_precinct
,sum(case when prod.person_id = dev.person_id and prod.person_id is not null then 1 else 0 end) as matched_person_id
,sum(case when prod.voterbase_id = dev.voterbase_id and prod.voterbase_id is not null then 1 else 0 end) as matched_voterbase_id
,sum(case when prod.dnc_precinct_id = dev.dnc_precinct_id and prod.dnc_precinct_id is not null then 1 else 0 end) as matched_dnc_precinct_id
,sum(case when prod.dnc_sos_id = dev.dnc_sos_id and prod.dnc_sos_id is not null then 1 else 0 end) as matched_dnc_sos_id
,sum(case when prod.sos_join_key_dnc = dev.sos_join_key_dnc and prod.sos_join_key_dnc is not null then 1 else 0 end) as matched_sos_join_key_dnc
,sum(case when prod.dnc_county_voter_id = dev.dnc_county_voter_id and prod.dnc_county_voter_id is not null then 1 else 0 end) as matched_dnc_county_voter_id
,sum(case when prod.county_fips = dev.county_fips and prod.county_fips is not null then 1 else 0 end) as matched_county_fips
,sum(case when prod.pdiid = dev.pdiid and prod.pdiid is not null then 1 else 0 end) as matched_pdiid
,sum(case when prod.pdi_sos_id = dev.pdi_sos_id and prod.pdi_sos_id is not null then 1 else 0 end) as matched_pdi_sos_id
,sum(case when prod.pdi_precinct = dev.pdi_precinct and prod.pdi_precinct is not null then 1 else 0 end) as matched_pdi_precinct
,sum(case when prod.actionkit_id = dev.actionkit_id and prod.actionkit_id is not null then 1 else 0 end) as matched_actionkit_id
,sum(case when prod.ak_updated_at = dev.ak_updated_at and prod.ak_updated_at is not null then 1 else 0 end) as matched_ak_updated_at
,sum(case when prod.bern_id = dev.bern_id and prod.bern_id is not null then 1 else 0 end) as matched_bern_id
,sum(case when prod.bern_canvasser_id = dev.bern_canvasser_id and prod.bern_canvasser_id is not null then 1 else 0 end) as matched_bern_canvasser_id
,sum(case when prod.myc_van_id = dev.myc_van_id and prod.myc_van_id is not null then 1 else 0 end) as matched_myc_van_id
,sum(case when prod.st_myc_van_id = dev.st_myc_van_id and prod.st_myc_van_id is not null then 1 else 0 end) as matched_st_myc_van_id
,sum(case when prod.myc_dupe = dev.myc_dupe and prod.myc_dupe is not null then 1 else 0 end) as matched_myc_dupe
,sum(case when prod.myc_voter_type_id = dev.myc_voter_type_id and prod.myc_voter_type_id is not null then 1 else 0 end) as matched_myc_voter_type_id
,sum(case when prod.myc_myv_linked = dev.myc_myv_linked and prod.myc_myv_linked is not null then 1 else 0 end) as matched_myc_myv_linked
,sum(case when prod.myv_van_id = dev.myv_van_id and prod.myv_van_id is not null then 1 else 0 end) as matched_myv_van_id
,sum(case when prod.st_myv_van_id = dev.st_myv_van_id and prod.st_myv_van_id is not null then 1 else 0 end) as matched_st_myv_van_id
,sum(case when prod.email = dev.email and prod.email is not null then 1 else 0 end) as matched_email
,sum(case when prod.aux_myc_email = dev.aux_myc_email and prod.aux_myc_email is not null then 1 else 0 end) as matched_aux_myc_email
,sum(case when prod.student_hash = dev.student_hash and prod.student_hash is not null then 1 else 0 end) as matched_student_hash
,sum(case when prod.student_rn = dev.student_rn and prod.student_rn is not null then 1 else 0 end) as matched_student_rn
,sum(case when prod.state_code = dev.state_code and prod.state_code is not null then 1 else 0 end) as matched_state_code

,sum(case when prod.lalvoterid is not null then 1 else 0 end) as prodids_lalvoterid
,sum(case when prod.voters_statevoterid is not null then 1 else 0 end) as prodids_voters_statevoterid
,sum(case when prod.sos_join_key_l2 is not null then 1 else 0 end) as prodids_sos_join_key_l2
,sum(case when prod.l2_precinct is not null then 1 else 0 end) as prodids_l2_precinct
,sum(case when prod.person_id is not null then 1 else 0 end) as prodids_person_id
,sum(case when prod.voterbase_id is not null then 1 else 0 end) as prodids_voterbase_id
,sum(case when prod.dnc_precinct_id is not null then 1 else 0 end) as prodids_dnc_precinct_id
,sum(case when prod.dnc_sos_id is not null then 1 else 0 end) as prodids_dnc_sos_id
,sum(case when prod.sos_join_key_dnc is not null then 1 else 0 end) as prodids_sos_join_key_dnc
,sum(case when prod.dnc_county_voter_id is not null then 1 else 0 end) as prodids_dnc_county_voter_id
,sum(case when prod.county_fips is not null then 1 else 0 end) as prodids_county_fips
,sum(case when prod.pdiid is not null then 1 else 0 end) as prodids_pdiid
,sum(case when prod.pdi_sos_id is not null then 1 else 0 end) as prodids_pdi_sos_id
,sum(case when prod.pdi_precinct is not null then 1 else 0 end) as prodids_pdi_precinct
,sum(case when prod.actionkit_id is not null then 1 else 0 end) as prodids_actionkit_id
,sum(case when prod.ak_updated_at is not null then 1 else 0 end) as prodids_ak_updated_at
,sum(case when prod.bern_id is not null then 1 else 0 end) as prodids_bern_id
,sum(case when prod.bern_canvasser_id is not null then 1 else 0 end) as prodids_bern_canvasser_id
,sum(case when prod.myc_van_id is not null then 1 else 0 end) as prodids_myc_van_id
,sum(case when prod.st_myc_van_id is not null then 1 else 0 end) as prodids_st_myc_van_id
,sum(case when prod.myc_dupe is not null then 1 else 0 end) as prodids_myc_dupe
,sum(case when prod.myc_voter_type_id is not null then 1 else 0 end) as prodids_myc_voter_type_id
,sum(case when prod.myc_myv_linked is not null then 1 else 0 end) as prodids_myc_myv_linked
,sum(case when prod.myv_van_id is not null then 1 else 0 end) as prodids_myv_van_id
,sum(case when prod.st_myv_van_id is not null then 1 else 0 end) as prodids_st_myv_van_id
,sum(case when prod.email is not null then 1 else 0 end) as prodids_email
,sum(case when prod.aux_myc_email is not null then 1 else 0 end) as prodids_aux_myc_email
,sum(case when prod.student_hash is not null then 1 else 0 end) as prodids_student_hash
,sum(case when prod.student_rn is not null then 1 else 0 end) as prodids_student_rn
,sum(case when prod.state_code is not null then 1 else 0 end) as prodids_state_code
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
,sum(case when prod.lalvoterid = dev.lalvoterid and prod.lalvoterid is not null then 1 else 0 end) as matched_lalvoterid
,sum(case when prod.voters_statevoterid = dev.voters_statevoterid and prod.voters_statevoterid is not null then 1 else 0 end) as matched_voters_statevoterid
,sum(case when prod.sos_join_key_l2 = dev.sos_join_key_l2 and prod.sos_join_key_l2 is not null then 1 else 0 end) as matched_sos_join_key_l2
,sum(case when prod.l2_precinct = dev.l2_precinct and prod.l2_precinct is not null then 1 else 0 end) as matched_l2_precinct
,sum(case when prod.person_id = dev.person_id and prod.person_id is not null then 1 else 0 end) as matched_person_id
,sum(case when prod.voterbase_id = dev.voterbase_id and prod.voterbase_id is not null then 1 else 0 end) as matched_voterbase_id
,sum(case when prod.dnc_precinct_id = dev.dnc_precinct_id and prod.dnc_precinct_id is not null then 1 else 0 end) as matched_dnc_precinct_id
,sum(case when prod.dnc_sos_id = dev.dnc_sos_id and prod.dnc_sos_id is not null then 1 else 0 end) as matched_dnc_sos_id
,sum(case when prod.sos_join_key_dnc = dev.sos_join_key_dnc and prod.sos_join_key_dnc is not null then 1 else 0 end) as matched_sos_join_key_dnc
,sum(case when prod.dnc_county_voter_id = dev.dnc_county_voter_id and prod.dnc_county_voter_id is not null then 1 else 0 end) as matched_dnc_county_voter_id
,sum(case when prod.county_fips = dev.county_fips and prod.county_fips is not null then 1 else 0 end) as matched_county_fips
,sum(case when prod.pdiid = dev.pdiid and prod.pdiid is not null then 1 else 0 end) as matched_pdiid
,sum(case when prod.pdi_sos_id = dev.pdi_sos_id and prod.pdi_sos_id is not null then 1 else 0 end) as matched_pdi_sos_id
,sum(case when prod.pdi_precinct = dev.pdi_precinct and prod.pdi_precinct is not null then 1 else 0 end) as matched_pdi_precinct
,sum(case when prod.actionkit_id = dev.actionkit_id and prod.actionkit_id is not null then 1 else 0 end) as matched_actionkit_id
,sum(case when prod.ak_updated_at = dev.ak_updated_at and prod.ak_updated_at is not null then 1 else 0 end) as matched_ak_updated_at
,sum(case when prod.bern_id = dev.bern_id and prod.bern_id is not null then 1 else 0 end) as matched_bern_id
,sum(case when prod.bern_canvasser_id = dev.bern_canvasser_id and prod.bern_canvasser_id is not null then 1 else 0 end) as matched_bern_canvasser_id
,sum(case when prod.myc_van_id = dev.myc_van_id and prod.myc_van_id is not null then 1 else 0 end) as matched_myc_van_id
,sum(case when prod.st_myc_van_id = dev.st_myc_van_id and prod.st_myc_van_id is not null then 1 else 0 end) as matched_st_myc_van_id
,sum(case when prod.myc_dupe = dev.myc_dupe and prod.myc_dupe is not null then 1 else 0 end) as matched_myc_dupe
,sum(case when prod.myc_voter_type_id = dev.myc_voter_type_id and prod.myc_voter_type_id is not null then 1 else 0 end) as matched_myc_voter_type_id
,sum(case when prod.myc_myv_linked = dev.myc_myv_linked and prod.myc_myv_linked is not null then 1 else 0 end) as matched_myc_myv_linked
,sum(case when prod.myv_van_id = dev.myv_van_id and prod.myv_van_id is not null then 1 else 0 end) as matched_myv_van_id
,sum(case when prod.st_myv_van_id = dev.st_myv_van_id and prod.st_myv_van_id is not null then 1 else 0 end) as matched_st_myv_van_id
,sum(case when prod.email = dev.email and prod.email is not null then 1 else 0 end) as matched_email
,sum(case when prod.aux_myc_email = dev.aux_myc_email and prod.aux_myc_email is not null then 1 else 0 end) as matched_aux_myc_email
,sum(case when prod.student_hash = dev.student_hash and prod.student_hash is not null then 1 else 0 end) as matched_student_hash
,sum(case when prod.student_rn = dev.student_rn and prod.student_rn is not null then 1 else 0 end) as matched_student_rn
,sum(case when prod.state_code = dev.state_code and prod.state_code is not null then 1 else 0 end) as matched_state_code

,sum(case when prod.lalvoterid is not null then 1 else 0 end) as prodids_lalvoterid
,sum(case when prod.voters_statevoterid is not null then 1 else 0 end) as prodids_voters_statevoterid
,sum(case when prod.sos_join_key_l2 is not null then 1 else 0 end) as prodids_sos_join_key_l2
,sum(case when prod.l2_precinct is not null then 1 else 0 end) as prodids_l2_precinct
,sum(case when prod.person_id is not null then 1 else 0 end) as prodids_person_id
,sum(case when prod.voterbase_id is not null then 1 else 0 end) as prodids_voterbase_id
,sum(case when prod.dnc_precinct_id is not null then 1 else 0 end) as prodids_dnc_precinct_id
,sum(case when prod.dnc_sos_id is not null then 1 else 0 end) as prodids_dnc_sos_id
,sum(case when prod.sos_join_key_dnc is not null then 1 else 0 end) as prodids_sos_join_key_dnc
,sum(case when prod.dnc_county_voter_id is not null then 1 else 0 end) as prodids_dnc_county_voter_id
,sum(case when prod.county_fips is not null then 1 else 0 end) as prodids_county_fips
,sum(case when prod.pdiid is not null then 1 else 0 end) as prodids_pdiid
,sum(case when prod.pdi_sos_id is not null then 1 else 0 end) as prodids_pdi_sos_id
,sum(case when prod.pdi_precinct is not null then 1 else 0 end) as prodids_pdi_precinct
,sum(case when prod.actionkit_id is not null then 1 else 0 end) as prodids_actionkit_id
,sum(case when prod.ak_updated_at is not null then 1 else 0 end) as prodids_ak_updated_at
,sum(case when prod.bern_id is not null then 1 else 0 end) as prodids_bern_id
,sum(case when prod.bern_canvasser_id is not null then 1 else 0 end) as prodids_bern_canvasser_id
,sum(case when prod.myc_van_id is not null then 1 else 0 end) as prodids_myc_van_id
,sum(case when prod.st_myc_van_id is not null then 1 else 0 end) as prodids_st_myc_van_id
,sum(case when prod.myc_dupe is not null then 1 else 0 end) as prodids_myc_dupe
,sum(case when prod.myc_voter_type_id is not null then 1 else 0 end) as prodids_myc_voter_type_id
,sum(case when prod.myc_myv_linked is not null then 1 else 0 end) as prodids_myc_myv_linked
,sum(case when prod.myv_van_id is not null then 1 else 0 end) as prodids_myv_van_id
,sum(case when prod.st_myv_van_id is not null then 1 else 0 end) as prodids_st_myv_van_id
,sum(case when prod.email is not null then 1 else 0 end) as prodids_email
,sum(case when prod.aux_myc_email is not null then 1 else 0 end) as prodids_aux_myc_email
,sum(case when prod.student_hash is not null then 1 else 0 end) as prodids_student_hash
,sum(case when prod.student_rn is not null then 1 else 0 end) as prodids_student_rn
,sum(case when prod.state_code is not null then 1 else 0 end) as prodids_state_code
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
,sum(case when prod.lalvoterid = dev.lalvoterid and prod.lalvoterid is not null then 1 else 0 end) as matched_lalvoterid
,sum(case when prod.voters_statevoterid = dev.voters_statevoterid and prod.voters_statevoterid is not null then 1 else 0 end) as matched_voters_statevoterid
,sum(case when prod.sos_join_key_l2 = dev.sos_join_key_l2 and prod.sos_join_key_l2 is not null then 1 else 0 end) as matched_sos_join_key_l2
,sum(case when prod.l2_precinct = dev.l2_precinct and prod.l2_precinct is not null then 1 else 0 end) as matched_l2_precinct
,sum(case when prod.person_id = dev.person_id and prod.person_id is not null then 1 else 0 end) as matched_person_id
,sum(case when prod.voterbase_id = dev.voterbase_id and prod.voterbase_id is not null then 1 else 0 end) as matched_voterbase_id
,sum(case when prod.dnc_precinct_id = dev.dnc_precinct_id and prod.dnc_precinct_id is not null then 1 else 0 end) as matched_dnc_precinct_id
,sum(case when prod.dnc_sos_id = dev.dnc_sos_id and prod.dnc_sos_id is not null then 1 else 0 end) as matched_dnc_sos_id
,sum(case when prod.sos_join_key_dnc = dev.sos_join_key_dnc and prod.sos_join_key_dnc is not null then 1 else 0 end) as matched_sos_join_key_dnc
,sum(case when prod.dnc_county_voter_id = dev.dnc_county_voter_id and prod.dnc_county_voter_id is not null then 1 else 0 end) as matched_dnc_county_voter_id
,sum(case when prod.county_fips = dev.county_fips and prod.county_fips is not null then 1 else 0 end) as matched_county_fips
,sum(case when prod.pdiid = dev.pdiid and prod.pdiid is not null then 1 else 0 end) as matched_pdiid
,sum(case when prod.pdi_sos_id = dev.pdi_sos_id and prod.pdi_sos_id is not null then 1 else 0 end) as matched_pdi_sos_id
,sum(case when prod.pdi_precinct = dev.pdi_precinct and prod.pdi_precinct is not null then 1 else 0 end) as matched_pdi_precinct
,sum(case when prod.actionkit_id = dev.actionkit_id and prod.actionkit_id is not null then 1 else 0 end) as matched_actionkit_id
,sum(case when prod.ak_updated_at = dev.ak_updated_at and prod.ak_updated_at is not null then 1 else 0 end) as matched_ak_updated_at
,sum(case when prod.bern_id = dev.bern_id and prod.bern_id is not null then 1 else 0 end) as matched_bern_id
,sum(case when prod.bern_canvasser_id = dev.bern_canvasser_id and prod.bern_canvasser_id is not null then 1 else 0 end) as matched_bern_canvasser_id
,sum(case when prod.myc_van_id = dev.myc_van_id and prod.myc_van_id is not null then 1 else 0 end) as matched_myc_van_id
,sum(case when prod.st_myc_van_id = dev.st_myc_van_id and prod.st_myc_van_id is not null then 1 else 0 end) as matched_st_myc_van_id
,sum(case when prod.myc_dupe = dev.myc_dupe and prod.myc_dupe is not null then 1 else 0 end) as matched_myc_dupe
,sum(case when prod.myc_voter_type_id = dev.myc_voter_type_id and prod.myc_voter_type_id is not null then 1 else 0 end) as matched_myc_voter_type_id
,sum(case when prod.myc_myv_linked = dev.myc_myv_linked and prod.myc_myv_linked is not null then 1 else 0 end) as matched_myc_myv_linked
,sum(case when prod.myv_van_id = dev.myv_van_id and prod.myv_van_id is not null then 1 else 0 end) as matched_myv_van_id
,sum(case when prod.st_myv_van_id = dev.st_myv_van_id and prod.st_myv_van_id is not null then 1 else 0 end) as matched_st_myv_van_id
,sum(case when prod.email = dev.email and prod.email is not null then 1 else 0 end) as matched_email
,sum(case when prod.aux_myc_email = dev.aux_myc_email and prod.aux_myc_email is not null then 1 else 0 end) as matched_aux_myc_email
,sum(case when prod.student_hash = dev.student_hash and prod.student_hash is not null then 1 else 0 end) as matched_student_hash
,sum(case when prod.student_rn = dev.student_rn and prod.student_rn is not null then 1 else 0 end) as matched_student_rn
,sum(case when prod.state_code = dev.state_code and prod.state_code is not null then 1 else 0 end) as matched_state_code

,sum(case when prod.lalvoterid is not null then 1 else 0 end) as prodids_lalvoterid
,sum(case when prod.voters_statevoterid is not null then 1 else 0 end) as prodids_voters_statevoterid
,sum(case when prod.sos_join_key_l2 is not null then 1 else 0 end) as prodids_sos_join_key_l2
,sum(case when prod.l2_precinct is not null then 1 else 0 end) as prodids_l2_precinct
,sum(case when prod.person_id is not null then 1 else 0 end) as prodids_person_id
,sum(case when prod.voterbase_id is not null then 1 else 0 end) as prodids_voterbase_id
,sum(case when prod.dnc_precinct_id is not null then 1 else 0 end) as prodids_dnc_precinct_id
,sum(case when prod.dnc_sos_id is not null then 1 else 0 end) as prodids_dnc_sos_id
,sum(case when prod.sos_join_key_dnc is not null then 1 else 0 end) as prodids_sos_join_key_dnc
,sum(case when prod.dnc_county_voter_id is not null then 1 else 0 end) as prodids_dnc_county_voter_id
,sum(case when prod.county_fips is not null then 1 else 0 end) as prodids_county_fips
,sum(case when prod.pdiid is not null then 1 else 0 end) as prodids_pdiid
,sum(case when prod.pdi_sos_id is not null then 1 else 0 end) as prodids_pdi_sos_id
,sum(case when prod.pdi_precinct is not null then 1 else 0 end) as prodids_pdi_precinct
,sum(case when prod.actionkit_id is not null then 1 else 0 end) as prodids_actionkit_id
,sum(case when prod.ak_updated_at is not null then 1 else 0 end) as prodids_ak_updated_at
,sum(case when prod.bern_id is not null then 1 else 0 end) as prodids_bern_id
,sum(case when prod.bern_canvasser_id is not null then 1 else 0 end) as prodids_bern_canvasser_id
,sum(case when prod.myc_van_id is not null then 1 else 0 end) as prodids_myc_van_id
,sum(case when prod.st_myc_van_id is not null then 1 else 0 end) as prodids_st_myc_van_id
,sum(case when prod.myc_dupe is not null then 1 else 0 end) as prodids_myc_dupe
,sum(case when prod.myc_voter_type_id is not null then 1 else 0 end) as prodids_myc_voter_type_id
,sum(case when prod.myc_myv_linked is not null then 1 else 0 end) as prodids_myc_myv_linked
,sum(case when prod.myv_van_id is not null then 1 else 0 end) as prodids_myv_van_id
,sum(case when prod.st_myv_van_id is not null then 1 else 0 end) as prodids_st_myv_van_id
,sum(case when prod.email is not null then 1 else 0 end) as prodids_email
,sum(case when prod.aux_myc_email is not null then 1 else 0 end) as prodids_aux_myc_email
,sum(case when prod.student_hash is not null then 1 else 0 end) as prodids_student_hash
,sum(case when prod.student_rn is not null then 1 else 0 end) as prodids_student_rn
,sum(case when prod.state_code is not null then 1 else 0 end) as prodids_state_code
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
,sum(case when prod.lalvoterid = dev.lalvoterid and prod.lalvoterid is not null then 1 else 0 end) as matched_lalvoterid
,sum(case when prod.voters_statevoterid = dev.voters_statevoterid and prod.voters_statevoterid is not null then 1 else 0 end) as matched_voters_statevoterid
,sum(case when prod.sos_join_key_l2 = dev.sos_join_key_l2 and prod.sos_join_key_l2 is not null then 1 else 0 end) as matched_sos_join_key_l2
,sum(case when prod.l2_precinct = dev.l2_precinct and prod.l2_precinct is not null then 1 else 0 end) as matched_l2_precinct
,sum(case when prod.person_id = dev.person_id and prod.person_id is not null then 1 else 0 end) as matched_person_id
,sum(case when prod.voterbase_id = dev.voterbase_id and prod.voterbase_id is not null then 1 else 0 end) as matched_voterbase_id
,sum(case when prod.dnc_precinct_id = dev.dnc_precinct_id and prod.dnc_precinct_id is not null then 1 else 0 end) as matched_dnc_precinct_id
,sum(case when prod.dnc_sos_id = dev.dnc_sos_id and prod.dnc_sos_id is not null then 1 else 0 end) as matched_dnc_sos_id
,sum(case when prod.sos_join_key_dnc = dev.sos_join_key_dnc and prod.sos_join_key_dnc is not null then 1 else 0 end) as matched_sos_join_key_dnc
,sum(case when prod.dnc_county_voter_id = dev.dnc_county_voter_id and prod.dnc_county_voter_id is not null then 1 else 0 end) as matched_dnc_county_voter_id
,sum(case when prod.county_fips = dev.county_fips and prod.county_fips is not null then 1 else 0 end) as matched_county_fips
,sum(case when prod.pdiid = dev.pdiid and prod.pdiid is not null then 1 else 0 end) as matched_pdiid
,sum(case when prod.pdi_sos_id = dev.pdi_sos_id and prod.pdi_sos_id is not null then 1 else 0 end) as matched_pdi_sos_id
,sum(case when prod.pdi_precinct = dev.pdi_precinct and prod.pdi_precinct is not null then 1 else 0 end) as matched_pdi_precinct
,sum(case when prod.actionkit_id = dev.actionkit_id and prod.actionkit_id is not null then 1 else 0 end) as matched_actionkit_id
,sum(case when prod.ak_updated_at = dev.ak_updated_at and prod.ak_updated_at is not null then 1 else 0 end) as matched_ak_updated_at
,sum(case when prod.bern_id = dev.bern_id and prod.bern_id is not null then 1 else 0 end) as matched_bern_id
,sum(case when prod.bern_canvasser_id = dev.bern_canvasser_id and prod.bern_canvasser_id is not null then 1 else 0 end) as matched_bern_canvasser_id
,sum(case when prod.myc_van_id = dev.myc_van_id and prod.myc_van_id is not null then 1 else 0 end) as matched_myc_van_id
,sum(case when prod.st_myc_van_id = dev.st_myc_van_id and prod.st_myc_van_id is not null then 1 else 0 end) as matched_st_myc_van_id
,sum(case when prod.myc_dupe = dev.myc_dupe and prod.myc_dupe is not null then 1 else 0 end) as matched_myc_dupe
,sum(case when prod.myc_voter_type_id = dev.myc_voter_type_id and prod.myc_voter_type_id is not null then 1 else 0 end) as matched_myc_voter_type_id
,sum(case when prod.myc_myv_linked = dev.myc_myv_linked and prod.myc_myv_linked is not null then 1 else 0 end) as matched_myc_myv_linked
,sum(case when prod.myv_van_id = dev.myv_van_id and prod.myv_van_id is not null then 1 else 0 end) as matched_myv_van_id
,sum(case when prod.st_myv_van_id = dev.st_myv_van_id and prod.st_myv_van_id is not null then 1 else 0 end) as matched_st_myv_van_id
,sum(case when prod.email = dev.email and prod.email is not null then 1 else 0 end) as matched_email
,sum(case when prod.aux_myc_email = dev.aux_myc_email and prod.aux_myc_email is not null then 1 else 0 end) as matched_aux_myc_email
,sum(case when prod.student_hash = dev.student_hash and prod.student_hash is not null then 1 else 0 end) as matched_student_hash
,sum(case when prod.student_rn = dev.student_rn and prod.student_rn is not null then 1 else 0 end) as matched_student_rn
,sum(case when prod.state_code = dev.state_code and prod.state_code is not null then 1 else 0 end) as matched_state_code

,sum(case when prod.lalvoterid is not null then 1 else 0 end) as prodids_lalvoterid
,sum(case when prod.voters_statevoterid is not null then 1 else 0 end) as prodids_voters_statevoterid
,sum(case when prod.sos_join_key_l2 is not null then 1 else 0 end) as prodids_sos_join_key_l2
,sum(case when prod.l2_precinct is not null then 1 else 0 end) as prodids_l2_precinct
,sum(case when prod.person_id is not null then 1 else 0 end) as prodids_person_id
,sum(case when prod.voterbase_id is not null then 1 else 0 end) as prodids_voterbase_id
,sum(case when prod.dnc_precinct_id is not null then 1 else 0 end) as prodids_dnc_precinct_id
,sum(case when prod.dnc_sos_id is not null then 1 else 0 end) as prodids_dnc_sos_id
,sum(case when prod.sos_join_key_dnc is not null then 1 else 0 end) as prodids_sos_join_key_dnc
,sum(case when prod.dnc_county_voter_id is not null then 1 else 0 end) as prodids_dnc_county_voter_id
,sum(case when prod.county_fips is not null then 1 else 0 end) as prodids_county_fips
,sum(case when prod.pdiid is not null then 1 else 0 end) as prodids_pdiid
,sum(case when prod.pdi_sos_id is not null then 1 else 0 end) as prodids_pdi_sos_id
,sum(case when prod.pdi_precinct is not null then 1 else 0 end) as prodids_pdi_precinct
,sum(case when prod.actionkit_id is not null then 1 else 0 end) as prodids_actionkit_id
,sum(case when prod.ak_updated_at is not null then 1 else 0 end) as prodids_ak_updated_at
,sum(case when prod.bern_id is not null then 1 else 0 end) as prodids_bern_id
,sum(case when prod.bern_canvasser_id is not null then 1 else 0 end) as prodids_bern_canvasser_id
,sum(case when prod.myc_van_id is not null then 1 else 0 end) as prodids_myc_van_id
,sum(case when prod.st_myc_van_id is not null then 1 else 0 end) as prodids_st_myc_van_id
,sum(case when prod.myc_dupe is not null then 1 else 0 end) as prodids_myc_dupe
,sum(case when prod.myc_voter_type_id is not null then 1 else 0 end) as prodids_myc_voter_type_id
,sum(case when prod.myc_myv_linked is not null then 1 else 0 end) as prodids_myc_myv_linked
,sum(case when prod.myv_van_id is not null then 1 else 0 end) as prodids_myv_van_id
,sum(case when prod.st_myv_van_id is not null then 1 else 0 end) as prodids_st_myv_van_id
,sum(case when prod.email is not null then 1 else 0 end) as prodids_email
,sum(case when prod.aux_myc_email is not null then 1 else 0 end) as prodids_aux_myc_email
,sum(case when prod.student_hash is not null then 1 else 0 end) as prodids_student_hash
,sum(case when prod.student_rn is not null then 1 else 0 end) as prodids_student_rn
,sum(case when prod.state_code is not null then 1 else 0 end) as prodids_state_code
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
,sum(case when prod.lalvoterid = dev.lalvoterid and prod.lalvoterid is not null then 1 else 0 end) as matched_lalvoterid
,sum(case when prod.voters_statevoterid = dev.voters_statevoterid and prod.voters_statevoterid is not null then 1 else 0 end) as matched_voters_statevoterid
,sum(case when prod.sos_join_key_l2 = dev.sos_join_key_l2 and prod.sos_join_key_l2 is not null then 1 else 0 end) as matched_sos_join_key_l2
,sum(case when prod.l2_precinct = dev.l2_precinct and prod.l2_precinct is not null then 1 else 0 end) as matched_l2_precinct
,sum(case when prod.person_id = dev.person_id and prod.person_id is not null then 1 else 0 end) as matched_person_id
,sum(case when prod.voterbase_id = dev.voterbase_id and prod.voterbase_id is not null then 1 else 0 end) as matched_voterbase_id
,sum(case when prod.dnc_precinct_id = dev.dnc_precinct_id and prod.dnc_precinct_id is not null then 1 else 0 end) as matched_dnc_precinct_id
,sum(case when prod.dnc_sos_id = dev.dnc_sos_id and prod.dnc_sos_id is not null then 1 else 0 end) as matched_dnc_sos_id
,sum(case when prod.sos_join_key_dnc = dev.sos_join_key_dnc and prod.sos_join_key_dnc is not null then 1 else 0 end) as matched_sos_join_key_dnc
,sum(case when prod.dnc_county_voter_id = dev.dnc_county_voter_id and prod.dnc_county_voter_id is not null then 1 else 0 end) as matched_dnc_county_voter_id
,sum(case when prod.county_fips = dev.county_fips and prod.county_fips is not null then 1 else 0 end) as matched_county_fips
,sum(case when prod.pdiid = dev.pdiid and prod.pdiid is not null then 1 else 0 end) as matched_pdiid
,sum(case when prod.pdi_sos_id = dev.pdi_sos_id and prod.pdi_sos_id is not null then 1 else 0 end) as matched_pdi_sos_id
,sum(case when prod.pdi_precinct = dev.pdi_precinct and prod.pdi_precinct is not null then 1 else 0 end) as matched_pdi_precinct
,sum(case when prod.actionkit_id = dev.actionkit_id and prod.actionkit_id is not null then 1 else 0 end) as matched_actionkit_id
,sum(case when prod.ak_updated_at = dev.ak_updated_at and prod.ak_updated_at is not null then 1 else 0 end) as matched_ak_updated_at
,sum(case when prod.bern_id = dev.bern_id and prod.bern_id is not null then 1 else 0 end) as matched_bern_id
,sum(case when prod.bern_canvasser_id = dev.bern_canvasser_id and prod.bern_canvasser_id is not null then 1 else 0 end) as matched_bern_canvasser_id
,sum(case when prod.myc_van_id = dev.myc_van_id and prod.myc_van_id is not null then 1 else 0 end) as matched_myc_van_id
,sum(case when prod.st_myc_van_id = dev.st_myc_van_id and prod.st_myc_van_id is not null then 1 else 0 end) as matched_st_myc_van_id
,sum(case when prod.myc_dupe = dev.myc_dupe and prod.myc_dupe is not null then 1 else 0 end) as matched_myc_dupe
,sum(case when prod.myc_voter_type_id = dev.myc_voter_type_id and prod.myc_voter_type_id is not null then 1 else 0 end) as matched_myc_voter_type_id
,sum(case when prod.myc_myv_linked = dev.myc_myv_linked and prod.myc_myv_linked is not null then 1 else 0 end) as matched_myc_myv_linked
,sum(case when prod.myv_van_id = dev.myv_van_id and prod.myv_van_id is not null then 1 else 0 end) as matched_myv_van_id
,sum(case when prod.st_myv_van_id = dev.st_myv_van_id and prod.st_myv_van_id is not null then 1 else 0 end) as matched_st_myv_van_id
,sum(case when prod.email = dev.email and prod.email is not null then 1 else 0 end) as matched_email
,sum(case when prod.aux_myc_email = dev.aux_myc_email and prod.aux_myc_email is not null then 1 else 0 end) as matched_aux_myc_email
,sum(case when prod.student_hash = dev.student_hash and prod.student_hash is not null then 1 else 0 end) as matched_student_hash
,sum(case when prod.student_rn = dev.student_rn and prod.student_rn is not null then 1 else 0 end) as matched_student_rn
,sum(case when prod.state_code = dev.state_code and prod.state_code is not null then 1 else 0 end) as matched_state_code

,sum(case when prod.lalvoterid is not null then 1 else 0 end) as prodids_lalvoterid
,sum(case when prod.voters_statevoterid is not null then 1 else 0 end) as prodids_voters_statevoterid
,sum(case when prod.sos_join_key_l2 is not null then 1 else 0 end) as prodids_sos_join_key_l2
,sum(case when prod.l2_precinct is not null then 1 else 0 end) as prodids_l2_precinct
,sum(case when prod.person_id is not null then 1 else 0 end) as prodids_person_id
,sum(case when prod.voterbase_id is not null then 1 else 0 end) as prodids_voterbase_id
,sum(case when prod.dnc_precinct_id is not null then 1 else 0 end) as prodids_dnc_precinct_id
,sum(case when prod.dnc_sos_id is not null then 1 else 0 end) as prodids_dnc_sos_id
,sum(case when prod.sos_join_key_dnc is not null then 1 else 0 end) as prodids_sos_join_key_dnc
,sum(case when prod.dnc_county_voter_id is not null then 1 else 0 end) as prodids_dnc_county_voter_id
,sum(case when prod.county_fips is not null then 1 else 0 end) as prodids_county_fips
,sum(case when prod.pdiid is not null then 1 else 0 end) as prodids_pdiid
,sum(case when prod.pdi_sos_id is not null then 1 else 0 end) as prodids_pdi_sos_id
,sum(case when prod.pdi_precinct is not null then 1 else 0 end) as prodids_pdi_precinct
,sum(case when prod.actionkit_id is not null then 1 else 0 end) as prodids_actionkit_id
,sum(case when prod.ak_updated_at is not null then 1 else 0 end) as prodids_ak_updated_at
,sum(case when prod.bern_id is not null then 1 else 0 end) as prodids_bern_id
,sum(case when prod.bern_canvasser_id is not null then 1 else 0 end) as prodids_bern_canvasser_id
,sum(case when prod.myc_van_id is not null then 1 else 0 end) as prodids_myc_van_id
,sum(case when prod.st_myc_van_id is not null then 1 else 0 end) as prodids_st_myc_van_id
,sum(case when prod.myc_dupe is not null then 1 else 0 end) as prodids_myc_dupe
,sum(case when prod.myc_voter_type_id is not null then 1 else 0 end) as prodids_myc_voter_type_id
,sum(case when prod.myc_myv_linked is not null then 1 else 0 end) as prodids_myc_myv_linked
,sum(case when prod.myv_van_id is not null then 1 else 0 end) as prodids_myv_van_id
,sum(case when prod.st_myv_van_id is not null then 1 else 0 end) as prodids_st_myv_van_id
,sum(case when prod.email is not null then 1 else 0 end) as prodids_email
,sum(case when prod.aux_myc_email is not null then 1 else 0 end) as prodids_aux_myc_email
,sum(case when prod.student_hash is not null then 1 else 0 end) as prodids_student_hash
,sum(case when prod.student_rn is not null then 1 else 0 end) as prodids_student_rn
,sum(case when prod.state_code is not null then 1 else 0 end) as prodids_state_code
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
,sum(case when prod.lalvoterid = dev.lalvoterid and prod.lalvoterid is not null then 1 else 0 end) as matched_lalvoterid
,sum(case when prod.voters_statevoterid = dev.voters_statevoterid and prod.voters_statevoterid is not null then 1 else 0 end) as matched_voters_statevoterid
,sum(case when prod.sos_join_key_l2 = dev.sos_join_key_l2 and prod.sos_join_key_l2 is not null then 1 else 0 end) as matched_sos_join_key_l2
,sum(case when prod.l2_precinct = dev.l2_precinct and prod.l2_precinct is not null then 1 else 0 end) as matched_l2_precinct
,sum(case when prod.person_id = dev.person_id and prod.person_id is not null then 1 else 0 end) as matched_person_id
,sum(case when prod.voterbase_id = dev.voterbase_id and prod.voterbase_id is not null then 1 else 0 end) as matched_voterbase_id
,sum(case when prod.dnc_precinct_id = dev.dnc_precinct_id and prod.dnc_precinct_id is not null then 1 else 0 end) as matched_dnc_precinct_id
,sum(case when prod.dnc_sos_id = dev.dnc_sos_id and prod.dnc_sos_id is not null then 1 else 0 end) as matched_dnc_sos_id
,sum(case when prod.sos_join_key_dnc = dev.sos_join_key_dnc and prod.sos_join_key_dnc is not null then 1 else 0 end) as matched_sos_join_key_dnc
,sum(case when prod.dnc_county_voter_id = dev.dnc_county_voter_id and prod.dnc_county_voter_id is not null then 1 else 0 end) as matched_dnc_county_voter_id
,sum(case when prod.county_fips = dev.county_fips and prod.county_fips is not null then 1 else 0 end) as matched_county_fips
,sum(case when prod.pdiid = dev.pdiid and prod.pdiid is not null then 1 else 0 end) as matched_pdiid
,sum(case when prod.pdi_sos_id = dev.pdi_sos_id and prod.pdi_sos_id is not null then 1 else 0 end) as matched_pdi_sos_id
,sum(case when prod.pdi_precinct = dev.pdi_precinct and prod.pdi_precinct is not null then 1 else 0 end) as matched_pdi_precinct
,sum(case when prod.actionkit_id = dev.actionkit_id and prod.actionkit_id is not null then 1 else 0 end) as matched_actionkit_id
,sum(case when prod.ak_updated_at = dev.ak_updated_at and prod.ak_updated_at is not null then 1 else 0 end) as matched_ak_updated_at
,sum(case when prod.bern_id = dev.bern_id and prod.bern_id is not null then 1 else 0 end) as matched_bern_id
,sum(case when prod.bern_canvasser_id = dev.bern_canvasser_id and prod.bern_canvasser_id is not null then 1 else 0 end) as matched_bern_canvasser_id
,sum(case when prod.myc_van_id = dev.myc_van_id and prod.myc_van_id is not null then 1 else 0 end) as matched_myc_van_id
,sum(case when prod.st_myc_van_id = dev.st_myc_van_id and prod.st_myc_van_id is not null then 1 else 0 end) as matched_st_myc_van_id
,sum(case when prod.myc_dupe = dev.myc_dupe and prod.myc_dupe is not null then 1 else 0 end) as matched_myc_dupe
,sum(case when prod.myc_voter_type_id = dev.myc_voter_type_id and prod.myc_voter_type_id is not null then 1 else 0 end) as matched_myc_voter_type_id
,sum(case when prod.myc_myv_linked = dev.myc_myv_linked and prod.myc_myv_linked is not null then 1 else 0 end) as matched_myc_myv_linked
,sum(case when prod.myv_van_id = dev.myv_van_id and prod.myv_van_id is not null then 1 else 0 end) as matched_myv_van_id
,sum(case when prod.st_myv_van_id = dev.st_myv_van_id and prod.st_myv_van_id is not null then 1 else 0 end) as matched_st_myv_van_id
,sum(case when prod.email = dev.email and prod.email is not null then 1 else 0 end) as matched_email
,sum(case when prod.aux_myc_email = dev.aux_myc_email and prod.aux_myc_email is not null then 1 else 0 end) as matched_aux_myc_email
,sum(case when prod.student_hash = dev.student_hash and prod.student_hash is not null then 1 else 0 end) as matched_student_hash
,sum(case when prod.student_rn = dev.student_rn and prod.student_rn is not null then 1 else 0 end) as matched_student_rn
,sum(case when prod.state_code = dev.state_code and prod.state_code is not null then 1 else 0 end) as matched_state_code

,sum(case when prod.lalvoterid is not null then 1 else 0 end) as prodids_lalvoterid
,sum(case when prod.voters_statevoterid is not null then 1 else 0 end) as prodids_voters_statevoterid
,sum(case when prod.sos_join_key_l2 is not null then 1 else 0 end) as prodids_sos_join_key_l2
,sum(case when prod.l2_precinct is not null then 1 else 0 end) as prodids_l2_precinct
,sum(case when prod.person_id is not null then 1 else 0 end) as prodids_person_id
,sum(case when prod.voterbase_id is not null then 1 else 0 end) as prodids_voterbase_id
,sum(case when prod.dnc_precinct_id is not null then 1 else 0 end) as prodids_dnc_precinct_id
,sum(case when prod.dnc_sos_id is not null then 1 else 0 end) as prodids_dnc_sos_id
,sum(case when prod.sos_join_key_dnc is not null then 1 else 0 end) as prodids_sos_join_key_dnc
,sum(case when prod.dnc_county_voter_id is not null then 1 else 0 end) as prodids_dnc_county_voter_id
,sum(case when prod.county_fips is not null then 1 else 0 end) as prodids_county_fips
,sum(case when prod.pdiid is not null then 1 else 0 end) as prodids_pdiid
,sum(case when prod.pdi_sos_id is not null then 1 else 0 end) as prodids_pdi_sos_id
,sum(case when prod.pdi_precinct is not null then 1 else 0 end) as prodids_pdi_precinct
,sum(case when prod.actionkit_id is not null then 1 else 0 end) as prodids_actionkit_id
,sum(case when prod.ak_updated_at is not null then 1 else 0 end) as prodids_ak_updated_at
,sum(case when prod.bern_id is not null then 1 else 0 end) as prodids_bern_id
,sum(case when prod.bern_canvasser_id is not null then 1 else 0 end) as prodids_bern_canvasser_id
,sum(case when prod.myc_van_id is not null then 1 else 0 end) as prodids_myc_van_id
,sum(case when prod.st_myc_van_id is not null then 1 else 0 end) as prodids_st_myc_van_id
,sum(case when prod.myc_dupe is not null then 1 else 0 end) as prodids_myc_dupe
,sum(case when prod.myc_voter_type_id is not null then 1 else 0 end) as prodids_myc_voter_type_id
,sum(case when prod.myc_myv_linked is not null then 1 else 0 end) as prodids_myc_myv_linked
,sum(case when prod.myv_van_id is not null then 1 else 0 end) as prodids_myv_van_id
,sum(case when prod.st_myv_van_id is not null then 1 else 0 end) as prodids_st_myv_van_id
,sum(case when prod.email is not null then 1 else 0 end) as prodids_email
,sum(case when prod.aux_myc_email is not null then 1 else 0 end) as prodids_aux_myc_email
,sum(case when prod.student_hash is not null then 1 else 0 end) as prodids_student_hash
,sum(case when prod.student_rn is not null then 1 else 0 end) as prodids_student_rn
,sum(case when prod.state_code is not null then 1 else 0 end) as prodids_state_code
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
,sum(case when prod.lalvoterid = dev.lalvoterid and prod.lalvoterid is not null then 1 else 0 end) as matched_lalvoterid
,sum(case when prod.voters_statevoterid = dev.voters_statevoterid and prod.voters_statevoterid is not null then 1 else 0 end) as matched_voters_statevoterid
,sum(case when prod.sos_join_key_l2 = dev.sos_join_key_l2 and prod.sos_join_key_l2 is not null then 1 else 0 end) as matched_sos_join_key_l2
,sum(case when prod.l2_precinct = dev.l2_precinct and prod.l2_precinct is not null then 1 else 0 end) as matched_l2_precinct
,sum(case when prod.person_id = dev.person_id and prod.person_id is not null then 1 else 0 end) as matched_person_id
,sum(case when prod.voterbase_id = dev.voterbase_id and prod.voterbase_id is not null then 1 else 0 end) as matched_voterbase_id
,sum(case when prod.dnc_precinct_id = dev.dnc_precinct_id and prod.dnc_precinct_id is not null then 1 else 0 end) as matched_dnc_precinct_id
,sum(case when prod.dnc_sos_id = dev.dnc_sos_id and prod.dnc_sos_id is not null then 1 else 0 end) as matched_dnc_sos_id
,sum(case when prod.sos_join_key_dnc = dev.sos_join_key_dnc and prod.sos_join_key_dnc is not null then 1 else 0 end) as matched_sos_join_key_dnc
,sum(case when prod.dnc_county_voter_id = dev.dnc_county_voter_id and prod.dnc_county_voter_id is not null then 1 else 0 end) as matched_dnc_county_voter_id
,sum(case when prod.county_fips = dev.county_fips and prod.county_fips is not null then 1 else 0 end) as matched_county_fips
,sum(case when prod.pdiid = dev.pdiid and prod.pdiid is not null then 1 else 0 end) as matched_pdiid
,sum(case when prod.pdi_sos_id = dev.pdi_sos_id and prod.pdi_sos_id is not null then 1 else 0 end) as matched_pdi_sos_id
,sum(case when prod.pdi_precinct = dev.pdi_precinct and prod.pdi_precinct is not null then 1 else 0 end) as matched_pdi_precinct
,sum(case when prod.actionkit_id = dev.actionkit_id and prod.actionkit_id is not null then 1 else 0 end) as matched_actionkit_id
,sum(case when prod.ak_updated_at = dev.ak_updated_at and prod.ak_updated_at is not null then 1 else 0 end) as matched_ak_updated_at
,sum(case when prod.bern_id = dev.bern_id and prod.bern_id is not null then 1 else 0 end) as matched_bern_id
,sum(case when prod.bern_canvasser_id = dev.bern_canvasser_id and prod.bern_canvasser_id is not null then 1 else 0 end) as matched_bern_canvasser_id
,sum(case when prod.myc_van_id = dev.myc_van_id and prod.myc_van_id is not null then 1 else 0 end) as matched_myc_van_id
,sum(case when prod.st_myc_van_id = dev.st_myc_van_id and prod.st_myc_van_id is not null then 1 else 0 end) as matched_st_myc_van_id
,sum(case when prod.myc_dupe = dev.myc_dupe and prod.myc_dupe is not null then 1 else 0 end) as matched_myc_dupe
,sum(case when prod.myc_voter_type_id = dev.myc_voter_type_id and prod.myc_voter_type_id is not null then 1 else 0 end) as matched_myc_voter_type_id
,sum(case when prod.myc_myv_linked = dev.myc_myv_linked and prod.myc_myv_linked is not null then 1 else 0 end) as matched_myc_myv_linked
,sum(case when prod.myv_van_id = dev.myv_van_id and prod.myv_van_id is not null then 1 else 0 end) as matched_myv_van_id
,sum(case when prod.st_myv_van_id = dev.st_myv_van_id and prod.st_myv_van_id is not null then 1 else 0 end) as matched_st_myv_van_id
,sum(case when prod.email = dev.email and prod.email is not null then 1 else 0 end) as matched_email
,sum(case when prod.aux_myc_email = dev.aux_myc_email and prod.aux_myc_email is not null then 1 else 0 end) as matched_aux_myc_email
,sum(case when prod.student_hash = dev.student_hash and prod.student_hash is not null then 1 else 0 end) as matched_student_hash
,sum(case when prod.student_rn = dev.student_rn and prod.student_rn is not null then 1 else 0 end) as matched_student_rn
,sum(case when prod.state_code = dev.state_code and prod.state_code is not null then 1 else 0 end) as matched_state_code

,sum(case when prod.lalvoterid is not null then 1 else 0 end) as prodids_lalvoterid
,sum(case when prod.voters_statevoterid is not null then 1 else 0 end) as prodids_voters_statevoterid
,sum(case when prod.sos_join_key_l2 is not null then 1 else 0 end) as prodids_sos_join_key_l2
,sum(case when prod.l2_precinct is not null then 1 else 0 end) as prodids_l2_precinct
,sum(case when prod.person_id is not null then 1 else 0 end) as prodids_person_id
,sum(case when prod.voterbase_id is not null then 1 else 0 end) as prodids_voterbase_id
,sum(case when prod.dnc_precinct_id is not null then 1 else 0 end) as prodids_dnc_precinct_id
,sum(case when prod.dnc_sos_id is not null then 1 else 0 end) as prodids_dnc_sos_id
,sum(case when prod.sos_join_key_dnc is not null then 1 else 0 end) as prodids_sos_join_key_dnc
,sum(case when prod.dnc_county_voter_id is not null then 1 else 0 end) as prodids_dnc_county_voter_id
,sum(case when prod.county_fips is not null then 1 else 0 end) as prodids_county_fips
,sum(case when prod.pdiid is not null then 1 else 0 end) as prodids_pdiid
,sum(case when prod.pdi_sos_id is not null then 1 else 0 end) as prodids_pdi_sos_id
,sum(case when prod.pdi_precinct is not null then 1 else 0 end) as prodids_pdi_precinct
,sum(case when prod.actionkit_id is not null then 1 else 0 end) as prodids_actionkit_id
,sum(case when prod.ak_updated_at is not null then 1 else 0 end) as prodids_ak_updated_at
,sum(case when prod.bern_id is not null then 1 else 0 end) as prodids_bern_id
,sum(case when prod.bern_canvasser_id is not null then 1 else 0 end) as prodids_bern_canvasser_id
,sum(case when prod.myc_van_id is not null then 1 else 0 end) as prodids_myc_van_id
,sum(case when prod.st_myc_van_id is not null then 1 else 0 end) as prodids_st_myc_van_id
,sum(case when prod.myc_dupe is not null then 1 else 0 end) as prodids_myc_dupe
,sum(case when prod.myc_voter_type_id is not null then 1 else 0 end) as prodids_myc_voter_type_id
,sum(case when prod.myc_myv_linked is not null then 1 else 0 end) as prodids_myc_myv_linked
,sum(case when prod.myv_van_id is not null then 1 else 0 end) as prodids_myv_van_id
,sum(case when prod.st_myv_van_id is not null then 1 else 0 end) as prodids_st_myv_van_id
,sum(case when prod.email is not null then 1 else 0 end) as prodids_email
,sum(case when prod.aux_myc_email is not null then 1 else 0 end) as prodids_aux_myc_email
,sum(case when prod.student_hash is not null then 1 else 0 end) as prodids_student_hash
,sum(case when prod.student_rn is not null then 1 else 0 end) as prodids_student_rn
,sum(case when prod.state_code is not null then 1 else 0 end) as prodids_state_code
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
,sum(case when prod.lalvoterid = dev.lalvoterid then 1 else 0 end) as matched_lalvoterid
,sum(case when prod.voters_statevoterid = dev.voters_statevoterid then 1 else 0 end) as matched_voters_statevoterid
,sum(case when prod.sos_join_key_l2 = dev.sos_join_key_l2 then 1 else 0 end) as matched_sos_join_key_l2
,sum(case when prod.l2_precinct = dev.l2_precinct then 1 else 0 end) as matched_l2_precinct
,sum(case when prod.person_id = dev.person_id then 1 else 0 end) as matched_person_id
,sum(case when prod.voterbase_id = dev.voterbase_id then 1 else 0 end) as matched_voterbase_id
,sum(case when prod.dnc_precinct_id = dev.dnc_precinct_id then 1 else 0 end) as matched_dnc_precinct_id
,sum(case when prod.dnc_sos_id = dev.dnc_sos_id then 1 else 0 end) as matched_dnc_sos_id
,sum(case when prod.sos_join_key_dnc = dev.sos_join_key_dnc then 1 else 0 end) as matched_sos_join_key_dnc
,sum(case when prod.dnc_county_voter_id = dev.dnc_county_voter_id then 1 else 0 end) as matched_dnc_county_voter_id
,sum(case when prod.county_fips = dev.county_fips then 1 else 0 end) as matched_county_fips
,sum(case when prod.pdiid = dev.pdiid then 1 else 0 end) as matched_pdiid
,sum(case when prod.pdi_sos_id = dev.pdi_sos_id then 1 else 0 end) as matched_pdi_sos_id
,sum(case when prod.pdi_precinct = dev.pdi_precinct then 1 else 0 end) as matched_pdi_precinct
,sum(case when prod.actionkit_id = dev.actionkit_id then 1 else 0 end) as matched_actionkit_id
,sum(case when prod.ak_updated_at = dev.ak_updated_at then 1 else 0 end) as matched_ak_updated_at
,sum(case when prod.bern_id = dev.bern_id then 1 else 0 end) as matched_bern_id
,sum(case when prod.bern_canvasser_id = dev.bern_canvasser_id then 1 else 0 end) as matched_bern_canvasser_id
,sum(case when prod.myc_van_id = dev.myc_van_id then 1 else 0 end) as matched_myc_van_id
,sum(case when prod.st_myc_van_id = dev.st_myc_van_id then 1 else 0 end) as matched_st_myc_van_id
,sum(case when prod.myc_dupe = dev.myc_dupe then 1 else 0 end) as matched_myc_dupe
,sum(case when prod.myc_voter_type_id = dev.myc_voter_type_id then 1 else 0 end) as matched_myc_voter_type_id
,sum(case when prod.myc_myv_linked = dev.myc_myv_linked then 1 else 0 end) as matched_myc_myv_linked
,sum(case when prod.myv_van_id = dev.myv_van_id then 1 else 0 end) as matched_myv_van_id
,sum(case when prod.st_myv_van_id = dev.st_myv_van_id then 1 else 0 end) as matched_st_myv_van_id
,sum(case when prod.email = dev.email then 1 else 0 end) as matched_email
,sum(case when prod.aux_myc_email = dev.aux_myc_email then 1 else 0 end) as matched_aux_myc_email
,sum(case when prod.student_hash = dev.student_hash then 1 else 0 end) as matched_student_hash
,sum(case when prod.student_rn = dev.student_rn then 1 else 0 end) as matched_student_rn
,sum(case when prod.state_code = dev.state_code then 1 else 0 end) as matched_state_code
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
,sum(case when prod.lalvoterid = dev.lalvoterid and prod.lalvoterid is not null then 1 else 0 end) as matched_lalvoterid
,sum(case when prod.voters_statevoterid = dev.voters_statevoterid and prod.voters_statevoterid is not null then 1 else 0 end) as matched_voters_statevoterid
,sum(case when prod.sos_join_key_l2 = dev.sos_join_key_l2 and prod.sos_join_key_l2 is not null then 1 else 0 end) as matched_sos_join_key_l2
,sum(case when prod.l2_precinct = dev.l2_precinct and prod.l2_precinct is not null then 1 else 0 end) as matched_l2_precinct
,sum(case when prod.person_id = dev.person_id and prod.person_id is not null then 1 else 0 end) as matched_person_id
,sum(case when prod.voterbase_id = dev.voterbase_id and prod.voterbase_id is not null then 1 else 0 end) as matched_voterbase_id
,sum(case when prod.dnc_precinct_id = dev.dnc_precinct_id and prod.dnc_precinct_id is not null then 1 else 0 end) as matched_dnc_precinct_id
,sum(case when prod.dnc_sos_id = dev.dnc_sos_id and prod.dnc_sos_id is not null then 1 else 0 end) as matched_dnc_sos_id
,sum(case when prod.sos_join_key_dnc = dev.sos_join_key_dnc and prod.sos_join_key_dnc is not null then 1 else 0 end) as matched_sos_join_key_dnc
,sum(case when prod.dnc_county_voter_id = dev.dnc_county_voter_id and prod.dnc_county_voter_id is not null then 1 else 0 end) as matched_dnc_county_voter_id
,sum(case when prod.county_fips = dev.county_fips and prod.county_fips is not null then 1 else 0 end) as matched_county_fips
,sum(case when prod.pdiid = dev.pdiid and prod.pdiid is not null then 1 else 0 end) as matched_pdiid
,sum(case when prod.pdi_sos_id = dev.pdi_sos_id and prod.pdi_sos_id is not null then 1 else 0 end) as matched_pdi_sos_id
,sum(case when prod.pdi_precinct = dev.pdi_precinct and prod.pdi_precinct is not null then 1 else 0 end) as matched_pdi_precinct
,sum(case when prod.actionkit_id = dev.actionkit_id and prod.actionkit_id is not null then 1 else 0 end) as matched_actionkit_id
,sum(case when prod.ak_updated_at = dev.ak_updated_at and prod.ak_updated_at is not null then 1 else 0 end) as matched_ak_updated_at
,sum(case when prod.bern_id = dev.bern_id and prod.bern_id is not null then 1 else 0 end) as matched_bern_id
,sum(case when prod.bern_canvasser_id = dev.bern_canvasser_id and prod.bern_canvasser_id is not null then 1 else 0 end) as matched_bern_canvasser_id
,sum(case when prod.myc_van_id = dev.myc_van_id and prod.myc_van_id is not null then 1 else 0 end) as matched_myc_van_id
,sum(case when prod.st_myc_van_id = dev.st_myc_van_id and prod.st_myc_van_id is not null then 1 else 0 end) as matched_st_myc_van_id
,sum(case when prod.myc_dupe = dev.myc_dupe and prod.myc_dupe is not null then 1 else 0 end) as matched_myc_dupe
,sum(case when prod.myc_voter_type_id = dev.myc_voter_type_id and prod.myc_voter_type_id is not null then 1 else 0 end) as matched_myc_voter_type_id
,sum(case when prod.myc_myv_linked = dev.myc_myv_linked and prod.myc_myv_linked is not null then 1 else 0 end) as matched_myc_myv_linked
,sum(case when prod.myv_van_id = dev.myv_van_id and prod.myv_van_id is not null then 1 else 0 end) as matched_myv_van_id
,sum(case when prod.st_myv_van_id = dev.st_myv_van_id and prod.st_myv_van_id is not null then 1 else 0 end) as matched_st_myv_van_id
,sum(case when prod.email = dev.email and prod.email is not null then 1 else 0 end) as matched_email
,sum(case when prod.aux_myc_email = dev.aux_myc_email and prod.aux_myc_email is not null then 1 else 0 end) as matched_aux_myc_email
,sum(case when prod.student_hash = dev.student_hash and prod.student_hash is not null then 1 else 0 end) as matched_student_hash
,sum(case when prod.student_rn = dev.student_rn and prod.student_rn is not null then 1 else 0 end) as matched_student_rn
,sum(case when prod.state_code = dev.state_code and prod.state_code is not null then 1 else 0 end) as matched_state_code

,sum(case when prod.lalvoterid is not null then 1 else 0 end) as prodids_lalvoterid
,sum(case when prod.voters_statevoterid is not null then 1 else 0 end) as prodids_voters_statevoterid
,sum(case when prod.sos_join_key_l2 is not null then 1 else 0 end) as prodids_sos_join_key_l2
,sum(case when prod.l2_precinct is not null then 1 else 0 end) as prodids_l2_precinct
,sum(case when prod.person_id is not null then 1 else 0 end) as prodids_person_id
,sum(case when prod.voterbase_id is not null then 1 else 0 end) as prodids_voterbase_id
,sum(case when prod.dnc_precinct_id is not null then 1 else 0 end) as prodids_dnc_precinct_id
,sum(case when prod.dnc_sos_id is not null then 1 else 0 end) as prodids_dnc_sos_id
,sum(case when prod.sos_join_key_dnc is not null then 1 else 0 end) as prodids_sos_join_key_dnc
,sum(case when prod.dnc_county_voter_id is not null then 1 else 0 end) as prodids_dnc_county_voter_id
,sum(case when prod.county_fips is not null then 1 else 0 end) as prodids_county_fips
,sum(case when prod.pdiid is not null then 1 else 0 end) as prodids_pdiid
,sum(case when prod.pdi_sos_id is not null then 1 else 0 end) as prodids_pdi_sos_id
,sum(case when prod.pdi_precinct is not null then 1 else 0 end) as prodids_pdi_precinct
,sum(case when prod.actionkit_id is not null then 1 else 0 end) as prodids_actionkit_id
,sum(case when prod.ak_updated_at is not null then 1 else 0 end) as prodids_ak_updated_at
,sum(case when prod.bern_id is not null then 1 else 0 end) as prodids_bern_id
,sum(case when prod.bern_canvasser_id is not null then 1 else 0 end) as prodids_bern_canvasser_id
,sum(case when prod.myc_van_id is not null then 1 else 0 end) as prodids_myc_van_id
,sum(case when prod.st_myc_van_id is not null then 1 else 0 end) as prodids_st_myc_van_id
,sum(case when prod.myc_dupe is not null then 1 else 0 end) as prodids_myc_dupe
,sum(case when prod.myc_voter_type_id is not null then 1 else 0 end) as prodids_myc_voter_type_id
,sum(case when prod.myc_myv_linked is not null then 1 else 0 end) as prodids_myc_myv_linked
,sum(case when prod.myv_van_id is not null then 1 else 0 end) as prodids_myv_van_id
,sum(case when prod.st_myv_van_id is not null then 1 else 0 end) as prodids_st_myv_van_id
,sum(case when prod.email is not null then 1 else 0 end) as prodids_email
,sum(case when prod.aux_myc_email is not null then 1 else 0 end) as prodids_aux_myc_email
,sum(case when prod.student_hash is not null then 1 else 0 end) as prodids_student_hash
,sum(case when prod.student_rn is not null then 1 else 0 end) as prodids_student_rn
,sum(case when prod.state_code is not null then 1 else 0 end) as prodids_state_code
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
,sum(case when prod.lalvoterid = dev.lalvoterid and prod.lalvoterid is not null then 1 else 0 end) as matched_lalvoterid
,sum(case when prod.voters_statevoterid = dev.voters_statevoterid and prod.voters_statevoterid is not null then 1 else 0 end) as matched_voters_statevoterid
,sum(case when prod.sos_join_key_l2 = dev.sos_join_key_l2 and prod.sos_join_key_l2 is not null then 1 else 0 end) as matched_sos_join_key_l2
,sum(case when prod.l2_precinct = dev.l2_precinct and prod.l2_precinct is not null then 1 else 0 end) as matched_l2_precinct
,sum(case when prod.person_id = dev.person_id and prod.person_id is not null then 1 else 0 end) as matched_person_id
,sum(case when prod.voterbase_id = dev.voterbase_id and prod.voterbase_id is not null then 1 else 0 end) as matched_voterbase_id
,sum(case when prod.dnc_precinct_id = dev.dnc_precinct_id and prod.dnc_precinct_id is not null then 1 else 0 end) as matched_dnc_precinct_id
,sum(case when prod.dnc_sos_id = dev.dnc_sos_id and prod.dnc_sos_id is not null then 1 else 0 end) as matched_dnc_sos_id
,sum(case when prod.sos_join_key_dnc = dev.sos_join_key_dnc and prod.sos_join_key_dnc is not null then 1 else 0 end) as matched_sos_join_key_dnc
,sum(case when prod.dnc_county_voter_id = dev.dnc_county_voter_id and prod.dnc_county_voter_id is not null then 1 else 0 end) as matched_dnc_county_voter_id
,sum(case when prod.county_fips = dev.county_fips and prod.county_fips is not null then 1 else 0 end) as matched_county_fips
,sum(case when prod.pdiid = dev.pdiid and prod.pdiid is not null then 1 else 0 end) as matched_pdiid
,sum(case when prod.pdi_sos_id = dev.pdi_sos_id and prod.pdi_sos_id is not null then 1 else 0 end) as matched_pdi_sos_id
,sum(case when prod.pdi_precinct = dev.pdi_precinct and prod.pdi_precinct is not null then 1 else 0 end) as matched_pdi_precinct
,sum(case when prod.actionkit_id = dev.actionkit_id and prod.actionkit_id is not null then 1 else 0 end) as matched_actionkit_id
,sum(case when prod.ak_updated_at = dev.ak_updated_at and prod.ak_updated_at is not null then 1 else 0 end) as matched_ak_updated_at
,sum(case when prod.bern_id = dev.bern_id and prod.bern_id is not null then 1 else 0 end) as matched_bern_id
,sum(case when prod.bern_canvasser_id = dev.bern_canvasser_id and prod.bern_canvasser_id is not null then 1 else 0 end) as matched_bern_canvasser_id
,sum(case when prod.myc_van_id = dev.myc_van_id and prod.myc_van_id is not null then 1 else 0 end) as matched_myc_van_id
,sum(case when prod.st_myc_van_id = dev.st_myc_van_id and prod.st_myc_van_id is not null then 1 else 0 end) as matched_st_myc_van_id
,sum(case when prod.myc_dupe = dev.myc_dupe and prod.myc_dupe is not null then 1 else 0 end) as matched_myc_dupe
,sum(case when prod.myc_voter_type_id = dev.myc_voter_type_id and prod.myc_voter_type_id is not null then 1 else 0 end) as matched_myc_voter_type_id
,sum(case when prod.myc_myv_linked = dev.myc_myv_linked and prod.myc_myv_linked is not null then 1 else 0 end) as matched_myc_myv_linked
,sum(case when prod.myv_van_id = dev.myv_van_id and prod.myv_van_id is not null then 1 else 0 end) as matched_myv_van_id
,sum(case when prod.st_myv_van_id = dev.st_myv_van_id and prod.st_myv_van_id is not null then 1 else 0 end) as matched_st_myv_van_id
,sum(case when prod.email = dev.email and prod.email is not null then 1 else 0 end) as matched_email
,sum(case when prod.aux_myc_email = dev.aux_myc_email and prod.aux_myc_email is not null then 1 else 0 end) as matched_aux_myc_email
,sum(case when prod.student_hash = dev.student_hash and prod.student_hash is not null then 1 else 0 end) as matched_student_hash
,sum(case when prod.student_rn = dev.student_rn and prod.student_rn is not null then 1 else 0 end) as matched_student_rn
,sum(case when prod.state_code = dev.state_code and prod.state_code is not null then 1 else 0 end) as matched_state_code

,sum(case when prod.lalvoterid is not null then 1 else 0 end) as prodids_lalvoterid
,sum(case when prod.voters_statevoterid is not null then 1 else 0 end) as prodids_voters_statevoterid
,sum(case when prod.sos_join_key_l2 is not null then 1 else 0 end) as prodids_sos_join_key_l2
,sum(case when prod.l2_precinct is not null then 1 else 0 end) as prodids_l2_precinct
,sum(case when prod.person_id is not null then 1 else 0 end) as prodids_person_id
,sum(case when prod.voterbase_id is not null then 1 else 0 end) as prodids_voterbase_id
,sum(case when prod.dnc_precinct_id is not null then 1 else 0 end) as prodids_dnc_precinct_id
,sum(case when prod.dnc_sos_id is not null then 1 else 0 end) as prodids_dnc_sos_id
,sum(case when prod.sos_join_key_dnc is not null then 1 else 0 end) as prodids_sos_join_key_dnc
,sum(case when prod.dnc_county_voter_id is not null then 1 else 0 end) as prodids_dnc_county_voter_id
,sum(case when prod.county_fips is not null then 1 else 0 end) as prodids_county_fips
,sum(case when prod.pdiid is not null then 1 else 0 end) as prodids_pdiid
,sum(case when prod.pdi_sos_id is not null then 1 else 0 end) as prodids_pdi_sos_id
,sum(case when prod.pdi_precinct is not null then 1 else 0 end) as prodids_pdi_precinct
,sum(case when prod.actionkit_id is not null then 1 else 0 end) as prodids_actionkit_id
,sum(case when prod.ak_updated_at is not null then 1 else 0 end) as prodids_ak_updated_at
,sum(case when prod.bern_id is not null then 1 else 0 end) as prodids_bern_id
,sum(case when prod.bern_canvasser_id is not null then 1 else 0 end) as prodids_bern_canvasser_id
,sum(case when prod.myc_van_id is not null then 1 else 0 end) as prodids_myc_van_id
,sum(case when prod.st_myc_van_id is not null then 1 else 0 end) as prodids_st_myc_van_id
,sum(case when prod.myc_dupe is not null then 1 else 0 end) as prodids_myc_dupe
,sum(case when prod.myc_voter_type_id is not null then 1 else 0 end) as prodids_myc_voter_type_id
,sum(case when prod.myc_myv_linked is not null then 1 else 0 end) as prodids_myc_myv_linked
,sum(case when prod.myv_van_id is not null then 1 else 0 end) as prodids_myv_van_id
,sum(case when prod.st_myv_van_id is not null then 1 else 0 end) as prodids_st_myv_van_id
,sum(case when prod.email is not null then 1 else 0 end) as prodids_email
,sum(case when prod.aux_myc_email is not null then 1 else 0 end) as prodids_aux_myc_email
,sum(case when prod.student_hash is not null then 1 else 0 end) as prodids_student_hash
,sum(case when prod.student_rn is not null then 1 else 0 end) as prodids_student_rn
,sum(case when prod.state_code is not null then 1 else 0 end) as prodids_state_code
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
,sum(case when prod.lalvoterid = dev.lalvoterid and prod.lalvoterid is not null then 1 else 0 end) as matched_lalvoterid
,sum(case when prod.voters_statevoterid = dev.voters_statevoterid and prod.voters_statevoterid is not null then 1 else 0 end) as matched_voters_statevoterid
,sum(case when prod.sos_join_key_l2 = dev.sos_join_key_l2 and prod.sos_join_key_l2 is not null then 1 else 0 end) as matched_sos_join_key_l2
,sum(case when prod.l2_precinct = dev.l2_precinct and prod.l2_precinct is not null then 1 else 0 end) as matched_l2_precinct
,sum(case when prod.person_id = dev.person_id and prod.person_id is not null then 1 else 0 end) as matched_person_id
,sum(case when prod.voterbase_id = dev.voterbase_id and prod.voterbase_id is not null then 1 else 0 end) as matched_voterbase_id
,sum(case when prod.dnc_precinct_id = dev.dnc_precinct_id and prod.dnc_precinct_id is not null then 1 else 0 end) as matched_dnc_precinct_id
,sum(case when prod.dnc_sos_id = dev.dnc_sos_id and prod.dnc_sos_id is not null then 1 else 0 end) as matched_dnc_sos_id
,sum(case when prod.sos_join_key_dnc = dev.sos_join_key_dnc and prod.sos_join_key_dnc is not null then 1 else 0 end) as matched_sos_join_key_dnc
,sum(case when prod.dnc_county_voter_id = dev.dnc_county_voter_id and prod.dnc_county_voter_id is not null then 1 else 0 end) as matched_dnc_county_voter_id
,sum(case when prod.county_fips = dev.county_fips and prod.county_fips is not null then 1 else 0 end) as matched_county_fips
,sum(case when prod.pdiid = dev.pdiid and prod.pdiid is not null then 1 else 0 end) as matched_pdiid
,sum(case when prod.pdi_sos_id = dev.pdi_sos_id and prod.pdi_sos_id is not null then 1 else 0 end) as matched_pdi_sos_id
,sum(case when prod.pdi_precinct = dev.pdi_precinct and prod.pdi_precinct is not null then 1 else 0 end) as matched_pdi_precinct
,sum(case when prod.actionkit_id = dev.actionkit_id and prod.actionkit_id is not null then 1 else 0 end) as matched_actionkit_id
,sum(case when prod.ak_updated_at = dev.ak_updated_at and prod.ak_updated_at is not null then 1 else 0 end) as matched_ak_updated_at
,sum(case when prod.bern_id = dev.bern_id and prod.bern_id is not null then 1 else 0 end) as matched_bern_id
,sum(case when prod.bern_canvasser_id = dev.bern_canvasser_id and prod.bern_canvasser_id is not null then 1 else 0 end) as matched_bern_canvasser_id
,sum(case when prod.myc_van_id = dev.myc_van_id and prod.myc_van_id is not null then 1 else 0 end) as matched_myc_van_id
,sum(case when prod.st_myc_van_id = dev.st_myc_van_id and prod.st_myc_van_id is not null then 1 else 0 end) as matched_st_myc_van_id
,sum(case when prod.myc_dupe = dev.myc_dupe and prod.myc_dupe is not null then 1 else 0 end) as matched_myc_dupe
,sum(case when prod.myc_voter_type_id = dev.myc_voter_type_id and prod.myc_voter_type_id is not null then 1 else 0 end) as matched_myc_voter_type_id
,sum(case when prod.myc_myv_linked = dev.myc_myv_linked and prod.myc_myv_linked is not null then 1 else 0 end) as matched_myc_myv_linked
,sum(case when prod.myv_van_id = dev.myv_van_id and prod.myv_van_id is not null then 1 else 0 end) as matched_myv_van_id
,sum(case when prod.st_myv_van_id = dev.st_myv_van_id and prod.st_myv_van_id is not null then 1 else 0 end) as matched_st_myv_van_id
,sum(case when prod.email = dev.email and prod.email is not null then 1 else 0 end) as matched_email
,sum(case when prod.aux_myc_email = dev.aux_myc_email and prod.aux_myc_email is not null then 1 else 0 end) as matched_aux_myc_email
,sum(case when prod.student_hash = dev.student_hash and prod.student_hash is not null then 1 else 0 end) as matched_student_hash
,sum(case when prod.student_rn = dev.student_rn and prod.student_rn is not null then 1 else 0 end) as matched_student_rn
,sum(case when prod.state_code = dev.state_code and prod.state_code is not null then 1 else 0 end) as matched_state_code

,sum(case when prod.lalvoterid is not null then 1 else 0 end) as prodids_lalvoterid
,sum(case when prod.voters_statevoterid is not null then 1 else 0 end) as prodids_voters_statevoterid
,sum(case when prod.sos_join_key_l2 is not null then 1 else 0 end) as prodids_sos_join_key_l2
,sum(case when prod.l2_precinct is not null then 1 else 0 end) as prodids_l2_precinct
,sum(case when prod.person_id is not null then 1 else 0 end) as prodids_person_id
,sum(case when prod.voterbase_id is not null then 1 else 0 end) as prodids_voterbase_id
,sum(case when prod.dnc_precinct_id is not null then 1 else 0 end) as prodids_dnc_precinct_id
,sum(case when prod.dnc_sos_id is not null then 1 else 0 end) as prodids_dnc_sos_id
,sum(case when prod.sos_join_key_dnc is not null then 1 else 0 end) as prodids_sos_join_key_dnc
,sum(case when prod.dnc_county_voter_id is not null then 1 else 0 end) as prodids_dnc_county_voter_id
,sum(case when prod.county_fips is not null then 1 else 0 end) as prodids_county_fips
,sum(case when prod.pdiid is not null then 1 else 0 end) as prodids_pdiid
,sum(case when prod.pdi_sos_id is not null then 1 else 0 end) as prodids_pdi_sos_id
,sum(case when prod.pdi_precinct is not null then 1 else 0 end) as prodids_pdi_precinct
,sum(case when prod.actionkit_id is not null then 1 else 0 end) as prodids_actionkit_id
,sum(case when prod.ak_updated_at is not null then 1 else 0 end) as prodids_ak_updated_at
,sum(case when prod.bern_id is not null then 1 else 0 end) as prodids_bern_id
,sum(case when prod.bern_canvasser_id is not null then 1 else 0 end) as prodids_bern_canvasser_id
,sum(case when prod.myc_van_id is not null then 1 else 0 end) as prodids_myc_van_id
,sum(case when prod.st_myc_van_id is not null then 1 else 0 end) as prodids_st_myc_van_id
,sum(case when prod.myc_dupe is not null then 1 else 0 end) as prodids_myc_dupe
,sum(case when prod.myc_voter_type_id is not null then 1 else 0 end) as prodids_myc_voter_type_id
,sum(case when prod.myc_myv_linked is not null then 1 else 0 end) as prodids_myc_myv_linked
,sum(case when prod.myv_van_id is not null then 1 else 0 end) as prodids_myv_van_id
,sum(case when prod.st_myv_van_id is not null then 1 else 0 end) as prodids_st_myv_van_id
,sum(case when prod.email is not null then 1 else 0 end) as prodids_email
,sum(case when prod.aux_myc_email is not null then 1 else 0 end) as prodids_aux_myc_email
,sum(case when prod.student_hash is not null then 1 else 0 end) as prodids_student_hash
,sum(case when prod.student_rn is not null then 1 else 0 end) as prodids_student_rn
,sum(case when prod.state_code is not null then 1 else 0 end) as prodids_state_code
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
,sum(case when prod.lalvoterid = dev.lalvoterid and prod.lalvoterid is not null then 1 else 0 end) as matched_lalvoterid
,sum(case when prod.voters_statevoterid = dev.voters_statevoterid and prod.voters_statevoterid is not null then 1 else 0 end) as matched_voters_statevoterid
,sum(case when prod.sos_join_key_l2 = dev.sos_join_key_l2 and prod.sos_join_key_l2 is not null then 1 else 0 end) as matched_sos_join_key_l2
,sum(case when prod.l2_precinct = dev.l2_precinct and prod.l2_precinct is not null then 1 else 0 end) as matched_l2_precinct
,sum(case when prod.person_id = dev.person_id and prod.person_id is not null then 1 else 0 end) as matched_person_id
,sum(case when prod.voterbase_id = dev.voterbase_id and prod.voterbase_id is not null then 1 else 0 end) as matched_voterbase_id
,sum(case when prod.dnc_precinct_id = dev.dnc_precinct_id and prod.dnc_precinct_id is not null then 1 else 0 end) as matched_dnc_precinct_id
,sum(case when prod.dnc_sos_id = dev.dnc_sos_id and prod.dnc_sos_id is not null then 1 else 0 end) as matched_dnc_sos_id
,sum(case when prod.sos_join_key_dnc = dev.sos_join_key_dnc and prod.sos_join_key_dnc is not null then 1 else 0 end) as matched_sos_join_key_dnc
,sum(case when prod.dnc_county_voter_id = dev.dnc_county_voter_id and prod.dnc_county_voter_id is not null then 1 else 0 end) as matched_dnc_county_voter_id
,sum(case when prod.county_fips = dev.county_fips and prod.county_fips is not null then 1 else 0 end) as matched_county_fips
,sum(case when prod.pdiid = dev.pdiid and prod.pdiid is not null then 1 else 0 end) as matched_pdiid
,sum(case when prod.pdi_sos_id = dev.pdi_sos_id and prod.pdi_sos_id is not null then 1 else 0 end) as matched_pdi_sos_id
,sum(case when prod.pdi_precinct = dev.pdi_precinct and prod.pdi_precinct is not null then 1 else 0 end) as matched_pdi_precinct
,sum(case when prod.actionkit_id = dev.actionkit_id and prod.actionkit_id is not null then 1 else 0 end) as matched_actionkit_id
,sum(case when prod.ak_updated_at = dev.ak_updated_at and prod.ak_updated_at is not null then 1 else 0 end) as matched_ak_updated_at
,sum(case when prod.bern_id = dev.bern_id and prod.bern_id is not null then 1 else 0 end) as matched_bern_id
,sum(case when prod.bern_canvasser_id = dev.bern_canvasser_id and prod.bern_canvasser_id is not null then 1 else 0 end) as matched_bern_canvasser_id
,sum(case when prod.myc_van_id = dev.myc_van_id and prod.myc_van_id is not null then 1 else 0 end) as matched_myc_van_id
,sum(case when prod.st_myc_van_id = dev.st_myc_van_id and prod.st_myc_van_id is not null then 1 else 0 end) as matched_st_myc_van_id
,sum(case when prod.myc_dupe = dev.myc_dupe and prod.myc_dupe is not null then 1 else 0 end) as matched_myc_dupe
,sum(case when prod.myc_voter_type_id = dev.myc_voter_type_id and prod.myc_voter_type_id is not null then 1 else 0 end) as matched_myc_voter_type_id
,sum(case when prod.myc_myv_linked = dev.myc_myv_linked and prod.myc_myv_linked is not null then 1 else 0 end) as matched_myc_myv_linked
,sum(case when prod.myv_van_id = dev.myv_van_id and prod.myv_van_id is not null then 1 else 0 end) as matched_myv_van_id
,sum(case when prod.st_myv_van_id = dev.st_myv_van_id and prod.st_myv_van_id is not null then 1 else 0 end) as matched_st_myv_van_id
,sum(case when prod.email = dev.email and prod.email is not null then 1 else 0 end) as matched_email
,sum(case when prod.aux_myc_email = dev.aux_myc_email and prod.aux_myc_email is not null then 1 else 0 end) as matched_aux_myc_email
,sum(case when prod.student_hash = dev.student_hash and prod.student_hash is not null then 1 else 0 end) as matched_student_hash
,sum(case when prod.student_rn = dev.student_rn and prod.student_rn is not null then 1 else 0 end) as matched_student_rn
,sum(case when prod.state_code = dev.state_code and prod.state_code is not null then 1 else 0 end) as matched_state_code

,sum(case when prod.lalvoterid is not null then 1 else 0 end) as prodids_lalvoterid
,sum(case when prod.voters_statevoterid is not null then 1 else 0 end) as prodids_voters_statevoterid
,sum(case when prod.sos_join_key_l2 is not null then 1 else 0 end) as prodids_sos_join_key_l2
,sum(case when prod.l2_precinct is not null then 1 else 0 end) as prodids_l2_precinct
,sum(case when prod.person_id is not null then 1 else 0 end) as prodids_person_id
,sum(case when prod.voterbase_id is not null then 1 else 0 end) as prodids_voterbase_id
,sum(case when prod.dnc_precinct_id is not null then 1 else 0 end) as prodids_dnc_precinct_id
,sum(case when prod.dnc_sos_id is not null then 1 else 0 end) as prodids_dnc_sos_id
,sum(case when prod.sos_join_key_dnc is not null then 1 else 0 end) as prodids_sos_join_key_dnc
,sum(case when prod.dnc_county_voter_id is not null then 1 else 0 end) as prodids_dnc_county_voter_id
,sum(case when prod.county_fips is not null then 1 else 0 end) as prodids_county_fips
,sum(case when prod.pdiid is not null then 1 else 0 end) as prodids_pdiid
,sum(case when prod.pdi_sos_id is not null then 1 else 0 end) as prodids_pdi_sos_id
,sum(case when prod.pdi_precinct is not null then 1 else 0 end) as prodids_pdi_precinct
,sum(case when prod.actionkit_id is not null then 1 else 0 end) as prodids_actionkit_id
,sum(case when prod.ak_updated_at is not null then 1 else 0 end) as prodids_ak_updated_at
,sum(case when prod.bern_id is not null then 1 else 0 end) as prodids_bern_id
,sum(case when prod.bern_canvasser_id is not null then 1 else 0 end) as prodids_bern_canvasser_id
,sum(case when prod.myc_van_id is not null then 1 else 0 end) as prodids_myc_van_id
,sum(case when prod.st_myc_van_id is not null then 1 else 0 end) as prodids_st_myc_van_id
,sum(case when prod.myc_dupe is not null then 1 else 0 end) as prodids_myc_dupe
,sum(case when prod.myc_voter_type_id is not null then 1 else 0 end) as prodids_myc_voter_type_id
,sum(case when prod.myc_myv_linked is not null then 1 else 0 end) as prodids_myc_myv_linked
,sum(case when prod.myv_van_id is not null then 1 else 0 end) as prodids_myv_van_id
,sum(case when prod.st_myv_van_id is not null then 1 else 0 end) as prodids_st_myv_van_id
,sum(case when prod.email is not null then 1 else 0 end) as prodids_email
,sum(case when prod.aux_myc_email is not null then 1 else 0 end) as prodids_aux_myc_email
,sum(case when prod.student_hash is not null then 1 else 0 end) as prodids_student_hash
,sum(case when prod.student_rn is not null then 1 else 0 end) as prodids_student_rn
,sum(case when prod.state_code is not null then 1 else 0 end) as prodids_state_code
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
,sum(case when prod.lalvoterid = dev.lalvoterid and prod.lalvoterid is not null then 1 else 0 end) as matched_lalvoterid
,sum(case when prod.voters_statevoterid = dev.voters_statevoterid and prod.voters_statevoterid is not null then 1 else 0 end) as matched_voters_statevoterid
,sum(case when prod.sos_join_key_l2 = dev.sos_join_key_l2 and prod.sos_join_key_l2 is not null then 1 else 0 end) as matched_sos_join_key_l2
,sum(case when prod.l2_precinct = dev.l2_precinct and prod.l2_precinct is not null then 1 else 0 end) as matched_l2_precinct
,sum(case when prod.person_id = dev.person_id and prod.person_id is not null then 1 else 0 end) as matched_person_id
,sum(case when prod.voterbase_id = dev.voterbase_id and prod.voterbase_id is not null then 1 else 0 end) as matched_voterbase_id
,sum(case when prod.dnc_precinct_id = dev.dnc_precinct_id and prod.dnc_precinct_id is not null then 1 else 0 end) as matched_dnc_precinct_id
,sum(case when prod.dnc_sos_id = dev.dnc_sos_id and prod.dnc_sos_id is not null then 1 else 0 end) as matched_dnc_sos_id
,sum(case when prod.sos_join_key_dnc = dev.sos_join_key_dnc and prod.sos_join_key_dnc is not null then 1 else 0 end) as matched_sos_join_key_dnc
,sum(case when prod.dnc_county_voter_id = dev.dnc_county_voter_id and prod.dnc_county_voter_id is not null then 1 else 0 end) as matched_dnc_county_voter_id
,sum(case when prod.county_fips = dev.county_fips and prod.county_fips is not null then 1 else 0 end) as matched_county_fips
,sum(case when prod.pdiid = dev.pdiid and prod.pdiid is not null then 1 else 0 end) as matched_pdiid
,sum(case when prod.pdi_sos_id = dev.pdi_sos_id and prod.pdi_sos_id is not null then 1 else 0 end) as matched_pdi_sos_id
,sum(case when prod.pdi_precinct = dev.pdi_precinct and prod.pdi_precinct is not null then 1 else 0 end) as matched_pdi_precinct
,sum(case when prod.actionkit_id = dev.actionkit_id and prod.actionkit_id is not null then 1 else 0 end) as matched_actionkit_id
,sum(case when prod.ak_updated_at = dev.ak_updated_at and prod.ak_updated_at is not null then 1 else 0 end) as matched_ak_updated_at
,sum(case when prod.bern_id = dev.bern_id and prod.bern_id is not null then 1 else 0 end) as matched_bern_id
,sum(case when prod.bern_canvasser_id = dev.bern_canvasser_id and prod.bern_canvasser_id is not null then 1 else 0 end) as matched_bern_canvasser_id
,sum(case when prod.myc_van_id = dev.myc_van_id and prod.myc_van_id is not null then 1 else 0 end) as matched_myc_van_id
,sum(case when prod.st_myc_van_id = dev.st_myc_van_id and prod.st_myc_van_id is not null then 1 else 0 end) as matched_st_myc_van_id
,sum(case when prod.myc_dupe = dev.myc_dupe and prod.myc_dupe is not null then 1 else 0 end) as matched_myc_dupe
,sum(case when prod.myc_voter_type_id = dev.myc_voter_type_id and prod.myc_voter_type_id is not null then 1 else 0 end) as matched_myc_voter_type_id
,sum(case when prod.myc_myv_linked = dev.myc_myv_linked and prod.myc_myv_linked is not null then 1 else 0 end) as matched_myc_myv_linked
,sum(case when prod.myv_van_id = dev.myv_van_id and prod.myv_van_id is not null then 1 else 0 end) as matched_myv_van_id
,sum(case when prod.st_myv_van_id = dev.st_myv_van_id and prod.st_myv_van_id is not null then 1 else 0 end) as matched_st_myv_van_id
,sum(case when prod.email = dev.email and prod.email is not null then 1 else 0 end) as matched_email
,sum(case when prod.aux_myc_email = dev.aux_myc_email and prod.aux_myc_email is not null then 1 else 0 end) as matched_aux_myc_email
,sum(case when prod.student_hash = dev.student_hash and prod.student_hash is not null then 1 else 0 end) as matched_student_hash
,sum(case when prod.student_rn = dev.student_rn and prod.student_rn is not null then 1 else 0 end) as matched_student_rn
,sum(case when prod.state_code = dev.state_code and prod.state_code is not null then 1 else 0 end) as matched_state_code

,sum(case when prod.lalvoterid is not null then 1 else 0 end) as prodids_lalvoterid
,sum(case when prod.voters_statevoterid is not null then 1 else 0 end) as prodids_voters_statevoterid
,sum(case when prod.sos_join_key_l2 is not null then 1 else 0 end) as prodids_sos_join_key_l2
,sum(case when prod.l2_precinct is not null then 1 else 0 end) as prodids_l2_precinct
,sum(case when prod.person_id is not null then 1 else 0 end) as prodids_person_id
,sum(case when prod.voterbase_id is not null then 1 else 0 end) as prodids_voterbase_id
,sum(case when prod.dnc_precinct_id is not null then 1 else 0 end) as prodids_dnc_precinct_id
,sum(case when prod.dnc_sos_id is not null then 1 else 0 end) as prodids_dnc_sos_id
,sum(case when prod.sos_join_key_dnc is not null then 1 else 0 end) as prodids_sos_join_key_dnc
,sum(case when prod.dnc_county_voter_id is not null then 1 else 0 end) as prodids_dnc_county_voter_id
,sum(case when prod.county_fips is not null then 1 else 0 end) as prodids_county_fips
,sum(case when prod.pdiid is not null then 1 else 0 end) as prodids_pdiid
,sum(case when prod.pdi_sos_id is not null then 1 else 0 end) as prodids_pdi_sos_id
,sum(case when prod.pdi_precinct is not null then 1 else 0 end) as prodids_pdi_precinct
,sum(case when prod.actionkit_id is not null then 1 else 0 end) as prodids_actionkit_id
,sum(case when prod.ak_updated_at is not null then 1 else 0 end) as prodids_ak_updated_at
,sum(case when prod.bern_id is not null then 1 else 0 end) as prodids_bern_id
,sum(case when prod.bern_canvasser_id is not null then 1 else 0 end) as prodids_bern_canvasser_id
,sum(case when prod.myc_van_id is not null then 1 else 0 end) as prodids_myc_van_id
,sum(case when prod.st_myc_van_id is not null then 1 else 0 end) as prodids_st_myc_van_id
,sum(case when prod.myc_dupe is not null then 1 else 0 end) as prodids_myc_dupe
,sum(case when prod.myc_voter_type_id is not null then 1 else 0 end) as prodids_myc_voter_type_id
,sum(case when prod.myc_myv_linked is not null then 1 else 0 end) as prodids_myc_myv_linked
,sum(case when prod.myv_van_id is not null then 1 else 0 end) as prodids_myv_van_id
,sum(case when prod.st_myv_van_id is not null then 1 else 0 end) as prodids_st_myv_van_id
,sum(case when prod.email is not null then 1 else 0 end) as prodids_email
,sum(case when prod.aux_myc_email is not null then 1 else 0 end) as prodids_aux_myc_email
,sum(case when prod.student_hash is not null then 1 else 0 end) as prodids_student_hash
,sum(case when prod.student_rn is not null then 1 else 0 end) as prodids_student_rn
,sum(case when prod.state_code is not null then 1 else 0 end) as prodids_state_code
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

