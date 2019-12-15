-- Tract
DROP TABLE IF EXISTS bernie_nmarchio2.geotable_intermed_tract;
CREATE TABLE bernie_nmarchio2.geotable_intermed_tract
  DISTSTYLE KEY
  DISTKEY (tract_id)
  SORTKEY (tract_id)
  AS (select
geo.tract_id
,left(geo.tract_id,5) as county_fip

,tcov.*
,tout.*
,pdb_tract.*

      from
(select distinct left(block_group_id,11) as tract_id from phoenix_census.geo_identifiers) geo

left join

(select lpad(state,2,'00') || lpad(county,3,'000') || lpad(tract,6,'00000') as tract_id_1 from   bernie_nmarchio2.tract_covariates) tcov
on geo.tract_id = tcov.tract_id_1

      left join
(select lpad(state,2,'00') || lpad(county,3,'000') || lpad(tract,6,'00000') as tract_id_2 from   bernie_nmarchio2.tract_outcomes_simple) tout
on geo.tract_id = tout.tract_id_2

left join bernie_nmarchio2.census_pdb_blocktract pdb_tract
on geo.tract_id = pdb_tract.gidtr);

-- County
DROP TABLE IF EXISTS bernie_nmarchio2.geotable_intermed_county;
CREATE TABLE bernie_nmarchio2.geotable_intermed_county
  DISTSTYLE KEY
  DISTKEY (county_fip_id)
  SORTKEY (county_fip_id)
  AS (select
geo.county_fip_id
,hood.*
,gmob.*
,pri.*
 from
(select distinct left(block_group_id,5) as county_fip_id from phoenix_census.geo_identifiers) geo

left join bernie_nmarchio2.primaryreturns16 pri on geo.county_fip_id = lpad(pri.census_county_fips,5,'00000')

left join bernie_nmarchio2.gender_mobility gmob
on geo.county_fip_id = lpad(gmob.cty,5,'00000')

left join bernie_nmarchio2.neighborhood_effects hood
on geo.county_fip_id = lpad(hood.county_fips_2000,5,'00000'));
