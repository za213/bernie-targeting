
DROP TABLE IF EXISTS bernie_zasman.unmatched_sos_matched_person_id;
CREATE TABLE bernie_zasman.unmatched_sos_matched_person_id AS
(SELECT source_id as sos_id,
        person_id,
        score
   FROM
   (SELECT source_id, matched_id, score FROM 
    (SELECT *, ROW_NUMBER() OVER(PARTITION BY source_id ORDER BY score DESC) AS rank FROM bernie_zasman.unmatched_sos_matched) WHERE rank = 1) match
   LEFT JOIN
   (SELECT person_id, voterbase_id FROM 
    (SELECT person_id, voterbase_id, ROW_NUMBER() OVER(PARTITION BY voterbase_id ORDER BY person_id NULLS LAST) AS dupe FROM bernie_data_commons.master_xwalk) WHERE dupe = 1) xwalk 
  ON match.matched_id = xwalk.voterbase_id) new_ids 
  ON base_ids.unique_id = new_ids.source_id);
  
  
