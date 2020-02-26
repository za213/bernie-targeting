with validation as (
    select ccj.person_id
    ,ccj.support_int
    ,case when (
            coalesce(s.field_id_1_score_ntile,1) >= 70
            or coalesce(s.field_id_composite_score_ntile,1) >= 70
            or coalesce(s.current_support_rank_order,1) >= 70
            or coalesce(s.sanders_strong_support_score_ntile,1) >= 70
        )
        and coalesce(s.field_id_5_score_ntile,1) < 90
        then 1 else 0 end as in_support_guardrail_s
    ,ntile(20) over (order by "in_support_guardrail_s" desc,coalesce(s.field_id_1_score_ntile,1) desc) as ntile_s
    ,case when (
            s2.field_id_1_score_100 >= 70
            or coalesce(s.field_id_composite_score_ntile,1) >= 70 -- does not exist in new scores
            or s2.current_support_raw_100 >= 70
            or coalesce(s.sanders_strong_support_score_ntile,1) >= 70 -- did not validate in this set, using national version
        )
        and s2.field_id_5_score_100 < 90
        then 1 else 0 end as in_support_guardrail_s2
    ,ntile(20) over (order by "in_support_guardrail_s2" desc,s2.field_id_1_score_100 desc) as ntile_s2
    from bernie_data_commons.ccj_dnc ccj
    join haystaq.set_no_current snc on snc.person_id = ccj.person_id and snc.set_no = 3
    join bernie_data_commons.all_scores_ntile s on s.person_id = ccj.person_id
    join (
        select s.person_id
        ,row_number() over (order by sanders_hr_aw4_donor_xg) as current_support_raw_100
        ,row_number() over (order by field_id_dv_1_aw2_xg) as field_id_1_score_100
        ,row_number() over (order by field_id_dv_5_aw2_xg) as field_id_5_score_100
        ,row_number() over (order by
            case
                when p.party_id not in(2,4) or ( p.party_id is null and pas.civis_2020_partisanship >=.35)
                then ((0.5*s.field_id_dv_1_aw2_xg)+(0.5*(100-s.field_id_dv_5_aw2_xg)))
                else null
            end) as field_id_composite_score_ntile
        from haystaq.va_scores_20200225_raw s
        join phoenix_analytics.person p on p.person_id = s.person_id
        join phoenix_scores.all_scores_2020 pas on pas.person_id = s.person_id
    ) s2 on s2.person_id = ccj.person_id
    where ccj.unique_id_flag = true
    and ccj.voter_state = 'VA'
    and ccj.support_int in (1,2,3,4,5)
)

select s.ntile
,s.id_1_rate as id_1_rate_current
,s2.id_1_rate as id_1_rate_new
from (
    select ntile_s as ntile
    ,count(person_id) as ids_all
    ,count(case when support_int = 1 then person_id end) as ids_1
    ,"ids_1"::float/nullif("ids_all",0) as id_1_rate
    from validation
    group by 1
) s
join (
    select ntile_s2 as ntile
    ,count(person_id) as ids_all
    ,count(case when support_int = 1 then person_id end) as ids_1
    ,"ids_1"::float/nullif("ids_all",0) as id_1_rate
    from validation
    group by 1
) s2 on s2.ntile = s.ntile
order by 1