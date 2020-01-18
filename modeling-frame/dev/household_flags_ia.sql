drop table if exists modeling.household_flags_ia;
create table modeling.household_flags_ia
distkey(person_id)
as (
    select p.person_id
    ,coalesce(household_id_1,0) as household_id_1
    ,coalesce(household_id_5,0) as household_id_5
    ,coalesce(household_ctc,0) as household_ctc
    ,coalesce(household_donor,0) as household_donor
    ,coalesce(household_event_rsvp,0) as household_event_rsvp
    ,coalesce(household_event_attendee,0) as household_event_attendee
    from phoenix_analytics.person p
    left join (
        select voting_address_id
        ,max(case when ccj.person_id is not null and ccj.support_int = 1 then 1 else 0 end) as household_id_1
        ,max(case when ccj.person_id is not null and ccj.support_int = 5 then 1 else 0 end) as household_id_5
        ,max(case when ctc.person_id is not null then 1 else 0 end) as household_ctc
        ,max(case when donors.user_id is not null then 1 else 0 end) as household_donor
        ,max(case when events.person_id is not null and events.events_rsvpd >= 1 then 1 else 0 end) as household_event_rsvp
        ,max(case when events.person_id is not null and events.events_attended >= 1 then 1 else 0 end) as household_event_attendee
        from phoenix_analytics.person p
        join bernie_data_commons.master_xwalk_dnc xw on xw.person_id = p.person_id
        left join bernie_data_commons.contactcontacts_joined ccj on ccj.person_id = p.person_id
            and ccj.unique_id_flag = 1
        left join (
            select co.user_id as user_id
            from ak_bernie.core_order co
            left join ak_bernie.core_transaction ct ON co.id = ct.order_id
            where ct.success = 1
            and coalesce(ct.created_at, co.created_at, null) >= date('2019-02-18')
            and ct.type in ('sale', 'credit')
            and ct.status in ('completed', '')
            and ct.amount > 0
            and co.status = 'completed'
            group by 1
        ) donors on donors.user_id = xw.actionkit_id
        left join (
            select xw.person_id
            ,xw.actionkit_id
            ,xw.st_myc_van_id
            ,xw.jsonid_encoded
            ,pasr.state as state_code
            ,count(case when status_name in ('Completed', 'Walk in') and role = 'Host' then pasr.event_id end) as events_hosted
            ,count(case when status_name in ('Completed', 'Walk in') then pasr.event_id end) as events_attended
            ,count(pasr.event_id) as events_rsvpd
            from ptg.ptg_all_shifts_regioned pasr
            join bernie_data_commons.master_xwalk_st_myc xw on xw.myc_van_id = pasr.myc_van_id and xw.state_code = pasr.state
            where status_rank = 1
            and event_date::date < (current_date)::date
            and xw.state_code in ('IA')
            group by 1,2,3,4,5
        ) events on events.person_id = xw.person_id
        left join (
            select ccj.person_id
            from contacts.surveyresponses sr
            join contacts.surveyresponsetext srt on srt.surveyresponseid = sr.surveyresponseid
            join bernie_data_commons.contactcontacts_joined ccj on ccj.contactcontact_id = sr.contactcontact_id
            where surveyquestionid in (32,30,9,29,31,35)
            and srt.surveyresponsetext = 'Commit to caucus/vote'
            and person_id is not null
            group by 1
        ) ctc on ctc.person_id = p.person_id
        group by 1
    ) p_household on p_household.voting_address_id = p.voting_address_id
    where p.reg_voter_flag = true
    and p.state_code in ('IA')
);


/*-- check
select count(person_id)
,count(distinct person_id)
,sum(household_id_1) as s_household_id_1
,count(household_id_1) as c_household_id_1
,sum(household_id_5) as s_household_id_5
,count(household_id_5) as c_household_id_5
,sum(household_donor) as s_household_donor
,count(household_donor) as c_household_donor
,sum(household_event_rsvp) as s_household_event_rsvp
,count(household_event_rsvp) as c_household_event_rsvp
,sum(household_event_attendee) as s_household_event_attendee
,count(household_event_attendee) as c_household_event_attendee
,sum(household_ctc) as s_household_ctc
,count(household_ctc) as c_household_ctc
from modeling.household_flags_ia
*/