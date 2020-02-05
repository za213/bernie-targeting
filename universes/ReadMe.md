
## Voter Contact Universe Table

#### What is the purpose of the `bernie_data_commons.base_universe` table?
* The `base_universe` table is designed to serve as a single source of truth for voter contact-related information and to streamline the list cutting process. Query templates are available to users who are looking for a recommended list cutting best practice or users can build their own custom criteria using a custom query template. 
* The high-level summary of how the default rank ordering criteria works is that it forces all verified supporters, activists, and donors to the top of the list and then rank orders the remaining voters who have not yet been contacted using support models which predict support based on voter characteristics and third party data. The field `support_guardrail` sets support thresholds to bucket individuals different support segments and deprioritize people who are less reliable GOTV targets.

#### Which voters does the `bernie_data_commons.base_universe` table cover?
* The full table contains the full Phoenix voter file using the standard exclusion criteria (`WHERE is_deceased = 'f' AND reg_record_merged = 'f' AND reg_on_current_file = 't' AND reg_voter_flag = 't'`). 
* There are a couple filters available for restricting the voter contact criteria based on the voter targeting considerations.
    * `dem_primary_eligible_2way` labels people as '1 - Dem Primary Eligible' who are currently eligible to vote in in a Democratic primary based on their party affiliation and if their registration state is open, closed, or mixed.
    * `electorate_2way` labels all eligible Democratic primary voters in `dem_primary_eligible_2way`, all verified supporters, donors and activists, and voters with high support scores as part of the '1 - Target universe'.
    * The last potentially useful fields is `vote_ready_5way` which attempts to segment voters between voters who are '1 - Vote-ready', '2 - Vote-ready lapsed', '3 - Register as Dem in current state',  '4 - Absentee voter', '5 - Non-target'.
    
#### Which features are included?
* **Scores** Predictive scores and percentiles including the best performing support models from the `bernie_data_commons.all_scores_ntiles` table and other useful scores including Spoke persuasion and event recruitment models. Note, higher percentiles represent higher scores. For example, the top 10% covers percentiles greater than or equal to 90. The table also includes Civis persuasion scores for expected increase (or decrease) in support from issue-based messaging.
* **Voter Information** Features pulled in from the `bernie_data_commons.rainbow_analytics_frame` and the `bernie_data_commons.national_constituency_table` including party, partisanship, vote history, registered in state, primary eligibility with state of registration, race / ethnicity, spanish language preference, age buckets, ideology, education, income, gender, urbanicity, child in household, marital status, religion and student flag.
* **Support IDs** All Field IDs and Third Party IDs (that are legal for voter contact). These IDs are used both for pre-launch validation to vet the quality of the list and also funneled into the list for GOTV re-targeting purposes.
* **Activists and Donors** Volunteer and events data from MyCampaign, ActionKit, Mobilize, Survey Responses, Bern App, the volunteer Slack, and all donors. People living in households where an occupant reported a Field ID, attended or signed up for a campaign related event, or donated to the campaign. Activists and Donors are automatically added to the top tier of the GOTV rank order.
* **Geo Codes** Geographic information including state codes, county FIPS, precints, latitudes and longitudes.

#### How do you cut lists? 
    




