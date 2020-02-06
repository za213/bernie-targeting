
## Voter Contact Universe Table

### What is the purpose of the `bernie_data_commons.base_universe` table?
* The `base_universe` table is designed to serve as a single source of truth for voter contact-related data and to streamline the list cutting process. Users can use the table for a variety of purposes, but for voter targeting users are encouraged to adapt the query examples available [here](https://github.com/Bernie-2020/bernie-targeting/blob/master/universes/query-templates/example-queries.sql) and the pre-launch QC example [here](https://github.com/Bernie-2020/bernie-targeting/blob/master/universes/query-templates/example_queries_prelaunch_qc.sql) to build a table [like this](https://docs.google.com/spreadsheets/d/15DofB9NSfflGj0GoHNd_XT8OdDGU0SXNnvIQs8N0TRc/edit#gid=0).

### How does the default GOTV rank order work?
* The default rank ordering criteria is based on a column called `support_guardrail` which uses numbered support thresholds to prioritize voters who are the best GOTV targets. There are currently four tiers: (1) verified supporters, activists, and donors, (2) voters who have not yet been contacted but have high predicted support, (3) voters with lower support levels and (4) voters who are non-targets (people ineligible to vote in a Dem primary). The categories are labeled as follows: `'0 - Donors, Activists, Supporters', '1 - Inside Support Guardrail','2 - Outside Support Guardrail', '3 - Non-target'`. 
* In order to validate the tiers for a particilar state use the `support_guardrail_validation` column which removes CCJ Field IDs from the top tier in order to enable validation. The categories are labeled as follows: `'0 - Donors and Activists', '1 - Inside Support Guardrail','2 - Outside Support Guardrail', '3 - Non-target'`. [Here is a query](https://github.com/Bernie-2020/bernie-targeting/blob/master/universes/scores-eval.sql) to validate the tiers and a [Google Sheet](https://docs.google.com/spreadsheets/d/1sAgFBeBmHRSxzDC-1DBqvUFokNXQ8fqPphbGUK2awdE/edit#gid=1022689451) that validates GOTV rank orders across all states. To validate your own custom list follow the validation query example available [here](https://github.com/Bernie-2020/bernie-targeting/blob/master/universes/query-templates/example_queries_prelaunch_qc.sql).

### Are all voters in the table?
* The full table contains the full Phoenix voter file using the standard exclusion criteria (`WHERE is_deceased = 'f' AND reg_record_merged = 'f' AND reg_on_current_file = 't' AND reg_voter_flag = 't'`). 
* There are a couple filters available for restricting the size of the GOTV universe
    * `electorate_2way` labels all eligible Democratic primary voters in `dem_primary_eligible_2way`, all verified supporters, donors and activists, and voters with high support scores as part of the `'1 - Target universe'`. This field is the broadest possible GOTV universe criteria and generally the recommended approach since it ensures that activists and donors who may not be Dem Primary Eligible are automatically included.
    * `dem_primary_eligible_2way` labels people as `'1 - Dem Primary Eligible'` who are currently eligible to vote in a Democratic primary based on their party affiliation and whether their registration state is open, closed, or mixed.
    * `vote_ready_5way` segments voters between voters who are `'1 - Vote-ready', '2 - Vote-ready lapsed', '3 - Register as Dem in current state',  '4 - Absentee voter', '5 - Non-target'`. This field can be useful if you are trying to deploy different messaging or voter contact methods based on the voters past vote history or whether they need to take a registration action or may need an absentee ballot.
    
### Which features are included in the table? 
* **Scores** Predictive scores and percentiles including the best performing support models from the `bernie_data_commons.all_scores_ntiles` table and other useful scores including Spoke persuasion and event recruitment models. Note, higher percentiles represent higher scores. For example, the top 10% covers percentiles greater than or equal to 90. The table also includes Civis persuasion scores for expected increase (or decrease) in support from issue-based messaging.
* **Voter Information** Features pulled in from the `bernie_data_commons.rainbow_analytics_frame` and the `bernie_data_commons.national_constituency_table` including party, partisanship, vote history, registered in state, primary eligibility with state of registration, race / ethnicity, spanish language preference, age buckets, ideology, education, income, gender, urbanicity, child in household, marital status, religion and student flag.
* **Support IDs** All Field IDs and Third Party IDs (that are legal for voter contact). These IDs are used both for pre-launch validation to vet the quality of the list and also funneled into the list for GOTV re-targeting purposes.
* **Activists and Donors** Volunteer and events data from MyCampaign, ActionKit, Mobilize, Survey Responses, Bern App, the volunteer Slack, and all donors. People living in households where an occupant reported a Field ID, attended or signed up for a campaign related event, or donated to the campaign. Activists and Donors are automatically added to the top tier of the GOTV rank order.
* **Geocodes** Geographic information including state codes, county FIPS, precints, latitudes and longitudes.

### Will the table undergo updates? 
* **Request features** To request features please file a GitHub issue, tag nmarchio, and DM Nico Marchio about the request.
* **Refresh cadence** The `bernie_data_commons.base_universe` and upstream tables such as the `bernie_data_commons.rainbow_analytics_frame` will be refreshed periodically to ensure that the GOTV universe captures the latest support scores and new voters appearing in Phoenix.



##### Appendix / will add
* Google Sheet Crosstab report
* Process for in field validation and tracking versions of cut lists.
* How to add in unmatched voters. 
* Special targeting criteria for phones, mail, and doors
* Fix mobilize issue
* Average holdout flag rates by state.
* data dictionary




