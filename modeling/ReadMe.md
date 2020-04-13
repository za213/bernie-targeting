
## Model Scores

#### Voter Engagement Models:
* [DV recoding script](https://github.com/Bernie-2020/bernie-targeting/blob/master/pipeline-etl/dv-recode/volunteer-dv-recode-v2.sql) and [modeling notebook](https://github.com/Bernie-2020/bernie-targeting/blob/master/modeling/volunteer-modeling-workflow-20191219.ipynb)
* Scores table: `actionpop_output_20191220` 
* Validation metrics: `actionpop_validation_20191220`

| Score column | Description | 
| :--- | :--- | 
| `attendee` | Probability to be an attendee of any kind of event |
| `kickoff_party_attendee` | Probability to attend a kickoff party |
| `canvasser_attendee` | Probability to be a canvass volunteer |
| `phonebank_attendee` | Probability to be a phonebank volunteer |
| `rally_barnstorm_attendee` | Probability to attend a rally or barnstorm event |
| `kickoff_party_rally_barnstorm_attendee` | Probability to attend a kickoff party, rally, or barnstorm event |
| `canvasser_phonebank_attendee` | Probability to be a canvass or phonebank volunteer |
| `donor_1plus_times` | Probability to donate at least once |
| `donor_27plus_usd` | Probability to donate more than $27 |
| `bernie_action` | Probability to take any of the above actions |
* Note columns with `_100` are cut into percentiles nationally such that when `where score_100 >= 80` would give the top 20% highest scores.

#### Spoke Persuasion Models:
* [DV recoding script](https://github.com/Bernie-2020/bernie-targeting/blob/master/pipeline-etl/dv-recode/spoke-dv-recode.sql) and [modeling notebook](https://github.com/Bernie-2020/bernie-targeting/blob/master/modeling/spoke-modeling-workflow-20191221.ipynb)
* Scores table: `spoke_output_20191221` 
* Validation metrics: `spoke_validation_20191221`

| Score column | Description |
| :--- | :--- |
| `spoke_support_1box` | Probability of responding with 1 support |
| `spoke_persuasion_1plus` | Probability of moving 1 or more in favor of Bernie |
| `spoke_persuasion_1minus` | Probability of moving 1 or more against Bernie |
* Note columns with `_100` are cut into percentiles nationally such that when `where score_100 >= 80` would give the top 20% highest scores.
