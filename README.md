# Organizing Modeling Workflows

## Platform workflow links:
* [Project Folder](https://platform.civisanalytics.com/spa/#/projects/129243)

## Modeling Frame:
* Schema and table name: `bernie_data_commons.rainbow_modeling_frame`
* Feature engineering and Modeling Frame table creation [SQL script](https://github.com/Bernie-2020/bernie-targeting/blob/master/modeling-frame/rainbow-modeling-frame.sql)
* [Data dictionary](https://docs.google.com/spreadsheets/d/1O1a4SdNBuPFMRT97__IeD1624OFDFafCSGQAuclDrFU/edit#gid=176972138) of modeling frame features.
* Raw data [archived on S3](https://github.com/Bernie-2020/bernie-targeting/blob/master/s3-files/modeling-frame-source-data.R) and [dev scripts for imputating missing data](https://github.com/Bernie-2020/bernie-targeting/tree/master/modeling-frame/dev).

## Modeling Pipelines:

#### Voter Engagement Models:
* [DV recoding script](https://github.com/Bernie-2020/bernie-targeting/blob/master/pipeline-etl/voluntee-dv-recode-v2.sql)
* [Modeling notebook](https://github.com/Bernie-2020/bernie-targeting/blob/master/modeling/volunteer-modeling-workflow-20191219.ipynb)
* Scores table `bernie_nmarchio2.actionpop_output_20191220` 
* Validation metrics `bernie_nmarchio2.actionpop_validation_20191220`

#### Spoke Persuasion Models:
* [DV recoding script](https://github.com/Bernie-2020/bernie-targeting/blob/master/pipeline-etl/spoke-dv-recode.sql)
* [Modeling notebook](https://github.com/Bernie-2020/bernie-targeting/blob/master/modeling/spoke-modeling-workflow-20191221.ipynb)
* Scores table `bernie_nmarchio2.spoke_output_20191221` 
* Validation metrics `bernie_nmarchio2.spoke_validation_20191221`

## Analytics


## Archive

* MyVoters: all voters 
* MyCampaign: all volunteers, 1 IDs, donors, emails, etc
* ActionKit: events and emails, donors, rally attendees, volunteers (subset of MyCampaign)
* Mobilize: events and signups into mobilize with email that gets (subset of ActionKit)


* Spoke model: https://github.com/Bernie-2020/bernie-targeting/blob/master/modeling/modeling-workflow-20191216.ipynb
* Donor model: https://github.com/Bernie-2020/fundraising/blob/master/ActionKit/donor_based/Modeling/HPC%20Model.ipynb
* ActionKit models (volunteering, events, donors):
https://github.com/Bernie-2020/fundraising/blob/master/ActionKit/donor_based/Modeling/Volunteer%20Model.ipynb
* Doors models: https://github.com/Bernie-2020/data_asna/blob/master/doors_model/ca_doors_model.ipynb
* Volunteer model https://platform.civisanalytics.com/spa/#/projects/123281


https://github.com/Bernie-2020/data_adrian/blob/master/nat_dialer_pipeline/nat_topline_demos.sql
https://github.com/Bernie-2020/data_adrian/blob/master/nat_dialer_pipeline/adriana_base_breakdown_1232019.sql
https://github.com/Bernie-2020/civis-jobs/blob/188b95f4a21c665e51ea1c902a559d0e8612f38a/civis_jobs/tools/getthru/refresh_lists/phones_state_date.sql
https://docs.google.com/spreadsheets/d/1Z_0kPxAhCsR7sp4I9KbHrjNnbGWvnee5dayx99D2GBU/edit#gid=862138946
https://docs.google.com/spreadsheets/d/1CJ6cLoh2Z0Hia8RHLZ1jZEq5CLZUafNWXokt8jdEZWs/edit#gid=0



