# Organizing Modeling Workflows

### About the `bernie-targeting` Repo

This repo contains a number of resources useful for performing common campaign-related data science tasks. Much of the code in this repo relies on Civis Platform, a research infrastructure built on AWS, often utilized for political campaigns. The SQL code in the repo is [Redshift SQL](https://aws.amazon.com/redshift/) and many of the modeling functions utilize the [Civis API](https://civis-python.readthedocs.io/en/stable/). Many of the scripts reference data available through the DNC Phoenix database, supplementary geotables ([available here]((https://uchicago.box.com/s/4b2vzr2mu7z2nbo3tx9mlorotah71xqt)), and proprietary campaign data. For this reason most of the code will require some modification, but can serve as a useful blueprint for setting up a data science capability on a campaign. 

A high level summary of some of the demostration code available in this repo: 
* [SQL](https://github.com/Bernie-2020/bernie-targeting/blob/master/modeling-frame/rainbow-modeling-frame.sql) to build a covariate matrix with over 500+ ML-ready predictors 
* [SQL](https://github.com/Bernie-2020/bernie-targeting/blob/master/modeling-frame/rainbow-analytics-frame.sql) to build an analysis table that applies demographic and socioeconomic labels to all individuals in the Phoenix database
* [Python notebooks](https://github.com/Bernie-2020/bernie-targeting/tree/master/modeling) to build ML models in Civis Platform and generate predictions  
* [SQL](https://github.com/Bernie-2020/bernie-targeting/tree/master/universes) to organize campaign data into a voter contact universe table.
* A [Python notebook](https://github.com/Bernie-2020/bernie-targeting/blob/master/analytics/travel-time-targeting_v2.ipynb) to assign travel times to individuals that live within a fixed distance to campaign related events.

Note, all of the above code is not actively maintained and will not run out of the box due to database dependencies. Please leave feedback via issues and we will do our best to respond.

## Data
### Modeling Frame:
* This [script](https://github.com/Bernie-2020/bernie-targeting/blob/master/modeling-frame/rainbow-modeling-frame.sql) creates a modeling frame consisting of over 500 covariates at the individual and small area level. The build process assumes that your campaign has access to DNC Phoenix, but changes may be needed subject to schema naming conventions or if there are revisions to the source tables. The SQL script uses AWS Redshift syntax. 
* The table build process also pulls in geotables from public data sources which you can download from this [link](https://uchicago.box.com/s/4b2vzr2mu7z2nbo3tx9mlorotah71xqt) and here is a [data dictionary](https://docs.google.com/spreadsheets/d/1IyvVre4zJMJq4bw0epxOhQe0_DSuyxXzQ0qmAObUDRQ/edit?usp=sharing) for descriptions.
* These [dev scripts were used to clean and imput missing data in the geotables](https://github.com/Bernie-2020/bernie-targeting/tree/master/modeling-frame/dev).

### Analytics Frame:
* This [script](https://github.com/Bernie-2020/bernie-targeting/blob/master/modeling-frame/rainbow-analytics-frame.sql) creates an analytics frame and labels individual-level data into common socioeconomic and demographic classifications. Again, this script requires access to DNC Phoenix, which may be subject to changes in schema naming conventions and revisions to source tables.

## Base Universe and Analytics tools
* Visit the [universes](https://github.com/Bernie-2020/bernie-targeting/tree/master/universes) folder for info about the `bernie_data_commons.base_universe` table for cutting GOTV lists and volunteer / event recruitment.
* To enable location based targeting [here is a workflow](https://github.com/Bernie-2020/bernie-targeting/blob/master/analytics/travel-time-targeting_v2.ipynb) that calculates the travel time between each voter within a given commuting radius of a set of points of interest. 

## Modeling 
* The [modeling folder](https://github.com/Bernie-2020/bernie-targeting/tree/master/modeling) contains a few scripts to train models in Civis Platform using the CivisML API. This [Python notebook](https://github.com/Bernie-2020/bernie-targeting/blob/master/modeling/spoke-modeling-workflow-20191221.ipynb) contains a demonstration workflow that trains several models for a series of dependent variables, creates a table of validation metrics, and generates predictions along with a table containing the model output and percentiled scores partitioned on states.

## Developer
Organizing Analytics Team. Nicholas Marchio, Data Science Engineer.
