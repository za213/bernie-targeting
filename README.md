# Organizing Modeling Workflows

### About the `bernie-targeting` Repo

This repo contains a number of resources useful for performing common campaign-related data science tasks. Much of the code in this repo relies on Civis Platform, a research infrastructure built on AWS, often utilized for political campaigns. The SQL code in the repo uses [Redshift SQL](https://aws.amazon.com/redshift/) and many of the modeling functions utilize the [Civis API](https://civis-python.readthedocs.io/en/stable/). In terms of data, the scripts reference data available through the DNC Phoenix database and supplementary geotables linked below. If these requirements are met, it is possible to use this code to spin up a [covariate matrix](https://github.com/Bernie-2020/bernie-targeting/blob/master/modeling-frame/rainbow-modeling-frame.sql) with over 500+ ML-ready predictors and an analysis table to apply demographic and socioeconomic labels to all individuals in the Phoenix database. There are also starter-scripts for building ML models in the [modeling]( https://github.com/Bernie-2020/bernie-targeting/tree/master/modeling) folder and [SQL code](https://github.com/Bernie-2020/bernie-targeting/tree/master/universes) for organizing campaign data into a voter contact universes table. The code is not actively maintained and won't run out of the box due to database dependencies, but we will do our best to respond to questions posed via issues. 

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

## Developers
Organizing Analytics Team. Nico Marchio, Data Science Engineer.
