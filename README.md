rsqoop
======

Sqoop-like utility for loading relational tables to Redshift

Creating ETL jobs for staging is a drag.  Essentially this utility helps from making a bunch of boilerplate ETL jobs.  Rsqoop does a kill and fill load of list of reference tables in Redshift from Postgres (cool if you are Postgres users, feel free to adapt to others).  

The utility will for a given set of tables read the source schema, drop and create a staging table on redshift under your desired schema and prefix convention, and load the data.  You could have multiple configs for different loads, and different source tables.

##How to use##
1. edit the given cfg, or create new ones. You can list as many tables as you like!
[rsqoop]
tables=public.Campaign public.CampaignHistory public.Locations public.Users
target_schema=staging
target_prefix=m1

[redshift]
db_name=edw
user=admin
host=somescluster.redshift.amazonaws.com
password=password

[pg]
db_name=myapp
user=user
host=pg_prod
password=password

2.  run the utility `python rsqoop.py` or `python rsqoop.py -c rsqoop_user_data.cfg`

THATS IT!

Will try to make incremental leveraging YAML based config in the future.. Then it could be used for larger tables too.. 
