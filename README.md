rsqoop
======
*Redshift-Sqoop*

Sqoop-like utility for loading relational tables to Redshift

Creating ETL jobs for staging is a drag.  Essentially this utility helps prevent you from making a bunch of boilerplate ETL jobs.  Rsqoop does a kill and fill load of list of reference tables in Redshift from Postgres (cool if you are Postgres users, feel free to adapt to others).  

The utility will for a given set of tables:
1. read the source schema
2. drop and create a staging table on redshift under your desired schema and prefix convention
3. load the data
4. analyze and vacuum

You could have multiple configs for different loads, and different groups of source tables.

##How to use##
1. edit the given cfg, or create new ones. You can list as many tables as you like in a space delimited list!
2. run the utility `python rsqoop.py` or `python rsqoop.py -c rsqoop_user_data.cfg` (assuming you are using multiple configs).

THATS IT!

Will try to make incremental leveraging YAML based config in the future.. Then it could be used for larger tables too.. 
