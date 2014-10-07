import psycopg2
from datetime import datetime
import ConfigParser
import argparse
import logging

#-------------------------------------------------------------------------------------
#tiny logging wrapper:

log_name = datetime.now().strftime("rsqoop-%Y%m%d-%H%M%S.log")
logging.basicConfig(filename=log_name,level=logging.DEBUG, format='%(asctime)s %(message)s',filmode='w')

def log(message):
    logging.info(message)
    print(message)

#-------------------------------------------------------------------------------------
#create a crude mapping table for pg and rs datatypes

dt_map = {}
dt_map['character']= 'char'
dt_map['double precision']=  'float'
dt_map['timestamp without time zone']=  'timestamp'
dt_map['character varying']=  'varchar'
dt_map['bigint'] = 'integer'
dt_map['boolean'] = 'boolean'
dt_map['integer'] = 'integer'
dt_map['numeric'] = 'float'  #yeah, we'll get numeric right later
dt_map['date'] = 'date'
dt_map['time without time zone']=  'timestamp'
dt_map['smallint'] = 'integer'
dt_map['ARRAY'] = 'varchar(2000)'
dt_map['text'] = 'varchar(2000)'

#-------------------------------------------------------------------------------------
#drop table:


def drop_table (schema, table_name):

    del_sql = 'drop table ' + schema + '.' + table_name

    try:
        rs_cur.execute(del_sql, table_name)
    except:
        log( "table does not exist - no worries mate!")
        rs_conn.rollback()
        return

    else:
        rs_conn.commit()

#-------------------------------------------------------------------------------------
#create table:


def create_table_from(src_schema, src_table, target_schema, target_table):

    schema_sql = """
        select table_name, column_name, data_type
        from information_schema.columns
        where table_name=lower(%s) and table_schema=lower(%s)
        order by ordinal_position """

    pg_cur.execute(schema_sql,(src_table,src_schema))
    rows = pg_cur.fetchall()

    create_sql='create table ' + target_schema + '.' + target_table + ' (\n'

    for r in rows:
        create_sql += r[1] + ' ' + dt_map[r[2]] + ',\n'
    create_sql += 'etl_date timestamp )'

    log(create_sql)

    rs_cur.execute(create_sql)
    rs_conn.commit()

#-------------------------------------------------------------------------------------
#load table:


def load_table (src_schema, src_table, target_schema, target_table):

    pg_cur.execute("SELECT *, now() as etl_date from " + src_schema + '.' + src_table)
    pg_rs = pg_cur.fetchall()

    log(str(len(pg_rs)) + ' rows fetched')

    if (len(pg_rs) == 0 ): # If there are no new records from source table, exit function
        return None

    target = target_schema + '.' + target_table

    insert_sql = "INSERT INTO %s VALUES " % target

    for row in pg_rs:
        insert_sql += "%s,"

    insert_sql_batch = rs_cur.mogrify(insert_sql, pg_rs)

    rs_cur.execute(insert_sql_batch[:-1])
    rs_conn.commit()


#-------------------------------------------------------------------------------------
#tune table:


def tune_table (schema, table):

    #note vac can not be run inside of transaction!
    old_isolation_level = rs_conn.isolation_level
    rs_conn.set_isolation_level(0)

    tune_me = schema +  '.' + table

    vac_sql = 'vacuum ' + tune_me
    analyze_sql = 'analyze ' + tune_me

    rs_cur.execute(vac_sql)
    rs_cur.execute(analyze_sql)

    #set back to old isolation
    rs_conn.set_isolation_level(old_isolation_level)

#-------------------------------------------------------------------------------------
#main:

if __name__ == "__main__":

    log('rsqoop start: '+ str(datetime.now()))

    #-------------------------------------------------------------------------------------
    #arg parse

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-h", "--help", action='help', help='command line utility for scooping data into Redshift from PG')
    parser.add_argument("-c", "--config",  help='specify a config, otherwise i will use rsqoop.cfg', default='rsqoop.cfg')
    args = parser.parse_args()
    config_name=args.config

    #-------------------------------------------------------------------------------------
    #config and args

    conf = ConfigParser.ConfigParser()
    conf.read(config_name)

    target_prefix = conf.get('rsqoop','target_prefix')
    target_schema = conf.get('rsqoop','target_schema')

    rs_db_name = conf.get('redshift','db_name')
    rs_user = conf.get('redshift','user')
    rs_host = conf.get('redshift','host')
    rs_password = conf.get('redshift','password')

    pg_db_name = conf.get('pg','db_name')
    pg_user = conf.get('pg','user')
    pg_host = conf.get('pg','host')
    pg_password = conf.get('pg','password')

    tables = conf.get('rsqoop','tables')

    #-------------------------------------------------------------------------------------
    #config and args

    rs_conn_str = " dbname='" + rs_db_name + "' user='" + rs_user + "' host='" + rs_host + "' port='5439' password='" + rs_password + "'"
    rs_conn = psycopg2.connect(rs_conn_str)
    rs_cur = rs_conn.cursor()

    pg_conn_str = " dbname='" + pg_db_name + "' user='" + pg_user + "' host='" + pg_host + "' port='5432' password='" + pg_password + "'"
    pg_conn = psycopg2.connect(pg_conn_str)
    pg_cur = pg_conn.cursor()

    #-------------------------------------------------------------------------------------
    #main logic

    table_list=tables.split(' ')

    for src_table in table_list:
        src_meta = src_table.split('.')

        if len(src_meta) == 2:
            src_schema = src_meta[0]
            src_table_name = src_meta[1]
        else:
            src_schema = 'public'
            src_table_name=src_meta[0]

        rs_table = target_prefix + '_' + src_table_name

        log('-----\nstarting ' + rs_table + ': ' +str(datetime.now()))

        drop_table(target_schema, rs_table)
        log('table dropped: ' + str(datetime.now()))

        create_table_from(src_schema, src_table_name, target_schema, rs_table)
        log('target created: ' + str(datetime.now()))

        load_table(src_schema, src_table_name, target_schema, rs_table)
        log('target loaded: ' + str(datetime.now()))

        tune_table(target_schema, rs_table)
        log('tuning complete: ' + str(datetime.now()))

    log('-----\ndone!: ' + str(datetime.now()))
