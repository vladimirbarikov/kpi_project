import math
import pandas as pd
from getpass import getpass
import mysql.connector
import warnings
warnings.filterwarnings('ignore')

# preparing data for database
df1 = pd.read_excel('~/Library/Mobile Documents/com~apple~CloudDocs/git_hub/kpi_project/excel_data/delivery.xlsx', header=0)
df2 = pd.read_excel('~/Library/Mobile Documents/com~apple~CloudDocs/git_hub/kpi_project/excel_data/final2.xlsx', header=0)

delivery_df = df1[['VIN', 'Дата сканирования ТС в очереди']]
delivery_df = delivery_df.rename(columns={'VIN':'delivery_vin', 'Дата сканирования ТС в очереди':'delivery_date'})

final2_df = df2[['VIN', 'Дата сканирования ТС в очереди']]
final2_df = final2_df.rename(columns={'VIN':'final2_vin', 'Дата сканирования ТС в очереди':'final2_date'})

delivery_df.to_csv('~/Library/Mobile Documents/com~apple~CloudDocs/git_hub/kpi_project/csv_data/delivery.csv', encoding='utf-8', index=True, sep=',')
final2_df.to_csv('~/Library/Mobile Documents/com~apple~CloudDocs/git_hub/kpi_project/csv_data/final2.csv', encoding='utf-8', index=True, sep=',')

# connection establishment
cnx = mysql.connector.connect(user=input('User:'),
                              password=getpass('Password:'),
                              host='localhost',
                              allow_local_infile=True)

cursor = cnx.cursor(buffered=True)

# creation kpi database
create_kpi_db = '''
create database if not exists kpi_db character set utf8;
'''

# use kpi database
use_database  = '''
use kpi_db
'''

# creation delivery table
create_table_delivery = '''
create table if not exists delivery (
delivery_id int unsigned not null auto_increment,
delivery_vin varchar(17) not null,
delivery_date datetime,
constraint pk_delivery primary key (delivery_id)
)
# '''

# creation final2 table
create_table_final2 = '''
create table if not exists final2 (
final2_id int unsigned not null auto_increment,
final2_vin varchar(17) not null,
final2_date datetime,
constraint pk_final2 primary key (final2_id)
)
'''

# uploading delivery data
delivery_data = '''
load data local infile '~/Library/Mobile Documents/com~apple~CloudDocs/git_hub/kpi_project/csv_data/delivery.csv'
into table delivery
fields terminated by ',' 
lines terminated by '\n' 
ignore 1 rows
(delivery_id, delivery_vin, delivery_date)
'''

# uploading final2 data
final2_data = '''
load data local infile '~/Library/Mobile Documents/com~apple~CloudDocs/git_hub/kpi_project/csv_data/final2.csv'
into table final2
fields terminated by ',' 
lines terminated by '\n' 
ignore 1 rows
(final2_id, final2_vin, final2_date)
'''

# delivery plan count
count_delivery_plan = '''
select count(delivery_vin)
from delivery
where delivery_date > '2025-04-24 08:30:00' and delivery_date < '2025-04-25 08:30:00'
'''

# delivery fact count
count_delivery_fact = '''
select count(delivery_vin)
from (
select d.delivery_vin, f.final2_date
from delivery d inner join final2 f
on d.delivery_vin = f.final2_vin
where d.delivery_date > '2025-04-24 08:30:00' and d.delivery_date < '2025-04-25 08:30:00'
) as t
where t.final2_date > '2025-04-23 08:30:00'
'''

# execution sql scripts
cursor.execute(create_kpi_db)
cursor.execute(use_database)
cursor.execute(create_table_delivery)
cursor.execute(create_table_final2)
cursor.execute(delivery_data)
cursor.execute(final2_data)
cursor.execute(count_delivery_plan)
delivery_plan = cursor.fetchone()[0]
cursor.execute(count_delivery_fact)
delivery_fact = cursor.fetchone()[0]

print(f'\
      \nDelivery plan: {delivery_plan} cars.\
      \nDelivery fact: {delivery_fact} cars.\
      \nKPI "Delivery accuracy": {math.ceil(delivery_fact / delivery_plan * 100)}%')

# execution commit
cnx.commit()

# cursor and connection close
if cnx.is_connected():
      cursor.close()
      cnx.close()