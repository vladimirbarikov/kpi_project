/* database pet_project creation*/

create database if not exists md_pm_database character set utf8;

/* begin to use database pet_project */

use md_pm_database database;

/* begin tables creation */

/* table delivery */
create table if not exists delivery (
    delivery_id int unsigned not null auto_increment,
    delivery_vin varchar(17) not null,
    delivery_date datetime,
    constraint pk_delivery primary key (delivery_id)
);

/* table final2 */
create table if not exists final2 (
    final2_id int unsigned not null auto_increment,
    final2_vin varchar(17) not null,
    final2_date datetime,
    constraint pk_final2 primary key (final2_id)
);
/* end tables creation */

/* begin data population */

/* delivery data population */
load data local infile '~/Library/Mobile Documents/com~apple~CloudDocs/git_hub/md_pm_project/database/delivery.csv'
into table delivery
fields terminated by ',' 
lines terminated by '\n' 
ignore 1 rows
(delivery_id, delivery_vin, delivery_date);

/* final2 data population */
load data local infile '~/Library/Mobile Documents/com~apple~CloudDocs/git_hub/md_pm_project/database/final2.csv'
into table final2
fields terminated by ',' 
lines terminated by '\n' 
ignore 1 rows
(final2_id, final2_vin, final2_date);
/* end data population */