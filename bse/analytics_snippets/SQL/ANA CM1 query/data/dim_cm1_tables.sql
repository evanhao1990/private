## Build dim tables for CM1 calculation

## Dim Sub costsdim
drop table if exists sandbox.dim_cm1_SubCost;
create table sandbox.dim_cm1_SubCost (
dim_sub_cost_type_id int NOT NULL AUTO_INCREMENT,
sub_cost_type varchar(200),
cost_type varchar(200),
segmentation varchar(200),
cost_state varchar(200),
driver_type_id varchar(200),
PRIMARY KEY(dim_sub_cost_type_id));

insert into sandbox.dim_cm1_SubCost (sub_cost_type, cost_type, segmentation, cost_state, driver_type_id) 
# Warehousing
select 'Intake', 'Warehousing Cost', 'Item', 'Dispatch', '-' union all
select 'Pick&Pack', 'Warehousing Cost', 'Item', 'Dispatch', '1,2' union all
select 'Packaging', 'Warehousing Cost', 'Order', 'Dispatch', '2' union all
select 'Return_Processing', 'Warehousing Cost', 'Item', 'Return', '-' union all
select 'CleaningCost', 'Warehousing Cost', 'Item', 'Return', '3' union all
select 'DamagedGoods', 'Warehousing Cost', 'Item', 'Return', '-' union all
select 'CustomGoods', 'Warehousing Cost', 'Item', 'Dispatch', '4' union all
# Distribution
select 'Shipping Cost', 'Distribution Cost', 'Order', 'Dispatch', '5' union all
select 'Returning Cost', 'Distribution Cost', 'Order', 'Return', '5' union all
# Payment
select 'PaymentFixedFee', 'Payment Cost', 'Order', 'Dispatch', '6' union all
select 'PaymentVarFee', 'Payment Cost', 'Order', 'Dispatch', '6' union all
select 'RefundFixedFee', 'Payment Cost', 'Order', 'Return', '6' union all
select 'RefundVarFee', 'Payment Cost', 'Order', 'Return', '6' 
;

select * from sandbox.dim_cm1_SubCost;
#######################################################################
## Dim CM1 cost drivers
drop table if exists sandbox.dim_cm1_DriverType;
create table sandbox.dim_cm1_DriverType (
dim_driver_type_id int NOT NULL AUTO_INCREMENT,
driver_type varchar(200),
notes varchar(200),
PRIMARY KEY(dim_driver_type_id));

insert into sandbox.dim_cm1_DriverType (driver_type, notes) 
select 'Brand','Brand in Direct Channels - dim_brand' union all
select 'Marketplace','dim_marketplace' union all
select 'Product Category','dim_style' union all
select 'Country','dim_country' union all
select 'Distribution Profile', 'Combination of country, carrier and shipping method - sandbox.dim_DistributionProfile' union all
select 'Payment Profile', 'Payment instruments - sandbox.dim_PaymentProfile'
;

select * from sandbox.dim_cm1_DriverType;
########################################################################
## Dim Distribution Profile
# Data inserted by importing csv - dist_profiles.csv (joined past data from sandbox.CM1_shippingCost with new updated costs
select * from sandbox.dim_DistributionProfile;
########################################################################
## Dim Payment Profile

# data inserted by importing csv - paym_profiles_cost.csv (Updated version of payment costs)
select * from sandbox.dim_PaymentProfile;
########################################################################
## Dim Warehouse Costs
drop table if exists sandbox.dim_WH_costs;
create table sandbox.dim_WH_costs (
wh_cost_id int NOT NULL AUTO_INCREMENT,
dim_sub_cost_type_id int,									## dim_cm1_SubCost
dim_driver_type_id int,										## dim_cm1_CostDrivers
driver_id varchar(200),										## ID of driver: DimBrancID, DimMarketplaceID,...
cost_value float not null default 0,
sub_cost_type varchar(200),
driver_type varchar(200),
PRIMARY KEY(wh_cost_id));


insert into sandbox.dim_WH_costs (dim_sub_cost_type_id, dim_driver_type_id, driver_id, cost_value, sub_cost_type, driver_type) 

## Intake
select 1, NULL, NULL,0.3, 'Intake','-' union all

## Pick&Pack
# Brand
select 2, 1, 1, 0.58, 'Pick&Pack','Brand' union all		# JJ
select 2, 1, 2, 0.58, 'Pick&Pack','Brand' union all		# MM
select 2, 1, 3, 0.58, 'Pick&Pack','Brand' union all		# NI
select 2, 1, 4, 0.58, 'Pick&Pack','Brand' union all		# ON
select 2, 1, 5, 0.58, 'Pick&Pack','Brand' union all		# PC
select 2, 1, 6, 0.58, 'Pick&Pack','Brand' union all		# VL
select 2, 1, 7, 0.58, 'Pick&Pack','Brand' union all		# VM
select 2, 1, 8, 0.58, 'Pick&Pack','Brand' union all		# SL
select 2, 1, 9, 0.58, 'Pick&Pack','Brand' union all		# OC
select 2, 1, 10, 0.58, 'Pick&Pack','Brand' union all	# OF
select 2, 1, 11, 0.58, 'Pick&Pack','Brand' union all	# BS
select 2, 1, 12, 0.58, 'Pick&Pack','Brand' union all	# CO
select 2, 1, 13, 0.58, 'Pick&Pack','Brand' union all	# JR
select 2, 1, 14, 1.12, 'Pick&Pack','Brand' union all	# JL
select 2, 1, 15, 0.58, 'Pick&Pack','Brand' union all	# YA
select 2, 1, 16, 0.58, 'Pick&Pack','Brand' union all	# OS
select 2, 1, 17, 0.58, 'Pick&Pack','Brand' union all	# AD
select 2, 1, 18, 0.58, 'Pick&Pack','Brand' union all	# PT
select 2, 1, 19, 0.58, 'Pick&Pack','Brand' union all	# BI
select 2, 1, 20, 0.58, 'Pick&Pack','Brand' union all	# BH
select 2, 1, 21, 0.58, 'Pick&Pack','Brand' union all	# NM
# MKPLs
select 2, 2, 1, 0.35, 'Pick&Pack','Marketplace' union all		# OTTO
select 2, 2, 2, 0.58, 'Pick&Pack','Marketplace' union all		# ZA
select 2, 2, 3, 0.58, 'Pick&Pack','Marketplace' union all		# EB
select 2, 2, 4, 0.58, 'Pick&Pack','Marketplace' union all		# AM
select 2, 2, 5, 0.58, 'Pick&Pack','Marketplace' union all		# LA
select 2, 2, 6, 0.58, 'Pick&Pack','Marketplace' union all		# PL
select 2, 2, 7, 0.58, 'Pick&Pack','Marketplace' union all		# PR
select 2, 2, 8, 0.58, 'Pick&Pack','Marketplace' union all		# RA
select 2, 2, 9, 0.35, 'Pick&Pack','Marketplace' union all		# OT
select 2, 2, 10, 0.58, 'Pick&Pack','Marketplace' union all		# CU
select 2, 2, 11, 0.35, 'Pick&Pack','Marketplace' union all		# AN
select 2, 2, 12, 0.35, 'Pick&Pack','Marketplace' union all		# AY
select 2, 2, 13, 0.35, 'Pick&Pack','Marketplace' union all		# AB
select 2, 2, 14, 0.58, 'Pick&Pack','Marketplace' union all		# BO
select 2, 2, 15, 0.58, 'Pick&Pack','Marketplace'	union all	# GL

## Packaging
# MKPLs
select 3, 2, 0, 0.5, 'Packaging','Marketplace' union all		# DMW
select 3, 2, 1, 0, 'Packaging','Marketplace' union all								# OTTO
select 3, 2, 2, 0.5, 'Packaging','Marketplace' union all							# ZA
select 3, 2, 3, 0.5, 'Packaging','Marketplace' union all							# EB
select 3, 2, 4, 0.5, 'Packaging','Marketplace' union all							# AM
select 3, 2, 5, 0.5, 'Packaging','Marketplace' union all							# LA
select 3, 2, 6, 0.5, 'Packaging','Marketplace' union all							# PL
select 3, 2, 7, 0.5, 'Packaging','Marketplace' union all							# PR
select 3, 2, 8, 0.5, 'Packaging','Marketplace' union all							# RA
select 3, 2, 9, 0, 'Packaging','Marketplace' union all								# OT
select 3, 2, 10, 0.5, 'Packaging','Marketplace' union all							# CU
select 3, 2, 11, 0, 'Packaging','Marketplace' union all							# AN
select 3, 2, 12, 0, 'Packaging','Marketplace' union all							# AY
select 3, 2, 13, 0, 'Packaging','Marketplace' union all							# AB
select 3, 2, 14, 0.5, 'Packaging','Marketplace' union all							# BO
select 3, 2, 15, 0.5, 'Packaging','Marketplace' union all							# GL

## Return Processing
select 4, NULL, NULL, 0.89, 'Return_Processing','-' union all	

## Cleaning
select 5, 3, cc.product_category, cc.average_cleaning_cost_per_returned_item, 'CleaningCost', 'Product Category' from standard_costing.hermes_cleaning_costs cc union all

## Damaged Goods
# Category average or total average is not relevant --> not included

# Custom Goods
select 7, 4, 43, 2, 'CustomGoods', 'Country' union all								# CH
select 7, 4, 166, 3, 'CustomGoods', 'Country' 											# NO
;

select * from sandbox.dim_WH_costs;