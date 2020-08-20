-- #!{"id":"this_is_a_auto_dag_id_2", "interval":"0 0 1 * *"}!#
create or replace table alg-hn-insights.Zz_Test_Airflow.airflow_create as select date_sub(current_date(), interval 1 day) as date