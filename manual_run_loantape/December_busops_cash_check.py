# Databricks notebook source
# MAGIC %run Users/atan@happymoney.com/get_snowflake_data

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import col
dec_output = (spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/science/alison_blobstorage/month_loantape/loantape_output/Dec_all_scenario_AT_01_26_21.csv")
#              .withColumn('current_month_cash_received', col('InterestPaid')+ col('PrincipalPaid'))
              .withColumn('diff_new', col('RemainingPrincipal') - col('JanuaryBeginningBalance'))
             )

dec_trans = (spark.read.format("csv").option("header","true").option("inferSchema","true").load("mnt/science/alison_blobstorage/month_loantape/December_loantape/December Transactions.csv")
              .filter("LoanID != 'NULL'")
              .withColumn('EffectiveDate_new', F.to_date('EffectiveDate', 'MM/dd/yy'))
              .withColumnRenamed(' Amount ', 'Amount')
            )
display(dec_output)

# COMMAND ----------

accurate_pi_split = (dec_output.filter('PaymentReceived_sum = (InterestDue + PrincipalDue)').select("LoanID","PaymentAmountDue",'PaymentReceived_sum', 'EffectiveDate_new','InterestDue','PrincipalDue','InterestPaid','PrincipalPaid','BeginningBalance','RemainingPrincipal','JanuaryBeginningBalance','NextPaymentDate_pull','diff','diff_new')
                    )

display(accurate_pi_split)

# COMMAND ----------

accurate_pi_split.count()

# COMMAND ----------

display(dec_output.filter('diff == PaymentReceived_sum AND diff != 0 AND RemainingPrincipal < JanuaryBeginningBalance').select("LoanID",'PaymentReceived_sum', 'EffectiveDate_new','BeginningBalance','RemainingPrincipal','JanuaryBeginningBalance','NextPaymentDate_pull','diff','diff_new'))

# COMMAND ----------

list(dec_output.filter('diff == PaymentReceived_sum AND diff != 0 AND RemainingPrincipal < JanuaryBeginningBalance').select("LoanID").toPandas()['LoanID'])

# COMMAND ----------

display(dec_output.filter(' -(PaymentReceived_sum) == diff_new AND diff_new != 0').select("LoanID",'PaymentReceived_sum', 'EffectiveDate_new','BeginningBalance','RemainingPrincipal','JanuaryBeginningBalance','NextPaymentDate_pull','diff','diff_new'))

# COMMAND ----------

# DBTITLE 1,Previous month's P+I, to date
options = create_snowflake_options(url= "https://happymoney.us-east-1.snowflakecomputing.com",
                            database="DATA_SCIENCE_DEV",
                            schema="MODEL_VAULT",
                            warehouse="DATA_SCIENCE_DEV",
                            role="DATA_SCIENCE_DEV",
                            preactions=None,
                            secrets_scope="snowflake_datascience", #snowflake_cron_pii
                            user_key="username",
                            pass_key="password")

query = """ Select asofdate, 
            loanid, 
            payoffuid,
            interestpaidtodate, 
            principalpaidtodate,
            recoveriespaidtodate,
            accruedinterest,
            edition
            from "BUSINESS_INTELLIGENCE"."DATA_STORE"."MVW_LOAN_TAPE_MONTHLY"
    where asofdate = '2020-11-30' 
    order by asofdate"""
#VW_STANDARD_LOAN_TAPE_DAILY_HISTORY_DRAFT
cash_df = spark.read \
  .format('snowflake') \
  .options(**options) \
  .option('query',query) \
  .load()
  #snowflake makes column names uppercase. This changes to lowercase
new_df = cash_df.toDF(*[c.lower() for c in cash_df.columns]).withColumnRenamed('loanid','LoanID')
display(new_df)

# COMMAND ----------

calc_df.filter('diff < 0.02').count()

# COMMAND ----------

from pyspark.sql.functions import col
calc_df = (new_df
       .join(dec_output, how='right', on='LoanID')
       .withColumn('current_month_cash_received', col('interestpaidtodate')+col('InterestPaid') + col('principalpaidtodate')+col('PrincipalPaid'))
       .withColumn('last_month_cash_received', col('interestpaidtodate')+ col('principalpaidtodate')+ col('recoveriespaidtodate'))
       .withColumn('cash_delta', col('current_month_cash_received') - col('last_month_cash_received')-col('PaymentReceived_sum'))
#        .withColumn()
       )
display(calc_df.filter('diff < 0.02').select('LoanID','BeginningBalance','PaymentAmountDue','PaymentReceived_sum','InterestPaid','PrincipalPaid', 'RemainingPrincipal', 'JanuaryBeginningBalance', 'diff_new','current_month_cash_received','last_month_cash_received','cash_delta'))

# COMMAND ----------

display(calc_df.filter('cash_delta = 0 AND InterestPaid != 0').select('BeginningBalance','PaymentReceived_sum', 'RemainingPrincipal', 'JanuaryBeginningBalance', 'diff_new','current_month_cash_received','last_month_cash_received','cash_delta'))

# COMMAND ----------

display(calc_df.filter('PaymentReceived_sum = (InterestDue + PrincipalDue)').select('LoanID','BeginningBalance','PaymentAmountDue','PaymentReceived_sum','InterestPaid','PrincipalPaid', 'RemainingPrincipal', 'JanuaryBeginningBalance', 'diff_new','current_month_cash_received','last_month_cash_received','cash_delta'))

# COMMAND ----------

display(calc_df.filter("LoanID = 'P007C1C94B77C'"))

# COMMAND ----------

options = create_snowflake_options(url= "https://happymoney.us-east-1.snowflakecomputing.com",
                            database="BUSINESS_INTELLIGENCE",
                            schema="CRON_STORE",
                            warehouse="BUSINESS_INTELLIGENCE",
                            role="BUSINESS_INTELLIGENCE",
                            preactions=None,
                            secrets_scope="snowflake_cron", #snowflake_cron_pii
                            user_key="username",
                            pass_key="password")
query = """ Select asofdate, 
            loanid, 
            payoffuid,
            interestpaidtodate, 
            principalpaidtodate,
            recoveriespaidtodate,
            edition
            from "BUSINESS_INTELLIGENCE"."DATA_STORE"."MVW_LOAN_TAPE_MONTHLY"
    where asofdate = '2020-11-30' 
    order by asofdate"""
#VW_STANDARD_LOAN_TAPE_DAILY_HISTORY_DRAFT
cash_df = spark.read \
  .format('snowflake') \
  .options(**options) \
  .option('query',query) \
  .load()

# COMMAND ----------


