# Databricks notebook source
# MAGIC %run Users/atan@happymoney.com/get_snowflake_data

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import col



jan_transactions = (spark.read.format('csv').option("header","true").option('inferSchema','true').load("/mnt/science/alison_blobstorage/month_loantape/January_2021_transactions.csv")
                  .withColumn('EffectiveDate_new', F.to_date('EffectiveDate', 'MM/dd/yy'))
                  .withColumnRenamed(' Amount ', 'Amount')
                  .withColumnRenamed('LoanID','loanid')
                      )
display(jan_transactions)

# COMMAND ----------

filter_unique_jan = jan_transactions.dropDuplicates(['loanid'])
display(filter_unique_jan)

# COMMAND ----------

#REFRESH TABLE science.interest_due_output_single_pay_jan_2021

jan_output = spark.sql("""

SELECT * FROM science.interest_due_output_single_pay_jan_2021
""")
display(jan_output)

# COMMAND ----------

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
    where asofdate = '2020-12-31' 
    order by asofdate"""
#VW_STANDARD_LOAN_TAPE_DAILY_HISTORY_DRAFT
cash_df = spark.read \
  .format('snowflake') \
  .options(**options) \
  .option('query',query) \
  .load()
  #snowflake makes column names uppercase. This changes to lowercase
new_df = cash_df.toDF(*[c.lower() for c in cash_df.columns])
display(new_df)

# COMMAND ----------

#January RemainingPrincipal as February beginning balance

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
            remainingprincipal as februarybeginningbalance_lt,  
            edition
            from "BUSINESS_INTELLIGENCE"."DATA_STORE"."MVW_LOAN_TAPE_MONTHLY"
    where asofdate = '2021-1-31' 
    order by asofdate"""
#VW_STANDARD_LOAN_TAPE_DAILY_HISTORY_DRAFT
feb_df = spark.read \
  .format('snowflake') \
  .options(**options) \
  .option('query',query) \
  .load()
  #snowflake makes column names uppercase. This changes to lowercase
feb_bb = feb_df.toDF(*[c.lower() for c in feb_df.columns])
display(feb_bb)

# COMMAND ----------

from pyspark.sql.functions import col
import pyspark.sql.functions as F
calc_df = (new_df[['loanid','interestpaidtodate','principalpaidtodate','recoveriespaidtodate']]
       .join(jan_output, how='right', on='loanid')
       .join(feb_bb[['loanid','februarybeginningbalance_lt']], how = 'left', on = 'loanid')
       .join(filter_unique_jan[['loanid','PortfolioId']], how = 'left', on = 'loanid')
       .withColumn('diff', F.abs(col('RemainingPrincipal') -col('februarybeginningbalance_lt')))
       .withColumn('current_month_cash_received', col('interestpaidtodate')+col('InterestPaid') + col('principalpaidtodate')+col('PrincipalPaid'))
       .withColumn('last_month_cash_received', col('interestpaidtodate')+ col('principalpaidtodate')+ col('recoveriespaidtodate'))
       .withColumn('cash_delta', col('current_month_cash_received') - col('last_month_cash_received')-col('PaymentReceived_sum'))
       .withColumn('cash_tie_flag', F.when( (col('cash_delta') < 1) & (col('cash_delta') > -1), 1).otherwise(0))       
          )

display(calc_df.select('loanid','jan_beginningbalance','PaymentAmountDue','PaymentReceived_sum','InterestPaid','PrincipalPaid', 'RemainingPrincipal', 'februarybeginningbalance_lt', 'diff','current_month_cash_received','last_month_cash_received','cash_delta','loanmod'))

# COMMAND ----------

calc_df.filter("cash_tie_flag = 1").count()

# COMMAND ----------

col_names = ['loanid',
   'PortfolioId',
 'interestpaidtodate',
 'principalpaidtodate',
 'recoveriespaidtodate',
 'paystatus',
 'term',
 'interestrate',
 'paytype',
 'PaymentAmountDue',
 'PaymentReceived_sum',
 'nextpaymentduedate_pull',
 'EffectiveDate_new',
 'loanmod',
 'status',
 'jan_beginningbalance',
 'InterestDue',
 'PrincipalDue',
 'InterestPaid',
 'PrincipalPaid',
 'UnpaidInterest',
 'UnpaidPrincipal',
 'NextInterestDue',
 'NextPaymentDate',
 'RemainingPrincipal',             
 'februarybeginningbalance_lt',
 'diff',
 'current_month_cash_received',
 'last_month_cash_received',
 'cash_delta']

# COMMAND ----------

display(calc_df.select(col_names))

# COMMAND ----------

display(calc_df.select('loanid','jan_beginningbalance','PaymentAmountDue','PaymentReceived_sum','InterestPaid','PrincipalPaid', 'RemainingPrincipal', 'februarybeginningbalance_lt', 'diff','current_month_cash_received','last_month_cash_received','cash_delta','loanmod'))

# COMMAND ----------

print(calc_df.count())

# COMMAND ----------

print(calc_df.filter('diff < 0.02').count(), calc_df.filter('diff < 0.02').count()/ calc_df.count())

# COMMAND ----------

calc_df.count()

# COMMAND ----------

print(calc_df.filter('-1 < cash_delta AND cash_delta < 1').count(), calc_df.filter('-1 < cash_delta AND cash_delta < 10').count())

# COMMAND ----------

display(calc_df.filter(col("cash_delta").between(-1,1)).select('loanid','jan_beginningbalance','PaymentAmountDue','PaymentReceived_sum','InterestPaid','PrincipalPaid', 'RemainingPrincipal', 'februarybeginningbalance_lt', 'diff','current_month_cash_received','last_month_cash_received','cash_delta')
       )

# COMMAND ----------

print(calc_df.filter('cash_delta BETWEEN -1 AND 1').count(), calc_df.filter('cash_delta > 10').count(), calc_df.filter('cash_delta  < 10').count(), calc_df.count())

# COMMAND ----------

print(calc_df.filter('cash_delta < -1 AND loanmod = "Y"').count(), calc_df.filter('cash_delta > 1 AND loanmod = "Y"').count())

# COMMAND ----------

#single payer vs out of all payers, 95485 total, 7% was multi payer
print(calc_df.filter('cash_delta BETWEEN -1 AND 1').count()/calc_df.count(), calc_df.filter('cash_delta BETWEEN -1 AND 1').count()/(calc_df.count()+5949))

# COMMAND ----------

print(calc_df.filter('diff = PaymentReceived_sum').count())

# COMMAND ----------

display(calc_df.filter('diff = PaymentReceived_sum'))

# COMMAND ----------

display(calc_df.filter('cash_delta > 10'))

# COMMAND ----------

calc_df.agg({'cash_delta': 'max'}).union(calc_df.select('cash_delta').agg({'cash_delta':'min'})).show()



# COMMAND ----------

print(calc_df.filter('cash_delta > 2').count())

# COMMAND ----------

# calc_df.toPandas().to_csv('/dbfs/mnt/science/alison_blobstorage/month_loantape/loantape_output/January_2021_InterestDueSinglePayerOutput_03_10_21.csv',index=False)

# COMMAND ----------

blue_check_out = (spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/science/alison_blobstorage/month_loantape/loantape_output/January_2021_InterestDueSinglePayerOutput_03_10_21.csv")
                 )
display(blue_check_out)

# COMMAND ----------

display(blue_check_out.filter("cash_delta > 990"))

# COMMAND ----------

display(blue_check_out.filter("cash_delta < -20000"))

# COMMAND ----------

display(blue_check_out.filter("InterestPaid is NULL"))

# COMMAND ----------



# COMMAND ----------

display(blue_check_out.filter("loanid = 'PA25C15722366'"))

# COMMAND ----------

display(jan_output.filter("loanid = 'P0E887E5D19E8'"))

# COMMAND ----------

#January RemainingPrincipal as February beginning balance

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
            interestrate,
            remainingprincipal as februarybeginningbalance_lt,  
            edition
            from "BUSINESS_INTELLIGENCE"."DATA_STORE"."MVW_LOAN_TAPE_MONTHLY"
    where asofdate = '2021-1-31' 
    order by asofdate"""
#VW_STANDARD_LOAN_TAPE_DAILY_HISTORY_DRAFT
feb_df = spark.read \
  .format('snowflake') \
  .options(**options) \
  .option('query',query) \
  .load()
  #snowflake makes column names uppercase. This changes to lowercase
test = feb_df.toDF(*[c.lower() for c in feb_df.columns])
display(test)

# COMMAND ----------

display(test.filter("loanid = 'P6CB87AE3C1F9'"))

# COMMAND ----------

options = create_snowflake_options(url= "https://happymoney.us-east-1.snowflakecomputing.com",
                            database="DATA_SCIENCE_DEV",
                            schema="MODEL_VAULT",
                            warehouse="DATA_SCIENCE_DEV",
                            role="DATA_SCIENCE_DEV",
                            preactions=None,
                            secrets_scope="snowflake_datascience", #snowflake_cron_pii
                            user_key="username",
                            pass_key="password")
'PayoffUID','LoanID','PortfolioID','PortfolioName','MaturityDate','LoanAmount','Term','RemainingPrincipal','InterestRate'
query = """ Select asofdate, 
            cycledate,
            loanid, 
            payoffuid,
            portfolioid,
            portfolioname,
            maturitydate,
            loanamount,
            term,
            remainingprincipal as jan_beginningbalance,
            interestrate,
            nextpaymentduedate as nextpaymentduedate_pull,
            loanmod,
            status,
            loansource,
            edition
            from "BUSINESS_INTELLIGENCE"."DATA_STORE"."MVW_LOAN_TAPE_MONTHLY"
    where asofdate = '2020-12-31' 
    order by asofdate"""
#VW_STANDARD_LOAN_TAPE_DAILY_HISTORY_DRAFT
new_df = spark.read \
  .format('snowflake') \
  .options(**options) \
  .option('query',query) \
  .load()
  #snowflake makes column names uppercase. This changes to lowercase
lt_df = new_df.toDF(*[c.lower() for c in new_df.columns])
display(lt_df)

# COMMAND ----------

display(lt_df.filter("loanid = 'P6CB87AE3C1F9'"))

# COMMAND ----------

display(blue_check_out.filter("loanid = 'P6CB87AE3C1F9'"))

# COMMAND ----------

display(jan_output.filter("loanid = 'P6CB87AE3C1F9'"))

# COMMAND ----------


