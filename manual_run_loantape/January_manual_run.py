# Databricks notebook source
# MAGIC %run Users/atan@happymoney.com/get_snowflake_data

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import col



jan_transactions = (spark.read.format('csv').option("header","true").option('inferSchema','true').load("/mnt/science/alison_blobstorage/month_loantape/January_2021_transactions.csv")
#                   .withColumn('EffectiveDate_new', F.to_date('EffectiveDate', 'MM/dd/yy'))
                    .withColumn('EffectiveDate_new', F.to_date(F.to_timestamp('EffectiveDate', 'M/d/yyyy')))
                    .withColumnRenamed(' Amount ', 'Amount'))
display(jan_transactions)

# COMMAND ----------

jan_transactions.filter(col(' Amount ') < 0).count()

# COMMAND ----------

# DBTITLE 1,Establish multiplayer
from pyspark.sql import Window
import pyspark.sql.functions as F


recoveries = jan_transactions.filter(col('Amount') < 0)

#get all payments that are not recoveries
jan_trans_only = jan_transactions.filter(col('Amount') > 0)
#how many duplicates are in jan_trans_only
window = Window.partitionBy('LoanID').orderBy('LoanID', 'EffectiveDate_new')\
               .rowsBetween(Window.unboundedPreceding, 0)

#get id's of all who made more than 1 payment (not including recoveries)
dupe_check = (jan_trans_only
                 .withColumn('duplicate_count', (F.count('LoanID').over(window))) 
                .filter("duplicate_count > 1")          
                .select('LoanID') 
                .distinct()
                 )
# #remove multipayer id's to get only transactions where member paid once
single_payer_trans = (jan_trans_only.join(dupe_check, how = 'left_anti', on = 'LoanID')
                      .union(recoveries)
                     )

# #all transactions flagged
# all_trans_flagged = (jan_transactions.join(dupe_check.withColumn('multipayer_flag', F.lit(1)), how = 'left',on = 'LoanID')
# #                 
#                      .withColumn('multipayer_flag', F.when(col('multipayer_flag').isNull(),0).otherwise(col('multipayer_flag')))
#                    )

#single payer payment + recoveries
single_payer_agg = (single_payer_trans
                    .groupby(['LoanID']).agg(F.round(F.sum('Amount'),2).alias('PaymentReceived_sum'))
                    .join(jan_trans_only[['LoanID','PortfolioID','EffectiveDate','EffectiveDate_new']], how = 'left', on = 'LoanID')
#                     .withColumn('multipayer_flag', F.when(col('multipayer_flag').isNull(),0))
                   )
print(dupe_check.count(), jan_transactions.count(), jan_transactions.select('LoanID').distinct().count(), single_payer_agg.distinct().count(),single_payer_trans.select('LoanID').distinct().count() ) #95,517 


# COMMAND ----------

#add all 

display(single_payer_agg)

# COMMAND ----------

# DBTITLE 1,Last Dec verified loan tape grab RP as Beginning Balance
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

display(lt_df.select('status').distinct())

# COMMAND ----------

# DBTITLE 1,Daily Originations from January
query = """Select asofdate,
            payoffuid,
            loanid, 
            portfolioid,
            portfolioname,
            maturitydate,
            loanamount,
            term,
            loanamount as jan_BeginningBalance,
            interestrate,
            nextpaymentduedate as nextpaymentduedate_pull,
            loanmod,
            loansource,
            status,
            createdat,
            updatedat,
            edition
            from "BUSINESS_INTELLIGENCE"."DATA_STORE"."MVW_LOAN_TAPE_DAILY_HISTORY"
    WHERE month(originationdate) = 1
    AND year(originationdate) = 2021
    AND originationdate = asofdate
    order by asofdate"""
#    AND originationdate = '2020-12-01'

#VW_STANDARD_LOAN_TAPE_DAILY_HISTORY_DRAFT
#VW_STANDARD_LOAN_TAPE_MONTHLY_HISTORY_DRAFT
daily_dec_orig_df = spark.read \
  .format('snowflake') \
  .options(**options) \
  .option('query',query) \
  .load()
  #snowflake makes column names uppercase. This changes to lowercase
daily_dec_df = (daily_dec_orig_df.toDF(*[c.lower() for c in daily_dec_orig_df.columns])
#           .withColumnRenamed('loanid','LoanID')
#           .withColumnRenamed('nextpaymentduedate','NextPaymentDate_pull')
#           .withColumnRenamed('loanamount', 'Jan_BeginningBalance')
         )
display(daily_dec_df)

# COMMAND ----------

print(daily_dec_df.cache().count())

# COMMAND ----------

display(daily_dec_df.select('asofdate', 'payoffuid','loanid','portfolioid','portfolioname','maturitydate','loanamount','term','jan_beginningbalance','interestrate','nextpaymentduedate_pull','status','edition','loanmod','loansource')
       )

# COMMAND ----------

display(lt_df.select('asofdate', 'payoffuid','loanid','portfolioid','portfolioname','maturitydate','loanamount','term','jan_beginningbalance','interestrate','nextpaymentduedate_pull','status','edition','loanmod','loansource')
       )

# COMMAND ----------

# DBTITLE 1,Beginning balance for Jan
from pyspark.sql.functions import col
#keep charge_off /paid-in-full loans
# new_lt_df = (lt_df.filter((col('status')!= 'Charge off') & (col('status') != 'Paid in Full'))
#               .select('asofdate', 'payoffuid','loanid','portfolioid','portfolioname','maturitydate','loanamount','term','jan_beginningbalance','interestrate','nextpaymentduedate_pull','status','edition','loanmod','loansource')
#             )
            
# daily_df = (daily_dec_df.filter((col('status')!= 'Charge off') & (col('status') != 'Paid in Full'))
#                 .select('asofdate', 'payoffuid','loanid','portfolioid','portfolioname','maturitydate','loanamount','term','jan_beginningbalance','interestrate','nextpaymentduedate_pull','status','edition','loanmod','loansource')
#            )
new_lt_df = (lt_df.filter((col('status')!= 'Sold'))
              .select('asofdate', 'payoffuid','loanid','portfolioid','portfolioname','maturitydate','loanamount','term','jan_beginningbalance','interestrate','nextpaymentduedate_pull','status','edition','loanmod','loansource')
            )
            
daily_df = (daily_dec_df.filter((col('status')!= 'Sold'))
                .select('asofdate', 'payoffuid','loanid','portfolioid','portfolioname','maturitydate','loanamount','term','jan_beginningbalance','interestrate','nextpaymentduedate_pull','status','edition','loanmod','loansource')
           )
jan_bb = (new_lt_df.union(daily_df)
          
#          .filter(((col('status')!= 'Charge off') & (col('status') != 'Paid in Full')))
         )
display(jan_bb)

# COMMAND ----------



# COMMAND ----------

display(new_lt_df.filter("loanid = 'PA25C15722366'"))

# COMMAND ----------

display(lt_df.filter("loanid = 'PA25C15722366'"))

# COMMAND ----------

display(lt_df.filter("loanid= 'PCE3C92B9ABB6'"))

# COMMAND ----------

# MAGIC %run ../loantape_lib

# COMMAND ----------

calc_monthly_loan_payment_udf = F.udf(calc_monthly_loan_payment,FloatType())
calc_interest_due_udf = F.udf(calc_interest_due, FloatType())
calc_principal_amount_udf = F.udf(calc_principal_amount,FloatType())
calc_remaining_principal_udf = F.udf(calc_remaining_principal,DoubleType())
generate_maturity_date_udf = F.udf(generate_maturity_date, DateType())
generate_next_payment_date_udf = F.udf(generate_next_payment_date, DateType())
generate_payment_due_udf = F.udf(generate_payment_due_date, DateType())
check_unsatisfied_bill_udf = F.udf(check_unsatisfied_bill, StringType())
calculate_payment_status_udf = F.udf(calculate_payment_status, StringType())  
early_status_udf = F.udf(early_status, FloatType())    


schema = StructType([
    StructField("RemainingPrincipal", DoubleType(), True),
    StructField("InterestDue", FloatType(),True),
    StructField("PrincipalDue", FloatType(),True),
    StructField("InterestPaid", FloatType(), True),
    StructField("PrincipalPaid", FloatType(), True),
    StructField("UnpaidInterest", FloatType(), True),
    StructField("UnpaidPrincipal", FloatType(), True),
    StructField("NextInterestDue", FloatType(), True),
    StructField("NextPaymentDate", DateType(), True),
    StructField("Overpay", FloatType(), True)
  
])
payment_allocation_udf = F.udf(payment_allocation,schema)

# COMMAND ----------

query = '''SELECT PAYOFF_LOAN_ID, CURRENT_PAYMENT_TYPE, LOAN_STATUS From BUSINESS_INTELLIGENCE.DATA_STORE.MVW_LOAN_ORIGINATIONS_INTEREST_DUE'''
df = spark.read \
  .format('snowflake') \
  .options(**options) \
  .option('query',query) \
  .load()
  #snowflake makes column names uppercase. This changes to lowercase
payer_type = df.toDF(*[c.lower() for c in df.columns])
paytype_df = payer_type.withColumnRenamed("payoff_loan_id", "loanid").withColumnRenamed("current_payment_type","paytype")
display(paytype_df)

# COMMAND ----------

jan_calc_df = (single_payer_agg
        .withColumnRenamed('LoanID', 'loanid')
        .join(jan_bb, how = 'left', on = 'loanid')
        .join(paytype_df[['loanid','paytype']], how = 'left', on = 'loanid')
              )

# COMMAND ----------

jan_prep = (jan_calc_df
        .withColumn('term', col('term').cast('float'))
        .withColumn('jan_beginningbalance', col('jan_beginningbalance').cast('float'))
        .withColumn('interestrate', col('interestrate').cast('float'))            
        .withColumn('PaymentAmountDue', F.round(calc_monthly_loan_payment_udf(col('jan_beginningbalance'),  col('term'),  col('interestrate')),2))
        .withColumn('UnpaidInterest', F.lit(0.0).cast('float'))
        .withColumn('UnpaidPrincipal', F.lit(0.0).cast('float'))
        .withColumn("PayStatus", calculate_payment_status_udf(col("paytype"),col('EffectiveDate_new'),col('nextpaymentduedate_pull')))       

       )

display(jan_prep)

# COMMAND ----------



# COMMAND ----------

jan_output = jan_prep.select('loanid',"term", "interestrate", "PaymentAmountDue",'jan_beginningbalance', 'PaymentReceived_sum', 'nextpaymentduedate_pull', 'EffectiveDate_new', "paytype", "PayStatus","status","loanmod", payment_allocation_udf("paytype","PaymentAmountDue", "interestrate", "jan_beginningbalance", "PaymentReceived_sum", "nextpaymentduedate_pull", "EffectiveDate_new", "UnpaidInterest","UnpaidPrincipal").alias("allocate")).select("loanid","paystatus", 'jan_beginningbalance',"term","interestrate","paytype","PaymentAmountDue",'PaymentReceived_sum', 'nextpaymentduedate_pull', 'EffectiveDate_new','loanmod','status', "allocate.*")

display(jan_output.filter("loanid= 'P52621F66C513'"))

# COMMAND ----------

def calculate_end_of_loan(df):
  
  overpay_df= (df.withColumn("Overpay_end_of_loan", F.when((col("RemainingPrincipal") <0) & (col('InterestDue') != -1), F.abs(col("RemainingPrincipal"))).otherwise(0.0))
               .withColumn("RemainingPrincipal", F.when(col("RemainingPrincipal") <0, 0.0).otherwise(col("RemainingPrincipal")))
              )
  
  return overpay_df

overpay_df = calculate_end_of_loan(jan_output)
display(overpay_df)

# display(jan_output.select('*', calculate_end_of_loan_udf("RemainingPrincipal").alias("eol")).select('*', 'eol.*'))

# COMMAND ----------

display(overpay_df.filter("EffectiveDate_new IS NULL"))

# COMMAND ----------

display(jan_output.filter("RemainingPrincipal <0"))

# COMMAND ----------

jan_output = jan_prep.select('loanid',"term", "interestrate", "PaymentAmountDue",'jan_beginningbalance', 'PaymentReceived_sum', 'nextpaymentduedate_pull', 'EffectiveDate_new', "paytype", "PayStatus","status","loanmod", payment_allocation_udf("paytype","PaymentAmountDue", "interestrate", "jan_beginningbalance", "PaymentReceived_sum", "nextpaymentduedate_pull", "EffectiveDate_new", "UnpaidInterest","UnpaidPrincipal").alias("allocate")).select("loanid","paystatus", 'jan_beginningbalance',"term","interestrate","paytype","PaymentAmountDue",'PaymentReceived_sum', 'nextpaymentduedate_pull', 'EffectiveDate_new','loanmod','status', "allocate.*")

display(jan_output)

# COMMAND ----------

print(jan_output.count())

# COMMAND ----------

# jan_output.write.mode("overwrite").saveAsTable("science.interest_due_output_single_pay_jan_2021", path="/mnt/science/alison_blobstorage/month_loantape/loantape_output/interest_due_output_single_pay_jan_2021")



# COMMAND ----------



# COMMAND ----------

member_data = spark.sql("""
SELECT * FROM science.interest_due_output_single_pay_jan_2021
""")
