# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

nov_lt = (spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/science/alison_blobstorage/month_loantape/December_loantape/All November Loan Tapes.csv")
         .filter("Status = 'Current'")
         )
dec_orig = spark.read.format("csv").option("header","true").option("inferSchema","true").load("mnt/science/alison_blobstorage/month_loantape/December_loantape/December 2020 Originations.csv")
# validated_june = spark.read.csv("mnt/science/alison_blobstorage/month_loantape/paytype/Validated Data from January 2020 - June 2020 Pay Types.csv", header =True,inferSchema = True).withColumnRenamed('PayType22','PayType').withColumnRenamed('Principal w/ Patytypes', 'Principal w/ Paytypes').drop('Paytype19')

dec_trans = (spark.read.format("csv").option("header","true").option("inferSchema","true").load("mnt/science/alison_blobstorage/month_loantape/December_loantape/December Transactions.csv")
              .filter("LoanID != 'NULL'")
              .withColumn('EffectiveDate_new', F.to_date('EffectiveDate', 'MM/dd/yy'))
              .withColumnRenamed(' Amount ', 'Amount')
            )
dec_lt = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/science/alison_blobstorage/month_loantape/December_loantape/All December Loan tape.csv").filter("Status = 'Current'")
display(dec_orig)

# COMMAND ----------

display(nov_lt.filter("InterestRate == 0"))

# COMMAND ----------

# DBTITLE 1,Get earliest transaction date and establish multi-payer
from pyspark.sql import Window
import pyspark.sql.functions as F

#
# window = Window.partitionBy('LoanID','EffectiveDate_new').orderBy('LoanID', F.asc('EffectiveDate_new'))\
#                .rowsBetween(Window.unboundedPreceding, 0)

window = Window.partitionBy('LoanID').orderBy('LoanID', 'EffectiveDate_new')\
               .rowsBetween(Window.unboundedPreceding, 0)
#pull first date associated with multi payer, some are multipayers on same day
dupe_check = (dec_trans
                 .withColumn('duplicate_count', (F.count('LoanID').over(window))) 
                .filter("duplicate_count == 1")
                 )
#get multi-payer
multi_payer = dec_trans.groupBy("LoanID").count().filter("count > 1").select("LoanID")

#put get sum of all payments, put dec df together
loan_pay_agg = dec_trans.groupby(['LoanID']).agg(F.round(F.sum('Amount'),2).alias('PaymentReceived_sum'))

dec_agg_trans = dupe_check.join(loan_pay_agg, how = 'left', on = 'LoanID').drop("duplicate_count","Amount")              
              
display(dupe_check.orderBy("LoanID"))

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,6008 multi-payers out of 92194 people
print(multi_payer.select("LoanID").distinct().count(),dec_trans.select("LoanID").distinct().count(), dec_trans.count(),dupe_check.count(),dec_agg_trans.count())

# COMMAND ----------

display(dec_agg_trans )         

# COMMAND ----------

from pyspark.sql.functions import col
display(dec_orig.select('PayoffUID','LoanID','PortfolioID','PortfolioName','MaturityDate','LoanAmount','Term','Rate')
                    .withColumn("BeginningBalance", col("LoanAmount"))
                    .select('PayoffUID','LoanID','PortfolioID','PortfolioName','MaturityDate','LoanAmount','Term','BeginningBalance','Rate'))


# COMMAND ----------

from pyspark.sql.functions import col
#get beginning balance for all dec, combining originations + loantape
dec_bb = (nov_lt.withColumnRenamed('LoanId','LoanID')
            .select('PayoffUID','LoanID','PortfolioID','PortfolioName','MaturityDate','LoanAmount','Term','RemainingPrincipal','InterestRate')
            .withColumnRenamed('RemainingPrincipal','BeginningBalance')
            .withColumn('InterestRate', F.round(col('InterestRate')*100,2))
            .withColumnRenamed('InterestRate','Rate')
         )
dec_full_bb =(dec_orig
                    .withColumn("BeginningBalance", col("LoanAmount"))
                    .select('PayoffUID','LoanID','PortfolioID','PortfolioName','MaturityDate','LoanAmount','Term','BeginningBalance','Rate')
                    .union(dec_bb)
                    .join(dec_agg_trans.select("PortfolioID","LoanID","EffectiveDate_new","PaymentReceived_sum"), how = 'left', on = 'LoanID')
              )
display(dec_full_bb)      


# COMMAND ----------

print(nov_lt.filter("BeginningBalance == 0").count(), dec_full_bb.filter("BeginningBalance == 0").count())

# COMMAND ----------

# DBTITLE 1,98363 loans
dec_full_bb.select("LoanID").distinct().count()

# COMMAND ----------

dec_full_bb[["LoanID"]].join(nov_lt[["LoanID"]], how = 'inner', on = "LoanID").count()

# COMMAND ----------

# DBTITLE 1,93% made payments, 7% no payment, 6169 no payment
print(dec_full_bb.select("LoanID").distinct().count(),dec_trans.select("LoanID").distinct().count())

# COMMAND ----------

# DBTITLE 1,All loans
print(dec_full_bb.count(),dec_full_bb.select('LoanID').distinct().count())

# COMMAND ----------

display(dec_full_bb.select('LoanID').groupby('LoanID').count().filter("count > 1").select("LoanID").join(dec_full_bb, on= 'LoanID', how = 'left'))

# COMMAND ----------

# MAGIC %run ../loantape_lib

# COMMAND ----------

calc_monthly_loan_payment_udf = F.udf(calc_monthly_loan_payment,FloatType())
# calc_monthly_loan_payment_udf = F.udf(calc_monthly_loan_payment,DecimalType(scale=2))
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
    StructField("NextPaymentDate", DateType(), True)
])
payment_allocation_udf = F.udf(payment_allocation,schema)

# COMMAND ----------

# MAGIC %run Users/atan@happymoney.com/get_snowflake_data

# COMMAND ----------

# DBTITLE 1,NextPaymentDate
options = create_snowflake_options(url= "https://happymoney.us-east-1.snowflakecomputing.com",
                            database="DATA_SCIENCE_DEV",
                            schema="MODEL_VAULT",
                            warehouse="DATA_SCIENCE_DEV",
                            role="DATA_SCIENCE_DEV",
                            preactions=None,
                            secrets_scope="snowflake_datascience", #snowflake_cron_pii
                            user_key="username",
                            pass_key="password")

query = """Select loanid, 
            asofdate, 
            nextpaymentduedate,
            edition
            from "BUSINESS_INTELLIGENCE"."DATA_STORE"."MVW_LOAN_TAPE_MONTHLY"
    where asofdate = '2020-11-30' 
    order by asofdate"""
#VW_STANDARD_LOAN_TAPE_DAILY_HISTORY_DRAFT
#VW_STANDARD_LOAN_TAPE_MONTHLY_HISTORY_DRAFT
nextpaymentdate_df = spark.read \
  .format('snowflake') \
  .options(**options) \
  .option('query',query) \
  .load()
  #snowflake makes column names uppercase. This changes to lowercase
new_df = nextpaymentdate_df.toDF(*[c.lower() for c in nextpaymentdate_df.columns]).withColumnRenamed('loanid','LoanID').withColumnRenamed('nextpaymentduedate','NextPaymentDate_pull').filter("NextPaymentDate_pull is not NULL")
display(new_df)

# COMMAND ----------

# display(new_df.select('LoanID').groupby('LoanID').count().filter("count > 1").select("LoanID").join(new_df, on= 'LoanID', how = 'left'))

# COMMAND ----------

print(new_df.filter("NextPaymentDate_pull is Null").count(), new_df.filter("NextPaymentDate_pull is Not Null").count())

# COMMAND ----------

query = '''SELECT PAYOFF_LOAN_ID, CURRENT_PAYMENT_TYPE, LOAN_STATUS From BUSINESS_INTELLIGENCE.DATA_STORE.MVW_LOAN_ORIGINATIONS_INTEREST_DUE'''
df = spark.read \
  .format('snowflake') \
  .options(**options) \
  .option('query',query) \
  .load()
  #snowflake makes column names uppercase. This changes to lowercase
loan_status = df.toDF(*[c.lower() for c in df.columns])
new_loan_status = loan_status.withColumnRenamed("payoff_loan_id", "LoanID").withColumnRenamed("current_payment_type","PayType")
display(loan_status)

# COMMAND ----------

loan_status.count()

# COMMAND ----------

display(new_loan_status)

# COMMAND ----------



# COMMAND ----------

# from pyspark.sql.types import ArrayType, FloatType, DateType, IntegerType, StringType, DecimalType, DoubleType
# from pyspark.sql.functions import udf
# from datetime import date, datetime, timedelta


# def calc_monthly_loan_payment(loan_amount, total_n_payments, interest, mths_year = 12):
#   """
#   Function for calculating an estimated loan payment given a requested loan amount, term, and interest
#   amount per pay period = P *( r(1+r)^n / (1+r)^n - 1), where P is the initial principal, r is the interest rate per period, and n is the total # of payments  
  
#   :param requested_loan_amount - pyspark dataframe column
#   :total_n_payments term (in months)
#   :interest interest rate (percentage)
#   :mnths_year number of months to divide the interest rate (12 for yearly)
  
#   :return a float of the estimated monthly payment
#   """
#   try:
#     if interest == 0 or interest == None:
#       monthly_payment_amount = None
#     else:
#       interest_convert = interest/ 100.0

#       rate = interest_convert/mths_year

#       # monthly_payment_amount = ((1 + apr)**36 - 1)/(apr*(1 + apr)**36)
#       monthly_payment_amount = round(loan_amount * (rate*(1+rate)**total_n_payments)/ ((1+rate)**total_n_payments -1),4)
#   except:
#     monthly_payment_amount = -999.0

#   return monthly_payment_amount

# calc_monthly_loan_payment_udf = F.udf(calc_monthly_loan_payment,FloatType())


# def calc_interest_due(beginning_balance, interest, mths_year = 12):
#   """
#   Function for calculating interest due
  
#   :param beginning balance 
#   :interest interest rate (percentage)
#   :mnths_year number of months to divide the interest rate (12 for yearly)
  
#   :return a float of interest due
#   """
#   if beginning_balance == None:
#     interest_due = None
#   else:
#     interest_convert = interest/ 100  
#     rate = interest_convert/mths_year

#     # monthly_payment_amount = ((1 + apr)**36 - 1)/(apr*(1 + apr)**36)
#     interest_due = round(beginning_balance * rate, 4)
  
#   return interest_due


# calc_interest_due_udf = F.udf(calc_interest_due, FloatType())

# def calc_principal_amount(payment_amount_due, interest_due):
#   """ payment_amount_received: Payment amount 
#       interest_due: amount of interest due during current period
#   """

# #   if payment_amount_received == None or interest_due == None:
# #     principal_amount = None
# #     out = principal_amount
# #   else:
#   try:
#     if interest_due == None:
#       out = None
#     else:
#       principal_amount = payment_amount_due - interest_due

#       out = principal_amount if principal_amount >=0 else 0.0
#   except:
#     out = -999.0
#   return out

# calc_principal_amount_udf = F.udf(calc_principal_amount,FloatType())

# def calc_remaining_principal(beginning_balance, principal_amount):
# #   when(principal_amount<0, 0 )
#   try:
#     if principal_amount == None:
#       remaining_principal = None
#     else:
# #   remaining_principal = round(beginning_balance - principal_amount,4)
#       remaining_principal = round(beginning_balance - principal_amount, 4)
#   except:
#     remaining_principal = -999.0
#   return remaining_principal

# calc_remaining_principal_udf = F.udf(calc_remaining_principal,DoubleType())

# def calculate_payment_status(paytype, payment_date, actual_due_date):
#     #
#     if payment_date == None or actual_due_date == None:
#         status = None
    
#     else:
#         #if paytype is none, treat as if Auto Payer
#         if paytype == None:
#           paytype = 'Auto Payer'

#     # set max_date range depending on pay type
#         if paytype == 'Manual Payer':
#             max_date = actual_due_date - timedelta(days=15)
#         #autopayer
#         else:
#             max_date = actual_due_date - timedelta(days=3)

#         #between time window from due date to max date
#         if actual_due_date >= payment_date >= max_date:
#             status = 'On Time'

#         elif payment_date > actual_due_date:
#             status = 'Late'
#         #if payment date is before earliest they can pay, max_date
#         elif max_date > payment_date :
#             status = 'Early'
#         else:
#             status = None

#     return status
# # generate_maturity_date_udf = F.udf(generate_maturity_date, DateType())
# # generate_next_payment_date_udf = F.udf(generate_next_payment_date, DateType())
# # generate_payment_due_udf = F.udf(generate_payment_due_date, DateType())
# # check_unsatisfied_bill_udf = F.udf(check_unsatisfied_bill, StringType())
# calculate_payment_status_udf = F.udf(calculate_payment_status, StringType())  
# # early_status_udf = F.udf(early_status, FloatType())    




# COMMAND ----------

calculate_payment_status(None, date(2020,1,10), date(2020,1,11))

# COMMAND ----------

from pyspark.sql.functions import when

dec_prep = (dec_full_bb.withColumn('PaymentAmountDue', F.round(calc_monthly_loan_payment_udf(col('BeginningBalance'),  col('Term'),  col('Rate')),2))
            
#              .withColumn("InterestDue", calc_interest_due_udf(col('BeginningBalance'), col('Rate')))      
#              .withColumn("PrincipalAmount", calc_principal_amount_udf( col('PaymentAmountDue'), col('InterestDue')))     
#              .withColumn("PrincipalAmount", when(col("PrincipalAmount") < 0, 0).otherwise(col("PrincipalAmount")))
#              .withColumn("RemainingPrincipal", calc_remaining_principal_udf(col('BeginningBalance'), col('PrincipalAmount')))
#              .withColumn("NextInterestDue", calc_interest_due_udf(col('RemainingPrincipal'),  col('Rate')))
             .withColumn('UnpaidInterest', F.lit(0.0).cast('float'))
             .withColumn('UnpaidPrincipal', F.lit(0.0).cast('float'))
             .join(new_df['LoanID','NextPaymentDate_pull'], on = 'LoanID', how = 'left')
            .join(new_loan_status, how = 'left', on = 'LoanID')
             .withColumn("PayStatus", calculate_payment_status_udf(col("PayType"),col('EffectiveDate_new'),col('NextPaymentDate_pull')))

           )
display(dec_prep.orderBy("LoanID"))

# COMMAND ----------

display(dec_full_bb.filter("LoanID ='P64FED615A96F'"))

# COMMAND ----------

display(dec_prep.select('LoanID').groupby('LoanID').count().filter("count > 1").select("LoanID").join(dec_prep, on= 'LoanID', how = 'left'))

# COMMAND ----------

# display(dec_prep.filter("RemainingPrincipal == -999.0"))

# COMMAND ----------

print(dec_prep.count(), dec_prep.select('LoanID').distinct().count())

# COMMAND ----------

print(dec_prep.filter("PayStatus is NULL").count(), dec_prep.filter("PayType is NULL").count())

# COMMAND ----------

display(dec_prep.filter("LoanID='P135D14B2C71A'"))

# COMMAND ----------

# from pyspark.sql.types import ArrayType,FloatType,StringType,DateType,DecimalType,StructType,StructField
# from dateutil import relativedelta


# def payment_allocation(paytype, payment_amount_due, interest, beginning_balance, payment_amount_received,
#                        next_payment_date, payment_received_date, unpaid_interest, unpaid_principal):
#   try:

#       # payment_amount_due = calc_monthly_payment()
#       if payment_amount_received == 0 or payment_amount_received is None or next_payment_date is None:
#           # payment_received_date = date()
#           payment_amount_received = 0.0
#           remaining_principal = beginning_balance
#           interest_due = calc_interest_due(beginning_balance, interest)
#           principal_due = calc_principal_amount(payment_amount_due, interest_due)
#           interest_paid = 0.0
#           principal_paid = 0.0
#           unpaid_interest = 0.0
#           unpaid_principal = 0.0
#           next_interest_due = calc_interest_due(beginning_balance - principal_due, interest)
#           next_payment_date = next_payment_date
# #           payment_status = None
#           out = (round(remaining_principal,2), interest_due, principal_due, interest_paid, principal_paid, unpaid_interest,
#                  unpaid_principal, next_interest_due, next_payment_date)
#       else:
#           # remaining_principal = beginning_balance
#           # checking for  missed payments
#           # if (payment_received_date.year == next_payment_date.year) & (
#           #         payment_received_date.month == next_payment_date.month):
#           #     #not missing even though payment received date is a few days later than next_payment_date
#           #     missing_payment_counter = 0
#           # else:
#           #     missing_payment_counter = 1

#           #if next_payment date and payment received date in same month
#           #next_payment_date missed payment
#           while (payment_amount_received > 0.0) and next_payment_date < payment_received_date:

#               if (payment_received_date.year == next_payment_date.year) & (payment_received_date.month == next_payment_date.month):
#                   #     #not missing even though payment received date is a few days later than next_payment_date
#                   break


#               print(next_payment_date)
#               interest_due = calc_interest_due(beginning_balance, interest)
#               principal_due = calc_principal_amount(payment_amount_due, interest_due)
#               after_interest = payment_amount_received - interest_due
#               after_principal = after_interest - principal_due

#               if after_interest > 0:  # more than interest can cover some principal
#                   interest_paid = interest_due
#                   # print(payment_amount_received, "interest due", interest_due, "principal due", principal_due,"interest paid", interest_paid, "after principal", after_principal, after_interest)

#                   if after_principal >= 0:  # can cover principal
#                       payment_amount_received -= interest_due + principal_due
#                       beginning_balance = beginning_balance - principal_due
#                       next_payment_date = next_payment_date + relativedelta.relativedelta(months=1)
#                       principal_paid = principal_due
#                       unpaid_principal = unpaid_principal + 0.0
#                       unpaid_interest = unpaid_interest + 0.0
#                       # for next interest calc
#                       remaining_principal = beginning_balance
#                       next_interest_due = calc_interest_due(remaining_principal, interest)
#                       print(next_payment_date, "next interest due", next_interest_due, "beginning bal",
#                             beginning_balance, "principal paid", principal_paid, "interest paid", interest_paid,
#                             payment_amount_received, after_interest, after_principal, unpaid_interest,
#                             unpaid_principal)
#                   else:  # can't cover principal
#                       beginning_balance = beginning_balance - after_interest
#                       principal_paid = after_interest
#                       unpaid_principal = unpaid_principal + principal_due - after_interest
#                       unpaid_interest = unpaid_interest + 0.0
#                       payment_amount_received -= interest_due + after_interest
#                       # for next interest calc
#                       remaining_principal = beginning_balance
#                       next_interest_due = calc_interest_due(remaining_principal, interest)

#                       print(next_payment_date, "next interest due", next_interest_due, "beginning bal",
#                             beginning_balance, payment_amount_received, beginning_balance, "principal paid",
#                             principal_paid, interest_paid,
#                             "unpaid principal", unpaid_principal, unpaid_interest)

#               else:
#                   # elif after_interest < 0: #not enough to pay interest
#                   interest_paid = payment_amount_received
#                   principal_paid = 0.0
#                   unpaid_interest = unpaid_interest + interest_due - interest_paid
#                   unpaid_principal = unpaid_principal + principal_due
#                   payment_amount_received -= payment_amount_received
#                   # for next interest calc
#                   remaining_principal = beginning_balance - principal_paid
#                   next_interest_due = unpaid_interest + calc_interest_due(remaining_principal, interest)

#                   print(next_payment_date, "beginning bal", beginning_balance, payment_amount_received,
#                         after_interest, interest_paid)

#           # print(payment_amount_received, "beginning bal", beginning_balance, "interest due", interest_due, "principal due", principal_due, "interest paid", interest_paid, "principal paid", principal_paid, "unpaid principal",
#           #           unpaid_principal, "unpaid interest", unpaid_interest, "next interest due", next_interest_due, next_payment_date)

#           payment_status = calculate_payment_status(paytype, payment_received_date, next_payment_date)
#           # if in current period, no more late payments
#           if (next_payment_date.month == payment_received_date.month) & (
#                   next_payment_date.year == payment_received_date.year) & (payment_amount_received > 0) \
#                   & (payment_status == 'On Time' or payment_status == 'Late'):
#               # while (next_payment_date.month == payment_received_date.month) & (
#               #             next_payment_date.year == payment_received_date.year) & (payment_amount_received > 0):

#               # if current payment is on time

#               #             if payment_status == 'On Time':
#               interest_due = calc_interest_due(beginning_balance, interest)
#               principal_due = calc_principal_amount(payment_amount_due, interest_due)
#               after_interest = payment_amount_received - interest_due
#               # can you cover principal due

#               after_principal = after_interest - principal_due

#               # more than enough to cover interest
#               if after_interest > 0:
#                   after_interest = payment_amount_received - interest_due

#                   # can you cover principal due
#                   # after_principal = after_interest - principal_due
#                   if (after_principal > 0):  # overpayment next_payment_date still same

#                       interest_paid = interest_due
#                       remaining_principal = beginning_balance - principal_due - after_principal
#                       next_payment_date = next_payment_date + relativedelta.relativedelta(months=1)
#                       next_interest_due = calc_interest_due(remaining_principal, interest)
#                       # remaining_principal = remaining_principal - after_principal
#                       principal_paid = principal_due
#                       payment_amount_received -= principal_paid + interest_paid
#                       unpaid_interest = 0.0
#                       unpaid_principal = 0.0

#                   # either you can cover full payment, or not enough to cover principal
#                   else:
#                       if payment_amount_received == payment_amount_due:
#                           remaining_principal = beginning_balance - principal_due
#                           interest_paid = interest_due
#                           # calc updated next interest due since bill is paid
#                           next_interest_due = calc_interest_due(remaining_principal, interest)
#                           principal_paid = principal_due
#                           unpaid_interest = 0.0
#                           unpaid_principal = 0.0
#                           next_payment_date = next_payment_date + relativedelta.relativedelta(months=1)

#                       # not enough to cover principal
#                       else:

#                           remaining_principal = beginning_balance - after_interest
#                           interest_paid = interest_due
#                           next_interest_due = calc_interest_due(remaining_principal, interest)
#                           principal_paid = after_interest
#                           unpaid_interest = 0.0
#                           unpaid_principal = principal_due - after_interest

#               # payment not enough to cover interest
#               else:
#                   next_payment_date = next_payment_date
#                   interest_paid = payment_amount_received
#                   principal_paid = 0.0
#                   unpaid_interest = interest_due - payment_amount_received
#                   unpaid_principal = principal_due
#                   remaining_principal = beginning_balance
#                   # if current unpaid interest, need to project next interest due, add up current unpaid interest + calculate next interest due using beginning balance? but that didn't change next
#                   next_interest_due = unpaid_interest + calc_interest_due(remaining_principal, interest)

#           #paid missing payment but not enough
#           elif payment_amount_received == 0.0:
#               interest_due = interest_due
#               remaining_principal = remaining_principal
#               principal_paid = principal_paid
#               interest_paid = interest_due
#               unpaid_interest = unpaid_interest
#               unpaid_principal = unpaid_principal
#               next_interest_due = next_interest_due
#           #early payers
#           else:
#               interest_due = calc_interest_due(beginning_balance, interest)
#               principal_due = calc_principal_amount(payment_amount_due, interest_due)
#               remaining_principal = beginning_balance - payment_amount_received
#               interest_paid = 0
#               principal_paid = 0
#               unpaid_interest = interest_due
#               unpaid_principal = principal_due
#               next_payment_date = next_payment_date
#               next_interest_due = calc_interest_due(remaining_principal, interest)

#           out = (round(remaining_principal,2), interest_due, principal_due, interest_paid, principal_paid, unpaid_interest,
#                  unpaid_principal, next_interest_due, next_payment_date)
#   except:
#       remaining_principal = -1.0
#       interest_due = -1.0
#       principal_due = -1.0
#       interest_paid = -1.0
#       principal_paid = -1.0
#       unpaid_interest = -1.0
#       unpaid_principal = -1.0
#       next_interest_due = -1.0
#       next_payment_date = None

#       out = (round(remaining_principal,2), interest_due, principal_due, interest_paid, principal_paid, unpaid_interest,
#              unpaid_principal, next_interest_due, next_payment_date)

#   #     print(payment_amount_received,"beginning bal", beginning_balance, "remaining principal", remaining_principal, "interest due", interest_due, "principal due", principal_due, "interest paid", interest_paid, "principal paid",
#   #     principal_paid, "unpaid principal",unpaid_principal,"unpaid interest",unpaid_interest, "next interest due", next_interest_due, next_payment_date)
#   # remaining_principal, interest_due, principal_due, interest_paid, principal_paid, unpaid_interest,unpaid_principal,next_interest_due,next_payment_date
#   return out

# schema = StructType([
#     StructField("RemainingPrincipal", DoubleType(), True),
#     StructField("InterestDue", FloatType(),True),
#     StructField("PrincipalDue", FloatType(),True),
#     StructField("InterestPaid", FloatType(), True),
#     StructField("PrincipalPaid", FloatType(), True),
#     StructField("UnpaidInterest", FloatType(), True),
#     StructField("UnpaidPrincipal", FloatType(), True),
#     StructField("NextInterestDue", FloatType(), True),
#     StructField("NextPaymentDate", DateType(), True)
# ])
# payment_allocation_udf = F.udf(payment_allocation,schema)

# COMMAND ----------



# COMMAND ----------


payment_allocation("Auto Payer",500.0,10.0,10000.0,None, date(2020,1,20), date(2020,1,19), 0.0,0.0)

# COMMAND ----------

payment_allocation('Auto Payer', 13.4781, 20.63, 438.07, 439.07, date(2020,12,27),date(2020,12,1), 0.0,0.0)

# COMMAND ----------

dec_prep.dtypes

# COMMAND ----------

print(dec_prep.count(), dec_prep.select('LoanID').distinct().count())

# COMMAND ----------

# DBTITLE 1,6 with 0 as beginning balance
print(dec_prep.filter("BeginningBalance == 0").count())

# COMMAND ----------

display(dec_prep.orderBy("LoanID"))

# COMMAND ----------

dec_output = dec_prep.select('LoanID',"Term", "Rate", "PaymentAmountDue",'BeginningBalance', 'PaymentReceived_sum', 'NextPaymentDate_pull', 'EffectiveDate_new', "PayType", "PayStatus", payment_allocation_udf("PayType","PaymentAmountDue", "Rate", "BeginningBalance", "PaymentReceived_sum", "NextPaymentDate_pull", "EffectiveDate_new", "UnpaidInterest","UnpaidPrincipal").alias("allocate")).select("LoanID","PaymentAmountDue","PayStatus", 'BeginningBalance',"Term","Rate","PayType",'PaymentReceived_sum', 'NextPaymentDate_pull', 'EffectiveDate_new', "allocate.*")

display(dec_output)

# COMMAND ----------

display(dec_output.filter("RemainingPrincipal = -1"))

# COMMAND ----------



# data2 = [("Auto Payer",500.0,10.0,10000.0,500.0,date(2020,1,20), date(2020,1,19), 0.0,0.0)
   
#   ]

# schema = StructType([ \
#     StructField("PayType",StringType(),True), \
#     StructField("PaymentAmountDue",FloatType(),True), \
#     StructField("Interest",DoubleType(),True), \
#     StructField("BeginningBalance", DoubleType(), True), \
#     StructField("PaymentAmountReceived_sum", DoubleType(), True), \
#     StructField("NextPaymentDate", DateType(), True), \
#     StructField("PaymentReceivedDate", DateType(), True), \
#     StructField("UnpaidInterest", FloatType(), True), \
#     StructField("UnpaidPrincipal", FloatType(), True) \
                     
#   ])
 
# example_df = spark.createDataFrame(data=data2,schema=schema)
# display(example_df.select(payment_allocation_udf("PayType","PaymentAmountDue", "Interest", "BeginningBalance", "PaymentAmountReceived_sum", "NextPaymentDate", "PaymentReceivedDate", "UnpaidInterest","UnpaidPrincipal").alias("allocate")). select("allocate.*"))

# COMMAND ----------

print(dec_output.filter("PayStatus is Null").count(),dec_output.filter("RemainingPrincipal != -1").count(), dec_output.count(),dec_output.filter("RemainingPrincipal == -1").count())

# COMMAND ----------

dec_lt.columns

# COMMAND ----------

# DBTITLE 1,364 loans not in output but in december loantape
total_dec_lt = dec_lt.select("LoanID","RemainingPrincipal").select("LoanID").distinct().count()
total = dec_output.count()
print(total_dec_lt, total, total_dec_lt-total) 

# COMMAND ----------

match_back = (dec_output.join(dec_lt.select("LoanID","RemainingPrincipal").withColumnRenamed("RemainingPrincipal","JanuaryBeginningBalance"), how = 'left', on= 'LoanID')
              .withColumn('diff', (F.abs(col("RemainingPrincipal") - col("JanuaryBeginningBalance"))))
             )

# select("LoanID","BeginningBalance","RemainingPrincipal","JulyBeginningBalance","InterestDue","PrincipalDue","PaymentAmountDue","Rate","diff")
print(match_back.filter("diff ==0").count()/total, match_back.filter("diff > .02").count()/total,  )



# COMMAND ----------

print(match_back.filter("diff < .02").count()/total)

# COMMAND ----------

print(match_back.filter("PayType is NULL").count())

# COMMAND ----------

display(match_back.orderBy("LoanID"))

# COMMAND ----------

non_matches = match_back.filter("diff > .02")
print(non_matches.count())

# COMMAND ----------

display(non_matches)

# COMMAND ----------

multi_payer1 = multi_payer.withColumn('multi_payer_flag', F.lit(1))

display(multi_payer1)

# COMMAND ----------

multi_payer1.count()

# COMMAND ----------

non_matches[['LoanID']].join(multi_payer,how='inner',on='LoanID').count()

# COMMAND ----------

non_match_multi_payer_flag = (non_matches.join(multi_payer1,how='left',on='LoanID')
                              .withColumn('multi_payer_flag', when(col('multi_payer_flag').isNull(), 0).otherwise(1))
                             )

display(non_match_multi_payer_flag)

# COMMAND ----------

non_match_multi_payer_flag.filter("multi_payer_flag== 1").count()

# COMMAND ----------

display(non_match_multi_payer_flag.filter("LoanID == 'P509481FE8515'"))

# COMMAND ----------

# match_back.toPandas().to_csv("/dbfs/mnt/science/alison_blobstorage/month_loantape/loantape_output/Dec_all_scenario_AT_01_26_21.csv",index = False)
# non_match_multi_payer_flag.toPandas().to_csv("/dbfs/mnt/science/alison_blobstorage/month_loantape/loantape_output/Dec_non_matches_AT_01_26_21.csv",index = False)

# COMMAND ----------


