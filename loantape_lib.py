# Databricks notebook source


# COMMAND ----------



# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import ArrayType, FloatType, DateType, IntegerType, StringType, DecimalType, DoubleType
from pyspark.sql.functions import udf

import datetime
from dateutil import relativedelta


# COMMAND ----------

# from decimal import Decimal

# def determine_beginning_balance(beginning_balance, loan_amount, lender_origination_date, as_of_date):
#   if (lender_origination_date.year == as_of_date.year) & (lender_origination_date.month == as_of_date.month):
#     beginning_balance = loan_amount
#   else:
#     beginning_balance = beginning_balance
#   return F.round(beginning_balance,2)


def calc_monthly_loan_payment(loan_amount, total_n_payments, interest, mths_year = 12):
  """
  Function for calculating an estimated loan payment given a requested loan amount, term, and interest
  amount per pay period = P *( r(1+r)^n / (1+r)^n - 1), where P is the initial principal, r is the interest rate per period, and n is the total # of payments  
  
  :param requested_loan_amount - pyspark dataframe column
  :total_n_payments term (in months)
  :interest interest rate (percentage)
  :mnths_year number of months to divide the interest rate (12 for yearly)
  
  :return a float of the estimated monthly payment
  """
  try:
    if interest > 1:
       interest_convert = interest/ 100
    else:
        interest_convert = interest   
    
    if interest == 0 or None:
      monthly_payment_amount = None
    else:
#       interest_convert = interest/ 100.0

      rate = interest_convert/mths_year

      # monthly_payment_amount = ((1 + apr)**36 - 1)/(apr*(1 + apr)**36)
      monthly_payment_amount = round(loan_amount * (rate*(1+rate)**total_n_payments)/ ((1+rate)**total_n_payments -1),4)
  except:
    monthly_payment_amount = -999.0

  return monthly_payment_amount


def calc_interest_due(beginning_balance, interest, mths_year = 12):
  """
  Function for calculating interest due
  
  :param beginning balance 
  :interest interest rate (percentage)
  :mnths_year number of months to divide the interest rate (12 for yearly)
  
  :return a float of interest due
  """
  if interest > 1:
    interest_convert = interest/ 100
  else:
    interest_convert = interest
    
  if beginning_balance == None:
    interest_due = None
  else:
     
    rate = interest_convert/mths_year

    # monthly_payment_amount = ((1 + apr)**36 - 1)/(apr*(1 + apr)**36)
    interest_due = round(beginning_balance * rate, 4)
  
  return interest_due



def calc_principal_amount(payment_amount_due, interest_due):
  """ payment_amount_received: Payment amount 
      interest_due: amount of interest due during current period
  """

#   if payment_amount_received == None or interest_due == None:
#     principal_amount = None
#     out = principal_amount
#   else:
  try:
    if interest_due == None:
      out = None
    else:
      principal_amount = payment_amount_due - interest_due

      out = principal_amount if principal_amount >=0 else 0.0
  except:
    out = -999.0
  return out


def calc_remaining_principal(beginning_balance, principal_amount):
#   when(principal_amount<0, 0 )
  try:
    if principal_amount == None:
      remaining_principal = None
    else:
#   remaining_principal = round(beginning_balance - principal_amount,4)
      remaining_principal = round(beginning_balance - principal_amount, 4)
  except:
    remaining_principal = -999.0
  return remaining_principal

def calculate_payment_status(paytype, payment_date, actual_due_date):
    #
    if payment_date == None or actual_due_date == None:
        status = None
    
    else:
        #if paytype is none, treat as if Auto Payer
        if paytype == None:
          paytype = 'Auto Payer'

    # set max_date range depending on pay type
        if paytype == 'Manual Payer':
            max_date = actual_due_date - timedelta(days=15)
        #autopayer
        else:
            max_date = actual_due_date - timedelta(days=3)

        #between time window from due date to max date
        if actual_due_date >= payment_date >= max_date:
            status = 'On Time'

        elif payment_date > actual_due_date:
            status = 'Late'
        #if payment date is before earliest they can pay, max_date
        elif max_date > payment_date :
            status = 'Early'
        else:
            status = None

    return status

# COMMAND ----------



# # def determine_beginning_balance(beginning_balance, loan_amount, lender_origination_date, as_of_date):
# #   if (lender_origination_date.year == as_of_date.year) & (lender_origination_date.month == as_of_date.month):
# #     beginning_balance = loan_amount
# #   else:
# #     beginning_balance = beginning_balance
# #   return F.round(beginning_balance,2)

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
  
#   interest_convert = interest/ 100

#   rate = interest_convert/mths_year

#   # monthly_payment_amount = ((1 + apr)**36 - 1)/(apr*(1 + apr)**36)
#   monthly_payment_amount = F.round(loan_amount * (rate*(1+rate)**total_n_payments)/ ((1+rate)**total_n_payments -1),4)

#   return monthly_payment_amount


# def calc_interest_due(beginning_balance, interest, mths_year = 12):
#   """
#   Function for calculating interest due
  
#   :param beginning balance 
#   :interest interest rate (percentage)
#   :mnths_year number of months to divide the interest rate (12 for yearly)
  
#   :return a float of interest due
#   """
    
#   interest_convert = interest/ 100  
#   rate = interest_convert/mths_year

#   # monthly_payment_amount = ((1 + apr)**36 - 1)/(apr*(1 + apr)**36)
#   interest_due = F.round(beginning_balance * rate, 4)
  
#   return interest_due

# # @udf("float")
# def calc_principal_amount(payment_amount_received, interest_due):
#   """ payment_amount_received: Payment amount 
#       interest_due: amount of interest due during current period
#       calculates amount going toward principal
#   """
  
#   principal_amount = F.round(payment_amount_received - interest_due, 4)

#   return principal_amount
# # calculate_principal_amount_udf = F.udf(calc_principal_amount, FloatType())


# def calc_remaining_principal(beginning_balance, principal_amount):
# #   when(principal_amount<0, 0 )
    
#   remaining_principal = F.round(beginning_balance - principal_amount,4)
#   return remaining_principal

# # def calculate_ending_balance(beginning_balance, payment_amount):
# #   end_balance = F.round(beginning_balance - payment_amount, 4)
# #   return end_balance

# COMMAND ----------

def generate_first_payment_date(lender_origination_date):
  """This function will generate the first payment date given params: lender_origination_date
  
  """
  #If lender_origination_date falls on the 29th, 30th, or 31st, first paymen day that rolls to the 1st of the next month, it cannot be on 29th/30th/31st
  no_payment_day = [29,30,31]
  if lender_origination_date.day in no_payment_day:
    sample_month = lender_origination_date.replace(day=1)
    first_payment_date = sample_month.replace(month=sample_month.month+2)
  else:
    first_payment_date =lender_origination_date + relativedelta.relativedelta(months=1)
  return first_payment_date

generate_first_payment_date_udf = F.udf(generate_first_payment_date, DateType())
# generate_first_payment_date_udf = F.udf(lambda z: generate_first_payment_date(z) if z is not None else None, DateType())


def generate_maturity_date(first_payment_date,term):
  """Generate maturity date of loan given first payment date, we use first payment date because it will reflect change-payment-date requests"""
  #first_payment_date will reflect change-payment-date requests (CPD) 
  maturity_date = first_payment_date + relativedelta.relativedelta(months=term-1)
  return maturity_date


def generate_next_payment_date(first_payment_date,maturity_date,as_of_date):
  #if pull date month and year is after first payment date then generate next payment date
  """params: as_of_date pull date at datetime.date object"""
  if (first_payment_date.month <= as_of_date.month) & (first_payment_date.year <= as_of_date.year):
    next_payment_date = as_of_date.replace(day=first_payment_date.day) + relativedelta.relativedelta(months=1)
    #if pull date is after or == to maturity date month and year
  elif (as_of_date.month >= maturity_date.month) & (as_of_date.year >= maturity_date.year):
    next_payment_date = None
  #pull date is before first payment month and year
  elif (as_of_date.month < first_payment_date.month) & (as_of_date.year <= first_payment_date.year):
    next_payment_date = None
  return next_payment_date

# def generate_payment_due_date(first_payment_date, maturity_date, as_of_date=datetime.date.today()):
#   """This function will give you the current payment due date"""
  
#   # if current period same as first payment date
#   if (first_payment_date.month == as_of_date.month) & (first_payment_date.year == as_of_date.year):
#     payment_due_date = first_payment_date
#   #if current period is after first payment but before maturity date
#   elif (first_payment_date < as_of_date < maturity_date):
#     payment_due_date = as_of_date.replace(day=first_payment_date.day)
#     #if current period is same as maturity date
#   elif (as_of_date.month == maturity_date.month) & (as_of_date.year == maturity_date.year):
#     payment_due_date = maturity_date
#     #if current period is beyond maturity date
#   elif (as_of_date.month > maturity_date.month) & (as_of_date.year >= maturity_date.year):
#     payment_due_date = None
#   else:
#     #if current period is before first payment date
#     payment_due_date = None

#   return payment_due_date

def generate_payment_due_date(first_payment_date, maturity_date,next_payment_date, as_of_date=datetime.date.today()):
  """This function will give you the current payment due date using next_payment_date
  Grabs day value from next_payment_date and replaces it in current period due date
  
  """
  
  ref_date = as_of_date
  # if current period same as first payment date
  if (first_payment_date.month == ref_date.month) & (first_payment_date.year == ref_date.year):
    payment_due_date = first_payment_date
  #if current period is after first payment but before maturity date
  elif (first_payment_date < ref_date < maturity_date):
    payment_due_date = ref_date.replace(day=next_payment_date.day)
  #if current period is same as maturity date
  elif (ref_date.month == maturity_date.month) & (ref_date.year == maturity_date.year):
    payment_due_date = maturity_date.replace(day=next_payment_date.day)
  #if current period date is beyond maturity date
  elif (ref_date.month > maturity_date.month) & (ref_date.year >= maturity_date.year):
    payment_due_date = None
  else:
  #if current period is before first payment date
    payment_due_date = None
  

  return payment_due_date



# as_of_date = datetime.date(2020,9,24)
# first_payment_date = datetime.date(2020,7,12)
# maturity_date = datetime.date(2024, 6, 12)


# sample_date = datetime.date(2020, 6,12)
# new_date = generate_first_payment_date(lender_origination_date = sample_date)
# mat_date = generate_maturity_date(new_date, 48)
# print(new_date,mat_date)

# COMMAND ----------

import pyspark.sql.functions as F
from datetime import date, datetime, timedelta
from pyspark.sql.types import ArrayType,FloatType,StringType,DateType,DecimalType,StructType,StructField



def check_unsatisfied_bill(next_payment_date, payment_received_date):
  if next_payment_date == None or payment_received_date == None:
    bill_status = None
  else:
    if (next_payment_date.month == payment_received_date.month) & (next_payment_date.year == payment_received_date.year):
      bill_status = "Previous Bill Satisfied"
    else:
      bill_status = "Previous Bill Unsatisfied"
  return bill_status

# def calculate_payment_status(paytype, payment_date, actual_due_date):
#     #
#     if payment_date == None or actual_due_date == None or paytype == None:
#         status = None
#     else:

#         if paytype == 'Manual Payer':
#             max_date = actual_due_date - timedelta(days=15)
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
  
def early_status(payment_status, payment_amount_received, remaining_principal):
  
  if payment_status == None or payment_amount_received == None or remaining_principal==None:
    remaining_principal = remaining_principal
  #entire payment goes to principal
  if payment_status == 'Early':
    remaining_principal = F.round(beginning_balance - payment_amount_received)
  else:
    remaining_principal = remaining_principal
  return remaining_principal


# COMMAND ----------

def missed_payment_calc(payment_amount_due, interest, beginning_balance, payment_amount_received,
                       next_payment_date, payment_received_date, unpaid_interest, unpaid_principal):
    interest_paid = 0
    principal_paid = 0
    #initiate for ontime payers going through this cycle
    remaining_principal = beginning_balance
    interest_due = 0
    next_interest_due = 0

    while (payment_amount_received > 0.0) and next_payment_date < payment_received_date:

        if (payment_received_date.year == next_payment_date.year) & (
                payment_received_date.month == next_payment_date.month):
            #     #not missing even though payment received date is a few days later than next_payment_date
            break


        print(next_payment_date)
        interest_due = calc_interest_due(beginning_balance, interest)
        principal_due = calc_principal_amount(payment_amount_due, interest_due)
        after_interest = payment_amount_received - interest_due
        after_principal = after_interest - principal_due


        if after_interest > 0:  # more than interest can cover some principal
            interest_paid += interest_due
            # print(payment_amount_received, "interest due", interest_due, "principal due", principal_due,"interest paid", interest_paid, "after principal", after_principal, after_interest)

            if after_principal >= 0:  # can cover principal
                payment_amount_received -= interest_due + principal_due
                beginning_balance = beginning_balance - principal_due
                next_payment_date = next_payment_date + relativedelta.relativedelta(months=1)
                principal_paid += principal_due
                unpaid_principal = unpaid_principal + 0.0
                unpaid_interest = unpaid_interest + 0.0
                # for next interest calc
                remaining_principal = beginning_balance
                next_interest_due = calc_interest_due(remaining_principal, interest)
                print(next_payment_date, "next interest due", next_interest_due, "beginning bal",
                      beginning_balance, payment_amount_received, "principal paid", principal_paid, "interest paid", interest_paid,
                      payment_amount_received, after_interest, after_principal, unpaid_interest,
                      unpaid_principal, "interest due", interest_due)
            else:  # can't cover principal
                beginning_balance = beginning_balance - after_interest
                principal_paid += after_interest
                unpaid_principal = unpaid_principal + principal_due - after_interest
                unpaid_interest = unpaid_interest + 0.0
                payment_amount_received -= interest_due + after_interest
                # for next interest calc
                remaining_principal = beginning_balance
                next_interest_due = calc_interest_due(remaining_principal, interest)

                print(next_payment_date, "next interest due", next_interest_due, "beginning bal",
                      beginning_balance, payment_amount_received, beginning_balance, "principal paid",
                      principal_paid, "interest paid", interest_paid,
                      "unpaid principal", unpaid_principal, unpaid_interest, "interest due", interest_due)

        else:
            # if payment cannot cover interest
            # elif after_interest < 0: #not enough to pay interest
            interest_paid += payment_amount_received
            principal_paid += 0.0
            unpaid_interest = unpaid_interest + interest_due - interest_paid
            unpaid_principal = unpaid_principal + principal_due
            payment_amount_received -= payment_amount_received
            # for next interest calc
            remaining_principal = beginning_balance - principal_paid
            next_interest_due = unpaid_interest + calc_interest_due(remaining_principal, interest)

            print(next_payment_date, "beginning bal", beginning_balance, remaining_principal, payment_amount_received,
                   "interest paid", interest_paid, principal_paid, "unpaid interest", unpaid_interest, "unpaid principal", unpaid_principal, "interest due", interest_due)

    return next_payment_date, beginning_balance, remaining_principal, payment_amount_received, unpaid_interest,unpaid_principal, interest_paid, principal_paid, next_interest_due, interest_due


  def payment_allocation(paytype, payment_amount_due, interest, beginning_balance, payment_amount_received,
                       next_payment_date, payment_received_date, unpaid_interest, unpaid_principal):
  try:

      # payment_amount_due = calc_monthly_payment()
      if payment_amount_received == 0 or payment_amount_received is None or next_payment_date is None:
          # payment_received_date = date()
          payment_amount_received = 0.0
          remaining_principal = beginning_balance
          interest_due = calc_interest_due(beginning_balance, interest)
          principal_due = calc_principal_amount(payment_amount_due, interest_due)
          interest_paid = 0.0
          principal_paid = 0.0
          unpaid_interest = 0.0
          unpaid_principal = 0.0
          next_interest_due = calc_interest_due(beginning_balance - principal_due, interest)
          next_payment_date = next_payment_date
          overpay = 0.0
#           payment_status = None
#           out = (round(remaining_principal,2), interest_due, principal_due, interest_paid, principal_paid, unpaid_interest,
#                  unpaid_principal, next_interest_due, next_payment_date)
          print("not valid entry", interest_paid, principal_paid)
      else:


          #if next_payment date and payment received date in same month
          #next_payment_date missed payment
          next_payment_date, beginning_balance, remaining_principal, payment_amount_received, unpaid_interest,unpaid_principal, interest_paid, principal_paid, next_interest_due, interest_due\
              = missed_payment_calc(payment_amount_due, interest, beginning_balance, payment_amount_received, \
                              next_payment_date, payment_received_date, unpaid_interest, unpaid_principal)

          # print(payment_amount_received, "beginning bal", beginning_balance, "interest due", interest_due, "principal due", principal_due, "interest paid", interest_paid, "principal paid", principal_paid, "unpaid principal",
          #           unpaid_principal, "unpaid interest", unpaid_interest, "next interest due", next_interest_due, next_payment_date)
          overpay = 0.0
          if payment_amount_received > 0:
              payment_status = calculate_payment_status(paytype, payment_received_date, next_payment_date)
              # if in current period, no more late payments
              if (next_payment_date.month == payment_received_date.month) & (
                      next_payment_date.year == payment_received_date.year) & (payment_amount_received > 0) \
                      & (payment_status == 'On Time' or payment_status == 'Late'):
                  # while (next_payment_date.month == payment_received_date.month) & (
                  #             next_payment_date.year == payment_received_date.year) & (payment_amount_received > 0):

                  # if current payment is on time

                  #             if payment_status == 'On Time':
                  interest_due = calc_interest_due(beginning_balance, interest)
                  principal_due = calc_principal_amount(payment_amount_due, interest_due)
                  after_interest = payment_amount_received - interest_due
                  # can you cover principal due

                  after_principal = after_interest - principal_due

                  # more than enough to cover interest
                  if after_interest > 0:
                      after_interest = payment_amount_received - interest_due

                      # can you cover principal due
                      # after_principal = after_interest - principal_due
                      if (after_principal > 0):  # overpayment next_payment_date still same

                          interest_paid += interest_due
                          remaining_principal = beginning_balance - principal_due - after_principal
                          next_payment_date = next_payment_date + relativedelta.relativedelta(months=1)
                          next_interest_due = calc_interest_due(remaining_principal, interest)
                          # remaining_principal = remaining_principal - after_principal
                          principal_paid += principal_due + after_principal
                          payment_amount_received -= principal_paid + interest_paid
                          unpaid_interest += 0.0
                          unpaid_principal += 0.0
                          overpay += after_principal
                          print("on time overpay")
                      # either you can cover full payment, or not enough to cover principal
                      else:
                          if payment_amount_received == payment_amount_due:
                              remaining_principal = beginning_balance - principal_due
                              interest_paid += interest_due
                              # calc updated next interest due since bill is paid
                              next_interest_due = calc_interest_due(remaining_principal, interest)
                              principal_paid += principal_due
                              unpaid_interest += 0.0
                              unpaid_principal += 0.0
                              next_payment_date = next_payment_date + relativedelta.relativedelta(months=1)
                              overpay = 0.0
                              print("amount covers exactly payment due")
                          # not enough to cover principal
                          else:

                              remaining_principal = beginning_balance - after_interest
                              interest_paid += interest_due
                              next_interest_due = calc_interest_due(remaining_principal, interest)
                              principal_paid += after_interest
                              unpaid_interest += 0.0
                              unpaid_principal += principal_due - after_interest
                              overpay = 0.0
                  # payment not enough to cover interest
                  else:
                      next_payment_date = next_payment_date
                      interest_paid = payment_amount_received
                      principal_paid = 0.0
                      unpaid_interest = interest_due - payment_amount_received
                      unpaid_principal = principal_due
                      remaining_principal = beginning_balance
                      # if current unpaid interest, need to project next interest due, add up current unpaid interest + calculate next interest due using beginning balance? but that didn't change next
                      next_interest_due = unpaid_interest + calc_interest_due(remaining_principal, interest)
                      overpay = 0.0

                      print("not enough to cover interest")
                      # out = (round(remaining_principal, 2), interest_due, principal_due, interest_paid, principal_paid,
                      #        unpaid_interest,
                      #        unpaid_principal, next_interest_due, next_payment_date)
              # #paid missing payment but not enough
              # elif payment_amount_received == 0.0:
              #     interest_due = interest_due
              #     remaining_principal = remaining_principal
              #     principal_paid = principal_paid
              #     interest_paid = interest_due
              #     unpaid_interest = unpaid_interest
              #     unpaid_principal = unpaid_principal
              #     next_interest_due = next_interest_due

              #early payers
              else:
                  interest_due = calc_interest_due(beginning_balance, interest)
                  principal_due = calc_principal_amount(payment_amount_due, interest_due)
                  remaining_principal = beginning_balance - payment_amount_received
                  interest_paid = 0.0
                  principal_paid = payment_amount_received
                  unpaid_interest = interest_due
                  unpaid_principal = principal_due
                  next_payment_date = next_payment_date
                  next_interest_due = calc_interest_due(remaining_principal, interest)
                  overpay = 0.0
                  print("early pay")
                  # out = (round(remaining_principal,2), interest_due, principal_due, interest_paid, principal_paid, unpaid_interest,
                  #    unpaid_principal, next_interest_due, next_payment_date)

          # payment_amount_received == 0, and they either only had enough to pay missed payments but not current OR  could not fully pay up to current(partial payment)
          else:
              principal_due = calc_principal_amount(payment_amount_due, interest_due)

      out = round(remaining_principal,2), interest_due, principal_due, interest_paid, principal_paid, unpaid_interest,\
      unpaid_principal, next_interest_due, next_payment_date, overpay

  except:
      remaining_principal = -1.0
      interest_due = -1.0
      principal_due = -1.0
      interest_paid = -1.0
      principal_paid = -1.0
      unpaid_interest = -1.0
      unpaid_principal = -1.0
      next_interest_due = -1.0
      overpay = -1.0
      next_payment_date = None

      out = (round(remaining_principal,2), interest_due, principal_due,  interest_paid, principal_paid, unpaid_interest,
             unpaid_principal, next_interest_due, next_payment_date, overpay)

      print(payment_amount_received,"beginning bal", beginning_balance, "remaining principal", remaining_principal, "interest due", interest_due, "principal due", principal_due, "interest paid", interest_paid, "principal paid",
      principal_paid, "unpaid principal",unpaid_principal,"unpaid interest",unpaid_interest, "next interest due", next_interest_due, next_payment_date)
  # remaining_principal, interest_due, principal_due, interest_paid, principal_paid, unpaid_interest,unpaid_principal,next_interest_due,next_payment_date
  return out





# COMMAND ----------

# #3/25/21 working

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
#                       principal_paid = principal_due + after_principal
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

# COMMAND ----------

#1st version

# def payment_allocation(paytype, payment_amount_due, interest, beginning_balance, payment_amount_received, next_payment_date, payment_received_date,unpaid_interest,unpaid_principal):
#   try:
# #     remaining_principal = -1.0
# #     interest_due = -1.0
# #     principal_due = -1.0
# #     interest_paid = -1.0
# #     principal_paid = -1.0
# #     unpaid_interest = -1.0
# #     unpaid_principal = -1.0
# #     next_interest_due = -1.0
# #     next_payment_date = None
    
#     # payment_amount_due = calc_monthly_payment()
#     if payment_amount_received == 0 or next_payment_date is None:
#           # payment_received_date = date()
#           payment_amount_received = 0.0
#           remaining_principal = beginning_balance
#           interest_due = calc_interest_due(beginning_balance, interest)
#           principal_due = payment_amount_due - interest_due
#           interest_paid = 0.0
#           principal_paid = 0.0
#           unpaid_interest = 0.0
#           unpaid_principal = 0.0
#           next_interest_due = calc_interest_due(beginning_balance - principal_due, interest)
#           next_payment_date = next_payment_date
#     else:

#         #checking for  missed payments
#         while (payment_amount_received > 0.0) and (next_payment_date < payment_received_date):
#             print(next_payment_date)
#             interest_due = calc_interest_due(beginning_balance, interest)
#             principal_due = calc_principal_amount(payment_amount_due, interest_due)
#             after_interest = payment_amount_received - interest_due
#             after_principal = after_interest - principal_due


#             if after_interest > 0:  # more than interest can cover some principal
#                 interest_paid = interest_due
#                 # print(payment_amount_received, "interest due", interest_due, "principal due", principal_due,"interest paid", interest_paid, "after principal", after_principal, after_interest)

#                 if after_principal >= 0:  # can cover principal
#                     payment_amount_received -= interest_due + principal_due
#                     beginning_balance = beginning_balance - principal_due
#                     next_payment_date = next_payment_date + relativedelta.relativedelta(months=1)
#                     principal_paid = principal_due
#                     unpaid_principal = unpaid_principal + 0.0
#                     unpaid_interest = unpaid_interest + 0.0
#                     # for next interest calc
#                     remaining_principal = beginning_balance
#                     next_interest_due = calc_interest_due(remaining_principal, interest)
#                     print(next_payment_date, "next interest due", next_interest_due, "beginning bal", beginning_balance, "principal paid", principal_paid, "interest paid", interest_paid, payment_amount_received, after_interest, after_principal, unpaid_interest,
#                           unpaid_principal)
#                 else:  # can't cover principal
#                     beginning_balance = beginning_balance - after_interest
#                     principal_paid = after_interest
#                     unpaid_principal = unpaid_principal + principal_due - after_interest
#                     unpaid_interest = unpaid_interest + 0.0
#                     payment_amount_received -= interest_due + after_interest
#                     # for next interest calc
#                     remaining_principal = beginning_balance
#                     next_interest_due = calc_interest_due(remaining_principal, interest)
#                     print(next_payment_date, "next interest due", next_interest_due,"beginning bal", beginning_balance, payment_amount_received, beginning_balance, "principal paid", principal_paid, interest_paid,
#                           "unpaid principal", unpaid_principal, unpaid_interest)

#             else:
#                 # elif after_interest < 0: #not enough to pay interest
#                 interest_paid = payment_amount_received
#                 principal_paid = 0.0
#                 unpaid_interest = unpaid_interest + interest_due - interest_paid
#                 unpaid_principal = unpaid_principal + principal_due
#                 payment_amount_received -= payment_amount_received
#                 # for next interest calc
#                 remaining_principal = beginning_balance - principal_paid
#                 next_interest_due = unpaid_interest + calc_interest_due(remaining_principal, interest)
#                 print(next_payment_date, "beginning bal", beginning_balance, payment_amount_received, after_interest, interest_paid)

#         # print(payment_amount_received, "beginning bal", beginning_balance, "interest due", interest_due, "principal due", principal_due, "interest paid", interest_paid, "principal paid", principal_paid, "unpaid principal",
#         #           unpaid_principal, "unpaid interest", unpaid_interest, "next interest due", next_interest_due, next_payment_date)

#         payment_status = calculate_payment_status(paytype, payment_received_date, next_payment_date)
#         #if in current period, no more late payments
#         if (next_payment_date.month == payment_received_date.month) & (next_payment_date.year == payment_received_date.year) & (payment_amount_received > 0) \
#         & (payment_status == 'On Time'):
#         # while (next_payment_date.month == payment_received_date.month) & (
#         #             next_payment_date.year == payment_received_date.year) & (payment_amount_received > 0):

#             # if current payment is on time

# #             if payment_status == 'On Time':
#             interest_due = calc_interest_due(beginning_balance, interest)
#             principal_due = calc_principal_amount(payment_amount_due, interest_due)
#             after_interest = payment_amount_received - interest_due
#             # can you cover principal due

#             after_principal = after_interest - principal_due

#             #more than enough to cover interest
#             if after_interest > 0:
#                 after_interest = payment_amount_received - interest_due

#                 # can you cover principal due
#                 # after_principal = after_interest - principal_due
#                 if (after_principal > 0):  # overpayment next_payment_date still same

#                     interest_paid = interest_due
#                     remaining_principal = beginning_balance - principal_due - after_principal
#                     next_payment_date = next_payment_date + relativedelta.relativedelta(months=1)
#                     next_interest_due = calc_interest_due(remaining_principal, interest)
#                     # remaining_principal = remaining_principal - after_principal
#                     principal_paid = principal_due
#                     payment_amount_received -= principal_paid + interest_paid
#                     unpaid_interest = 0.0
#                     unpaid_principal = 0.0

#                #either you can cover full payment, or not enough to cover principal
#                 else:
#                     if payment_amount_received == payment_amount_due:
#                         remaining_principal = beginning_balance - principal_due
#                         interest_paid = interest_due
#                         # calc updated next interest due since bill is paid
#                         next_interest_due = calc_interest_due(remaining_principal, interest)
#                         principal_paid = principal_due
#                         unpaid_interest = 0.0
#                         unpaid_principal = 0.0
#                         next_payment_date = next_payment_date + relativedelta.relativedelta(months=1)

#                     # not enough to cover principal
#                     else:

#                         remaining_principal = beginning_balance - after_interest
#                         interest_paid = interest_due
#                         next_interest_due = calc_interest_due(remaining_principal, interest)
#                         principal_paid = after_interest
#                         unpaid_interest = 0.0
#                         unpaid_principal = principal_due - after_interest

#              #payment not enough to cover interest
#             else:
#                 next_payment_date = next_payment_date
#                 interest_paid = payment_amount_received
#                 principal_paid = 0.0
#                 unpaid_interest = interest_due - payment_amount_received
#                 unpaid_principal = principal_due
#                 remaining_principal = beginning_balance
#                 #if current unpaid interest, need to project next interest due, add up current unpaid interest + calculate next interest due using beginning balance? but that didn't change next
#                 next_interest_due = unpaid_interest + calc_interest_due(remaining_principal, interest)
      
#         else:
#           interest_due = calc_interest_due(beginning_balance, interest)
#           principal_due = calc_principal_amount(payment_amount_due, interest_due)
#           remaining_principal = beginning_balance - payment_amount_received
#           interest_paid = 0
#           principal_paid = 0
#           unpaid_interest = interest_due
#           unpaid_principal = principal_due
#           next_payment_date = next_payment_date
#           next_interest_due = calc_interest_due(remaining_principal, interest)
        
                    
#     out = (remaining_principal, interest_due, principal_due, interest_paid, principal_paid, unpaid_interest,unpaid_principal,next_interest_due,next_payment_date)
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
      
#       out = (remaining_principal, interest_due, principal_due, interest_paid, principal_paid, unpaid_interest,unpaid_principal,next_interest_due,next_payment_date)
      
# #     print(payment_amount_received,"beginning bal", beginning_balance, "remaining principal", remaining_principal, "interest due", interest_due, "principal due", principal_due, "interest paid", interest_paid, "principal paid", 
# #     principal_paid, "unpaid principal",unpaid_principal,"unpaid interest",unpaid_interest, "next interest due", next_interest_due, next_payment_date)
# # remaining_principal, interest_due, principal_due, interest_paid, principal_paid, unpaid_interest,unpaid_principal,next_interest_due,next_payment_date
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


