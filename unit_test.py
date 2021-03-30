# Databricks notebook source
# from interest_due_cases import payment_allocation,calculate_payment_status
from loantape_lib_v2 import payment_allocation
# from troubleshoot import payment_allocation

from datetime import date, timedelta
from dateutil import relativedelta


def on_time_pay():

# def on_time_pay(payment_amount_due,next_payment_date,payment_received_date,paytype,interest,beginning_balance, payment_amount_received,unpaid_interest,unpaid_principal):
    payment_amount_due = 500
    # interest_due= 300
    # remaining_principal = 1000
    # unpaid_interest = 0.0
    # unpaid_principal = 0.0
    # interest_paid = 0.0
    # principal_paid = 0.0
    # principal_due = 200
    # next_payment_date = date(2020, 10, 6)
    # payment_received_date = date(2020, 12, 6)
    next_payment_date = date(2020, 12, 6)
    payment_received_date = date(2020, 12, 6)
    paytype = 'Auto Payer'

    interest = 10
    beginning_balance = 10000
    payment_amount_received = 500
    # if (next_payment_date.month < payment_received_date.month) & (next_payment_date.year < payment_received_date.year):

    unpaid_interest = 0.0
    unpaid_principal = 0.0

    remaining_principal, interest_due, principal_due, interest_paid, principal_paid, unpaid_interest, unpaid_principal, next_interest_due, next_payment_date, overpay = payment_allocation(paytype, payment_amount_due, interest, beginning_balance, payment_amount_received, next_payment_date, payment_received_date,unpaid_interest,unpaid_principal)
    # assert (round(remaining_principal, 2) == round(beginning_balance - principal_paid,2))
    print("on time pay", interest_paid,principal_paid, overpay)
    assert round(remaining_principal, 2) == round(9583.333, 2), "Remaining principal doesn't match"
    assert round(interest_due,2) == round(83.3333,2),"Interest_due doesn't match"
    assert round(interest_paid,2) == round(83.333,2), "Interest paid does not match"
    assert round(principal_paid, 2) == round(416.6667, 2), "Interest paid does not match"
    assert next_payment_date == date(2021, 1, 6)
    assert overpay == 0
    return remaining_principal,beginning_balance, principal_paid

on_time_pay()

def on_time_pay_partial():

# def on_time_pay(payment_amount_due,next_payment_date,payment_received_date,paytype,interest,beginning_balance, payment_amount_received,unpaid_interest,unpaid_principal):
    payment_amount_due = 500
    # interest_due= 300
    # remaining_principal = 1000
    # unpaid_interest = 0.0
    # unpaid_principal = 0.0
    # interest_paid = 0.0
    # principal_paid = 0.0
    # principal_due = 200
    # next_payment_date = date(2020, 10, 6)
    # payment_received_date = date(2020, 12, 6)
    next_payment_date = date(2020, 12, 6)
    payment_received_date = date(2020, 12, 6)
    paytype = 'Auto Payer'

    interest = 10
    beginning_balance = 10000
    payment_amount_received = 40
    # if (next_payment_date.month < payment_received_date.month) & (next_payment_date.year < payment_received_date.year):

    unpaid_interest = 0.0
    unpaid_principal = 0.0

    remaining_principal, interest_due, principal_due, interest_paid, principal_paid, unpaid_interest, unpaid_principal, next_interest_due, next_payment_date, overpay = payment_allocation(paytype, payment_amount_due, interest, beginning_balance, payment_amount_received, next_payment_date, payment_received_date,unpaid_interest,unpaid_principal)
    # assert (round(remaining_principal, 2) == round(beginning_balance - principal_paid,2))

    print("on time pay partial", interest_paid,principal_paid, overpay)
    assert round(remaining_principal, 2) == round(10000, 2), "Remaining principal doesn't match"
    assert round(interest_due,2) == round(83.3333,2),"Interest_due doesn't match"
    assert round(interest_paid,2) == round(40,2), "Interest paid does not match"
    assert round(principal_paid, 2) == round(0, 2), "Interest paid does not match"
    assert overpay == 0
    assert next_payment_date == date(2020, 12, 6)

    return remaining_principal,beginning_balance, principal_paid

on_time_pay_partial()

def late_same_month():

# def on_time_pay(payment_amount_due,next_payment_date,payment_received_date,paytype,interest,beginning_balance, payment_amount_received,unpaid_interest,unpaid_principal):
    payment_amount_due = 705.33
    # interest_due= 300
    # remaining_principal = 1000
    # unpaid_interest = 0.0
    # unpaid_principal = 0.0
    # interest_paid = 0.0
    # principal_paid = 0.0
    # principal_due = 200
    # next_payment_date = date(2020, 10, 6)
    # payment_received_date = date(2020, 12, 6)
    next_payment_date = date(2020, 6, 20)
    payment_received_date = date(2020, 6, 22)
    paytype = 'Auto Payer'

    interest = 11.57
    beginning_balance = 27000
    payment_amount_received = 705.33
    # if (next_payment_date.month < payment_received_date.month) & (next_payment_date.year < payment_received_date.year):

    unpaid_interest = 0.0
    unpaid_principal = 0.0

    remaining_principal, interest_due, principal_due, interest_paid, principal_paid, unpaid_interest, unpaid_principal, next_interest_due, next_payment_date = payment_allocation(paytype, payment_amount_due, interest, beginning_balance, payment_amount_received, next_payment_date, payment_received_date,unpaid_interest,unpaid_principal)
    print(next_payment_date,remaining_principal)
    # assert (round(remaining_principal, 2) == round(beginning_balance - principal_paid,2))
    assert round(remaining_principal, 2) == round(26554.995, 2), "Remaining principal doesn't match {}".format(remaining_principal)
    assert round(interest_due, 2) == round(260.325, 2), "Interest_due doesn't match"

    assert next_payment_date == date(2020, 7, 20)

    return remaining_principal,beginning_balance, principal_paid


# late_same_month()

def on_time_overpay():

# def on_time_pay(payment_amount_due,next_payment_date,payment_received_date,paytype,interest,beginning_balance, payment_amount_received,unpaid_interest,unpaid_principal):
    payment_amount_due = 500
    # interest_due= 300
    # remaining_principal = 1000
    # unpaid_interest = 0.0
    # unpaid_principal = 0.0
    # interest_paid = 0.0
    # principal_paid = 0.0
    # principal_due = 200
    # next_payment_date = date(2020, 10, 6)
    # payment_received_date = date(2020, 12, 6)
    next_payment_date = date(2020, 12, 6)
    payment_received_date = date(2020, 12, 6)
    paytype = 'Auto Payer'

    interest = 10
    beginning_balance = 10000
    payment_amount_received = 700
    # if (next_payment_date.month < payment_received_date.month) & (next_payment_date.year < payment_received_date.year):

    unpaid_interest = 0.0
    unpaid_principal = 0.0

    remaining_principal, interest_due, principal_due, interest_paid, principal_paid, unpaid_interest, unpaid_principal, next_interest_due, next_payment_date, overpay = payment_allocation(paytype, payment_amount_due, interest, beginning_balance, payment_amount_received, next_payment_date, payment_received_date,unpaid_interest,unpaid_principal)
    print("on time overpay", overpay, remaining_principal)
    # assert (round(remaining_principal, 2) == round(beginning_balance - principal_paid,2))
    assert round(remaining_principal, 2) == round(9383.333, 2), "Remaining principal doesn't match"
    assert next_payment_date == date(2021,1,6)
    assert overpay == 200
    return remaining_principal,interest_due, principal_paid

on_time_overpay()

def late_missed_2payments():
#pay 3 payments
# def on_time_pay(payment_amount_due,next_payment_date,payment_received_date,paytype,interest,beginning_balance, payment_amount_received,unpaid_interest,unpaid_principal):
    payment_amount_due = 500
    # interest_due= 300
    # remaining_principal = 1000
    # unpaid_interest = 0.0
    # unpaid_principal = 0.0
    # interest_paid = 0.0
    # principal_paid = 0.0
    # principal_due = 200
    # next_payment_date = date(2020, 10, 6)
    # payment_received_date = date(2020, 12, 6)
    next_payment_date = date(2020, 10, 6)
    payment_received_date = date(2020, 12, 6)
    paytype = 'Auto Payer'

    interest = 10
    beginning_balance = 10000
    payment_amount_received = 1500
    # if (next_payment_date.month < payment_received_date.month) & (next_payment_date.year < payment_received_date.year):

    unpaid_interest = 0.0
    unpaid_principal = 0.0

    remaining_principal, interest_due, principal_due, interest_paid, principal_paid, unpaid_interest, unpaid_principal, next_interest_due, next_payment_date, overpay = payment_allocation(paytype, payment_amount_due, interest, beginning_balance, payment_amount_received, next_payment_date, payment_received_date,unpaid_interest,unpaid_principal)
    # assert (round(remaining_principal, 2) == round(beginning_balance - principal_paid,2))

    print(remaining_principal, interest_paid, principal_paid)
    assert round(interest_paid,2) == round(239.554,2), "Interest paid does not match"
    assert round(principal_paid,2) == round(1260.4456,2), "principal paid does not match"
    assert round(remaining_principal, 2) == round(8739.551, 2), "Remaining principal doesn't match"
    assert next_payment_date == date(2021,1,6)
    return remaining_principal,interest_due, principal_paid,next_payment_date

# late_missed_2payments()



def late_missed_2payments_overpay():

# def on_time_pay(payment_amount_due,next_payment_date,payment_received_date,paytype,interest,beginning_balance, payment_amount_received,unpaid_interest,unpaid_principal):
    payment_amount_due = 500
    # interest_due= 300
    # remaining_principal = 1000
    # unpaid_interest = 0.0
    # unpaid_principal = 0.0
    # interest_paid = 0.0
    # principal_paid = 0.0
    # principal_due = 200
    # next_payment_date = date(2020, 10, 6)
    # payment_received_date = date(2020, 12, 6)
    next_payment_date = date(2020, 10, 6)
    payment_received_date = date(2020, 12, 6)
    paytype = 'Auto Payer'

    interest = 10
    beginning_balance = 10000
    payment_amount_received = 1700
    # if (next_payment_date.month < payment_received_date.month) & (next_payment_date.year < payment_received_date.year):

    unpaid_interest = 0.0
    unpaid_principal = 0.0

    remaining_principal, interest_due, principal_due, interest_paid, principal_paid, unpaid_interest, unpaid_principal, next_interest_due, next_payment_date, overpay = payment_allocation(paytype, payment_amount_due, interest, beginning_balance, payment_amount_received, next_payment_date, payment_received_date,unpaid_interest,unpaid_principal)
    # assert (round(remaining_principal, 2) == round(beginning_balance - principal_paid,2))
    print("late_missed_2payments_overpay", "interest paid", interest_paid, remaining_principal, "principal paid", principal_paid, overpay)
    assert round(remaining_principal, 2) == round(8539.551, 2), "Remaining principal doesn't match {}".format(remaining_principal)
    assert next_payment_date == date(2021,1,6), "date does not match"
    assert interest_paid == 239.5544
    assert principal_paid == 1460.4456
    assert overpay == 200
    return remaining_principal,interest_due, principal_paid

late_missed_2payments_overpay()
#
# payment_amount_due = 500
# # interest_due= 300
# # remaining_principal = 1000
# # unpaid_interest = 0.0
# # unpaid_principal = 0.0
# # interest_paid = 0.0
# # principal_paid = 0.0
# # principal_due = 200
# # next_payment_date = date(2020, 10, 6)
# # payment_received_date = date(2020, 12, 6)
# next_payment_date = date(2020, 10, 6)
# payment_received_date = None
# paytype = 'Auto Payer'
#
# interest = 10
# beginning_balance = 10000
# payment_amount_received = 0
# # if (next_payment_date.month < payment_received_date.month) & (next_payment_date.year < payment_received_date.year):
#
# unpaid_interest = 0.0
# unpaid_principal = 0.0
#
# payment_allocation(paytype, payment_amount_due, interest, beginning_balance, payment_amount_received, next_payment_date, payment_received_date,unpaid_interest,unpaid_principal)
#
def late_one_month():

# def on_time_pay(payment_amount_due,next_payment_date,payment_received_date,paytype,interest,beginning_balance, payment_amount_received,unpaid_interest,unpaid_principal):
    payment_amount_due = 259.47
    # interest_due= 300
    # remaining_principal = 1000
    # unpaid_interest = 0.0
    # unpaid_principal = 0.0
    # interest_paid = 0.0
    # principal_paid = 0.0
    # principal_due = 200
    # next_payment_date = date(2020, 10, 6)
    # payment_received_date = date(2020, 12, 6)
    next_payment_date = date(2020, 5, 26)
    payment_received_date = date(2020, 6, 23)

    # next_payment_date = date(2020, 6, 20)
    # payment_received_date = date(2020, 6, 22)
    paytype = 'Manual Payer'

    interest = 15.81
    beginning_balance = 7020.8
    payment_amount_received = 259.47
    # if (next_payment_date.month < payment_received_date.month) & (next_payment_date.year < payment_received_date.year):

    unpaid_interest = 0.0
    unpaid_principal = 0.0

    remaining_principal, interest_due, principal_due, interest_paid, principal_paid, unpaid_interest, unpaid_principal, next_interest_due, next_payment_date, overpay = payment_allocation(paytype, payment_amount_due, interest, beginning_balance, payment_amount_received, next_payment_date, payment_received_date,unpaid_interest,unpaid_principal)
    print("late one month", next_payment_date,remaining_principal,interest_due)
    assert round(remaining_principal, 2) == round(6853.8290400000005, 2), "Remaining principal doesn't match"
    assert round(interest_due, 2) == round(92.4990, 2), "Interest_due doesn't match"
    #
    assert next_payment_date == date(2020, 6, 26)

    return remaining_principal,beginning_balance, principal_paid


# late_one_month()

def early_payer_ver1():
    # Next Payment date in current month but payment received was few days early
    payment_amount_due = 500
    # interest_due= 300
    # remaining_principal = 1000
    # unpaid_interest = 0.0
    # unpaid_principal = 0.0
    # interest_paid = 0.0
    # principal_paid = 0.0
    # principal_due = 200
    # next_payment_date = date(2020, 10, 6)
    # payment_received_date = date(2020, 12, 6)
    next_payment_date = date(2020, 11, 6)
    payment_received_date = date(2020, 11, 2)
    paytype = 'Auto Payer'

    interest = 10
    beginning_balance = 10000
    payment_amount_received = 500
    # if (next_payment_date.month < payment_received_date.month) & (next_payment_date.year < payment_received_date.year):

    unpaid_interest = 0.0
    unpaid_principal = 0.0

    remaining_principal, interest_due, principal_due, interest_paid, principal_paid, unpaid_interest, unpaid_principal, next_interest_due, next_payment_date, overpay = payment_allocation(
        paytype, payment_amount_due, interest, beginning_balance, payment_amount_received, next_payment_date,
        payment_received_date, unpaid_interest, unpaid_principal)
    print("interest_paid",interest_paid,"principal_paid",principal_paid, "remaining principal", remaining_principal)
    assert (round(remaining_principal, 2) == round(beginning_balance - payment_amount_received,2))
    return remaining_principal,beginning_balance, principal_paid

# early_payer_ver1()

def early_payer_ver2():
    #Next Payment Date is already in the future month, and payment received date is in current month
    payment_amount_due = 500
    # interest_due= 300
    # remaining_principal = 1000
    # unpaid_interest = 0.0
    # unpaid_principal = 0.0
    # interest_paid = 0.0
    # principal_paid = 0.0
    # principal_due = 200
    # next_payment_date = date(2020, 10, 6)
    # payment_received_date = date(2020, 12, 6)
    next_payment_date = date(2020, 12, 6)
    payment_received_date = date(2020, 11, 2)
    paytype = 'Auto Payer'
    interest = 10
    beginning_balance = 10000
    # remaining_principal = beginning_balance
    payment_amount_received = 500
    # if (next_payment_date.month < payment_received_date.month) & (next_payment_date.year < payment_received_date.year):

    unpaid_interest = 0.0
    unpaid_principal = 0.0

    remaining_principal, interest_due, principal_due, interest_paid, principal_paid, unpaid_interest, unpaid_principal, \
    next_interest_due, next_payment_date, overpay = payment_allocation(paytype, payment_amount_due, interest, beginning_balance, payment_amount_received, next_payment_date, payment_received_date, unpaid_interest, unpaid_principal)
    print(remaining_principal)

    assert round(remaining_principal, 2) == round(beginning_balance - payment_amount_received,2)
    return remaining_principal, beginning_balance, principal_paid

def late_makes_partial_pay():
    payment_amount_due = 500
    # interest_due= 300
    # remaining_principal = 1000
    # unpaid_interest = 0.0
    # unpaid_principal = 0.0
    # interest_paid = 0.0
    # principal_paid = 0.0
    # principal_due = 200
    # next_payment_date = date(2020, 10, 6)
    # payment_received_date = date(2020, 12, 6)
    next_payment_date = date(2020, 10, 6)
    payment_received_date = date(2020, 12, 6)
    paytype = 'Auto Payer'

    interest = 10
    beginning_balance = 10000
    payment_amount_received = 700
    # if (next_payment_date.month < payment_received_date.month) & (next_payment_date.year < payment_received_date.year):

    unpaid_interest = 0
    unpaid_principal = 0
    remaining_principal, interest_due, principal_due, interest_paid, principal_paid, unpaid_interest, unpaid_principal, next_interest_due, next_payment_date, overpay = payment_allocation(
        paytype, payment_amount_due, interest, beginning_balance, payment_amount_received, next_payment_date,
        payment_received_date, unpaid_interest, unpaid_principal)

    assert round(interest_paid,2) == 163.19, "Interest paid does not match"
    assert round(principal_paid,2)  == 536.81, "Principal paid does not match"
    assert round(remaining_principal, 2) == round(9463.1944,2)

    return remaining_principal, interest_paid, principal_paid
# def schedule_auto_ontime_test():
#     paytype = 'Auto Payer'
#     next_payment_date = date(2020,12,10)
#     payment_received_date = date(2020,12,8)
#
#     # status = current_payment_status(paytype, payment_received_date, next_payment_date)
#     status = calculate_payment_status(paytype, payment_received_date, next_payment_date)
#
#     print(status)
#     assert(status == 'On Time')
#
# schedule_auto_ontime_test()
#
# def schedule_late_test():
#     paytype = 'Auto Payer'
#     next_payment_date = date(2020,12,10)
#     payment_received_date = date(2020,12,11)
#
#     # status = current_payment_status(paytype, payment_received_date, next_payment_date)
#     status = calculate_payment_status(paytype, payment_received_date, next_payment_date)
#
#     print(status)
#     assert(status == 'Late')
#
# schedule_late_test()
#
# def schedule_early_test():
#     paytype = 'Auto Payer'
#     next_payment_date = date(2020,12,10)
#     payment_received_date = date(2020,12,6)
#
#     # status = current_payment_status(paytype, payment_received_date, next_payment_date)
#     status = calculate_payment_status(paytype, payment_received_date, next_payment_date)
#
#     print(status)
#     assert(status == 'Early')
#
# schedule_early_test()
#
# def schedule_manual_ontime_test():
#     paytype = 'Manual Payer'
#     #if
#     next_payment_date = date(2020,12,10)
#     # payment_received_date = date(2020,11,30)
#     payment_received_date = date(2020,12,1)
#
#     # status = current_payment_status(paytype, payment_received_date, next_payment_date)
#     status = calculate_payment_status(paytype, payment_received_date, next_payment_date)
#
#     print(status)
#     assert(status == 'On Time')
#
# schedule_manual_ontime_test()
#
# def schedule_auto_late():
#     paytype = 'Auto Payer'
#     #if
#     next_payment_date = date(2020,10,10)
#     # payment_received_date = date(2020,11,30)
#     payment_received_date = date(2020,12,1)
#
#     # status = current_payment_status(paytype, payment_received_date, next_payment_date)
#     status = calculate_payment_status(paytype, payment_received_date, next_payment_date)
#
#     print(status)
#     assert(status == 'Late')
#
# schedule_auto_late()
#
# def schedule_auto_ontime():
#     paytype = 'Auto Payer'
#     #if
#     next_payment_date = date(2020,10,10)
#     # payment_received_date = date(2020,11,30)
#     payment_received_date = date(2020,10,8)
#
#     # status = current_payment_status(paytype, payment_received_date, next_payment_date)
#     status = calculate_payment_status(paytype, payment_received_date, next_payment_date)
#
#     print(status)
#     assert(status == 'On Time')
#
# schedule_auto_ontime()
#
#
# def schedule_auto_ontime():
#     paytype = 'Auto Payer'
#     #if
#     next_payment_date = date(2020,10,10)
#     # payment_received_date = date(2020,11,30)
#     payment_received_date = date(2020,10,8)
#
#     # status = current_payment_status(paytype, payment_received_date, next_payment_date)
#     status = calculate_payment_status(paytype, payment_received_date, next_payment_date)
#
#     print(status)
#     assert(status == 'On Time')
#
# schedule_auto_ontime()




