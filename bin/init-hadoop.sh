#!/bin/sh
hadoop fs -mkdir -p /ml/uinfo
hadoop fs -mkdir -p /ml/actions
hadoop fs -mkdir -p /ml/df
hadoop fs -mkdir -p /ml/payment
hadoop fs -put user_008.txt /ml/uinfo/user.txt
hadoop fs -put action0_008.txt /ml/actions/action0.txt
hadoop fs -put payment.txt /ml/actions/pay.txt
