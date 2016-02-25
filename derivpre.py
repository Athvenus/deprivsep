#-*-coding:utf-8-*-
from pyspark import SparkContext
import os
import sys


def judge_region(mi,dts,mu_mi,delta_mi,u_dts,delta_dts):
    r1,r2='0','0'
    if dts > delta_dts:
        r1='A'
    if 0 < dts <= delta_dts:
        r1='B'
    if -delta_dts < dts <=0:
        r1='C'
    if dts <= -delta_dts:
        r1='D'
    if mi > mu_mi+delta_mi:
        r2='a'
    if mu_mi < mi < mu_mi+delta_mi:
        r2='b'
    if mu_mi-delta_mi < mi <= mu_mi:
        r2='c'
    if mi <= mu_mi-delta_mi:
        r2='d'
    return r1+r2


def judge_first_deriv(dts,hdts,ddts):
    return 0


def judge_second_deriv(dts,hdts,ddts):
    return 0


def load_model():
    return 0


def load_data():
    return 0
  

def preidct(mi,dts,hdts,ddts,dmaxr,dmaxl,dminr,dminl):
    region=judge_region(mi,dts)
    deside=''
    if region in ['aA','aB','aC','aD','bA']:
        deside='bound'
    else:
        if region in ['dA','dB','dC','dD','cD']:
            deside='separate'
        else:
            if region in ['bC','cA']:
                if hdts > delta_1:
                    deside='bound'
                else:
                    if ddts > xi_1:
                        deside='separate'         
                    else:
                        deside='?'       
                else:
                    if region in ['cB','bD']:
                        if hdts > delta_2:
                            deside='bound'
                        else:
                            if ddts > xi_2:
                                deside='separate'
                            else:
                                deside='?'
                        else:
                            if region=='Cc':
                                if hdts > delta_3:
                                    deside='bound'
                                else:
                                    if ddts > xi_3:
                                        deside='separate'
                                    else:
                                        deside='?'
                            else:
                                if region=='Bb': 
                                    if hdts > 0:
                                        deside='bound'
                                    else:
                                        if ddts > xi_3:
                                            deside='separate'
                                        else:
                                            deside='?'
    if deside=='':
        if dmaxr > 0 or dminr > 0:
            deside='->'
        else:
            if dmaxl > 0 or dminl > 0:
                deside='<-'
            else:
                deside='?'            
    return deside


if __name__=='__main__':
    #Judging Parameters
    global delta_1,delta_2,delta_3,xi_1,xi_2,xi_3=0,0,0,0,0,0
    print 0