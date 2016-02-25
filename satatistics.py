#-*-coding:utf-8-*-
from pyspark import SparkContext
import os
import sys
import conf
import hashlib
import math


#Data Parse Method
def hashurl(url):
    reload(sys)
    sys.setdefaultencoding('utf-8')
    return hashlib.md5(url).hexdigest()

def title_parse(data):
    reload(sys)
    sys.setdefaultencoding('utf-8')
    seps=[u'¡±',u'¡°',u'"',u',',u';',u'£º',u':',u'£¬',u'£¨',u'£©',u'£¡',u'¡¶',u'¡·',u'¡¿',u'¡¾',u'¡ª',u'|',u'_',u'-',u'(',u')',u']',u'[',u' ']
    for sep in seps:
        data=data.replace(sep,'|')
    dtl=list(set(data.split('|')))
    try:
        dtl.remove('')
    except Exception as e:
        print e
    return dtl


#Get Training Parameter
def add(a,b):
    return a+b

def get_id_clause(kv):
    new=[]
    for clause in kv[1]:
        new.append((kv[0],clause))
    return new

def get_single_word(clause):
    new=[]
    for i in range(len(clause)):
        new.append(clause[i])
    return new

def get_joint_words(clause):
    new=[]
    for i in range(len(clause)-1):
        new.append(clause[i]+clause[i+1])
    return new

def get_ternate_words(clause):
    new=[]
    L=len(clause)
    if L>=3:
        for i in range(L-2):
           new.append(clause[i]+clause[i+1]+clause[i+2])
    return new

def get_four_words(clause):
    new=[]
    L=len(clause)
    if L>=4:
        for i in range(L-3):
            new.append(clause[i]+clause[i+1]+clause[i+2]+clause[i+3])
    return new

def get_six_words(clause):
    new=[]
    L=len(clause)
    if L>=6:
        for i in range(L-5):
            new.append(clause[i]+clause[i+1]+clause[i+2]+clause[i+3]+clause[i+4]+clause[i+5])
    return new

def get_seven_words(clause):
    new=[]
    L=len(clause)
    if L>=7:
        for i in range(L-6):
            new.append(clause[i]+clause[i+1]+clause[i+2]+clause[i+3]+clause[i+4]+clause[i+5]+clause[6])
    return new

def get_mutual_information(kv,wordict):
    reload(sys)
    sys.setdefaultencoding('utf-8')
    words=kv[0]
    w1=words[0]
    w2=words[1]
    jprob=kv[1]
    try:
        mi=math.log(jprob*1.0/(wordict[w1]*wordict[w2]),math.e)
    except Exception as e:
        mi='unknow'
        print e
    return mi

def get_conditional_prob(kv,wordict):
    words=kv[0]
    c_word=words[1]
    prob=kv[1]/wordict[c_word]
    return prob

def get_t_score(kv,fvar,lvar):
    #(words,(prob_0,prob_1))
    words=kv[0]
    fprob=kv[1][0]
    lprob=kv[1][1]  
    prob=(lprob-fprob)/math.sqrt(fvar+lvar)
    return (words,prob)

def judge_local_maximum(lmr_dts):
    #(dts_left,dts_middle,dts_right)
    dts_left=lmr_dts[0]
    dts_middle=lmr_dts[1]
    dts_right=lmr_dts[2]
    hdts=None
    if dts_middle > dts_left and dts_middle > dts_right:
        hdts=min([dts_middle-dts_left,dts_middle-dts_right])
    return hdts

def judge_local_minimum(lmr_dts):
    #(dts_left,dts_middle,dts_right)
    dts_left=lmr_dts[0]
    dts_middle=lmr_dts[1]
    dts_right=lmr_dts[2]
    ddts=None
    if dts_middle < dts_left and dts_middle < dts_right:
        ddts=min([dts_left-dts_middle,dts_right-dts_middle])
    return ddts

def judge_right_second_local_maximum(dtss):
    #(dts_first,dts_second,dts_left,dts_right)
    dts_first=dtss[0]
    dts_second=dtss[1]
    dts_left=dtss[2]
    dts_right=dtss[3]
    dis_loc=0
    if dts_second > dts_left and dts_second > dts_right:
        dis_loc=dts_first-dts_second
    return dis_loc

def judge_right_second_local_minimum(dtss):
    #(dts_first,dts_second,dts_left,dts_right)
    dts_first=dtss[0]
    dts_second=dtss[1]
    dts_left=dtss[2]
    dts_right=dtss[3]
    dis_loc=0
    if dts_second < dts_left and dts_second < dts_right:
        dis_loc=dts_first-dts_second
    return dis_loc

def judge_left_second_local_maximum(dtss):
    #(dts_first,dts_second,dts_left,dts_right)
    dts_first=dtss[0]
    dts_second=dtss[1]
    dts_left=dtss[2]
    dts_right=dtss[3]
    dis_loc=0
    if dts_second > dts_left and dts_second > dts_right:
        dis_loc=dts_first-dts_second
    return dis_loc

def judge_left_second_local_minimum(dtss):
    #(dts_first,dts_second,dts_left,dts_right)
    dts_first=dtss[0]
    dts_second=dtss[1]
    dts_left=dtss[2]
    dts_right=dtss[3]
    dis_loc=0
    if dts_second < dts_left and dts_second < dts_right:
        dis_loc=dts_first-dts_second
    return dis_loc

def basic_statistics(sc,dataPATH):
    reload(sys)
    sys.setdefaultencoding('utf-8')
    data=sc.textFile(dataPATH).map(lambda line:line.split('\x01')).filter(lambda linel:len(linel)==2).filter(lambda kv:len(kv[1])>1).map(lambda kv:(kv[0],kv[1]))
    idldocs=data.zipWithIndex().map(lambda kv:(kv[1],kv[0]))
    #(id,(url,title))
    idurl=idldocs.map(lambda kv:(kv[0],kv[1][0]))
    #(id,url)
    idocs=idldocs.map(lambda kv:(kv[0],kv[1][1])).map(lambda kv:(kv[0],title_parse(kv[1])))
    #(id,title)
    idause=idocs.flatMap(lambda kv:get_id_clause(kv))
    #(id,clause)
    single_word=sc.parallelize(idause.flatMap(lambda kv:get_single_word(kv[1])).countByValue().items())
    #(word,number)
    single_word_count=single_word.map(lambda kv:kv[1]).sum()
    joint_words=sc.parallelize(idause.flatMap(lambda kv:get_joint_words(kv[1])).countByValue().items())
    #(words,number)
    joint_words_count=joint_words.map(lambda kv:kv[1]).sum()
    single_word_prob=single_word.map(lambda kv:(kv[0],kv[1]*1.0/single_word_count))
    #(word,prob)
    joint_words_prob=joint_words.map(lambda kv:(kv[0],kv[1]*1.0/joint_words_count))
    #(words,prob)
    single_word_prob_collect=single_word_prob.take(single_word_prob.count())
    joint_words_prob_collect=joint_words_prob.take(joint_words_prob.count())
    wordict=dict(single_word_prob_collect)
    mutual_information=joint_words_prob.map(lambda kv:(kv[0],get_mutual_information(kv,wordict)))
    
    conditional_prob=joint_words_prob.map(lambda kv:(kv[0],get_conditional_prob(kv,wordict))).repartition(100)
    #p(z|y)=p(z,y)/p(y)
    ternate_words=idause.flatMap(lambda kv:get_ternate_words(kv[1])).distinct().repartition(100)
    fvar,lvar=get_cond_prob_var(ternate_words,conditional_prob)
    t_score_fore=ternate_words.map(lambda words:(words[:2],words)).leftOuterJoin(conditional_prob).map(lambda kv:kv[1])
    #words--->(words[:2],words)--->(words[:2],(words,prob_0))--->(words,prob_0)
    t_score_latter=ternate_words.map(lambda words:(words[1:],words)).leftOuterJoin(conditional_prob).map(lambda kv:kv[1])
    #words--->(words[1:],words)--->(words[1:],(words,prob_1))--->(words,prob_1)
    t_score=t_score_fore.leftOuterJoin(t_score_latter).map(lambda kv:get_t_score(kv,fvar,lvar))
    #(words,(prob_0,prob_1))--->(words,t_score)

    four_words=idause.flatMap(lambda kv:get_four_words(kv[1])).distinct().repartition(100)
    t_score_fore=four_words.map(lambda words:(words[:3],words)).leftOuterJoin(t_score).map(lambda kv:kv[1])
    #(words[:3],words)--->(words[:3],(words,t_score_fore))--->(words,t_score_fore)
    t_score_latter=four_words.map(lambda words:(words[1:],words)).leftOuterJoin(t_score).map(lambda kv:kv[1])
    #(words[1:],words)--->(words[1:],(words,t_score_latter))--->(words,t_score_latter)
    dts=t_score_fore.join(t_score_latter).map(lambda kv:(kv[0],kv[1][0]-kv[1][1]))
    #(words,(t_score_fore,t_score_latter))-(words,dts:difference of t_score between middle two words)
        
    mu_mi=mutual_information.map(lambda kv:kv[1]).mean()
    #(words,mi)--->mu_mi
    delta_mi=mutual_information.map(lambda kv:(kv[1]-mu_mi)**2).sum()*1.0/mutual_information.count()
    mu_dts=dts.map(lambda kv:kv[1]).mean()
    #(words,dts)--->mu_dts
    delta_dts=dts.map(lambda kv:(kv[1]-mu_dts)**2).sum()*1.0/dts.count()
    print 'mu_mi=',mu_mi,'delta_mi=',delta_mi,'mu_dts=',mu_dts,'delta_dts=',delta_dts

    six_words=idause.flatMap(lambda kv:get_six_words(kv[1])).distinct().repartition(100)
    h_or_d_dts_left=six_words.map(lambda words:(words[0:4],words)).leftOuterJoin(dts).map(lambda kv:kv[1])
    #(words[0:4],words)--->(words[0:4],(words,dts_left))--->(words,dts_left)
    h_or_d_dts_middle=six_words.map(lambda words:(words[1:5],words)).leftOuterJoin(dts).map(lambda kv:kv[1])
    h_or_d_dts_right=six_words.map(lambda words:(words[2:6],words)).leftOuterJoin(dts).map(lambda kv:kv[1])
    h_or_d_dts=h_or_d_dts_left.join(h_or_d_dts_middle).join(h_or_d_dts_right).map(lambda kv:(kv[0],(kv[1][0][0],kv[1][0][1],kv[1][1])))
    #(words,(dts_left,dts_middle))--->(words,((dts_left,dts_middle),dts_right))--->(words,(dts_left,dts_middle,dts_right))
    h_dts=h_or_d_dts.map(lambda kv:(kv[0],judge_local_maximum(kv[1]))).filter(lambda kv:kv[1]!=None)
    #(words,hdts)
    d_dts=h_or_d_dts.map(lambda kv:(kv[0],judge_local_minimum(kv[1]))).filter(lambda kv:kv[1]!=None)

    seven_words=idause.flatMap(lambda kv:get_seven_words(kv[1])).distinct().repartition(100)
    dis_max_second_max_left=seven_words.map(lambda words:(words[0:6],words)).leftOuterJoin(h_dts).map(lambda kv:kv[1]).filter(lambda kv:kv[1]!=None).map(lambda kv:kv[0])
    #(words[0:6],(words,hdts))--->(words,hdts)--->words  eg:u'3\u5e74\u5c0f\u51e4\u59d0\u91c7\u91c7'
    dis_max_second_max_left_first=dis_max_second_max_left.map(lambda words:(words[2:6],words)).leftOuterJoin(dts).map(lambda kv:kv[1]).filter(lambda kv:kv[1]!=None)
    #words--->(words[2:6],words)--->(words[2:6],(words,dts_first))--->(words,dts_first)
    dis_max_second_max_left_second=dis_max_second_max_left.map(lambda words:(words[1:5],words)).leftOuterJoin(dts).map(lambda kv:kv[1]).filter(lambda kv:kv[1]!=None)
    #words--->(words[1:5],words)--->(words[1:5],(words,dts_second))--->(words,dts_second)
    dis_max_second_max_left_lefth=dis_max_second_max_left.map(lambda words:(words[0:4],words)).leftOuterJoin(dts).map(lambda kv:kv[1]).filter(lambda kv:kv[1]!=None)
    #words--->(words[0:4],words)--->(words[0:4],(words,dts_lefth))--->(words,dts_lefth)
    dis_max_second_max_left_righth=dis_max_second_max_left.map(lambda words:(words[3:7],words)).leftOuterJoin(dts).map(lambda kv:kv[1]).filter(lambda kv:kv[1]!=None)
    #example:(u'\u8def\u519b\u5bf9\u4ed8\u9b3c\u5b50\u5b50', 0.84806080826188912)
    dis_max_l=dis_max_second_max_left.join(dis_max_second_max_left_first).join(dis_max_second_max_left_second).join(dis_max_second_max_left_lefth).join(dis_max_second_max_left_righth).map(lambda kv:(kv[0],(kv[1][0][0][0][1],kv[1][0][0][1],kv[1][0][1],kv[1][1]))).map(lambda kv:(kv[0],judge_left_second_local_maximum(kv[1])))
    #(words,((((ddts,dts_first),dts_second),dts_right),dts_left))--->(words,(dts_first,dts_second,dts_left,dts_right))
    for i in dis_max_second_max_left_righth.take(100):
        print 'dis_max_second_max_left_righth',i
    for i in dis_max_second_max_left_lefth.take(100):
        print 'dis_max_second_max_left_lefth',i
    for i in dis_max_second_max_left_second.take(100):
        print 'dis_max_second_max_left_second',i
    for i in dis_max_second_max_left_first.take(100):
        print 'dis_max_second_max_left_first',i
    print 'count right',dis_max_second_max_left_righth.count()
    print 'count left',dis_max_second_max_left_lefth.count()
    print 'count second',dis_max_second_max_left_second.count()
    print 'count first',dis_max_second_max_left_first.count()
      
   
    dis_min_second_min_left=seven_words.map(lambda words:(words[0:6],words)).leftOuterJoin(d_dts).map(lambda kv:kv[1]).filter(lambda kv:kv[1]!=None).map(lambda kv:kv[0])
    #(words[0:6],(words,ddts))--->(words,ddts)--->words
    dis_min_second_min_left_first=dis_min_second_min_left.map(lambda words:(words[2:6],words)).leftOuterJoin(dts).map(lambda kv:kv[1]).filter(lambda kv:kv[1]!=None)
    dis_min_second_min_left_second=dis_min_second_min_left.map(lambda words:(words[1:5],words)).leftOuterJoin(dts).map(lambda kv:kv[1]).filter(lambda kv:kv[1]!=None)
    dis_min_second_min_left_lefth=dis_min_second_min_left.map(lambda words:(words[0:4],words)).leftOuterJoin(dts).map(lambda kv:kv[1]).filter(lambda kv:kv[1]!=None)
    dis_min_second_min_left_righth=dis_min_second_min_left.map(lambda words:(words[3:7],words)).leftOuterJoin(dts).map(lambda kv:kv[1]).filter(lambda kv:kv[1]!=None)
    dis_min_l=dis_min_second_min_left.join(dis_min_second_min_left_first).join(dis_min_second_min_left_second).join(dis_min_second_min_left_lefth).join(dis_min_second_min_left_lefth).map(lambda kv:(kv[0],(kv[1][0][0][0][1],kv[1][0][0][1],kv[1][0][1],kv[1][1]))).map(lambda kv:(kv[0],judge_left_second_local_minimum(kv[1])))
    
    dis_max_second_max_right=seven_words.map(lambda words:(words[1:7],words)).leftOuterJoin(h_dts).map(lambda kv:kv[1]).filter(lambda kv:kv[1]!=None).map(lambda kv:kv[0])
    #(words[1:7],(words,hdts))--->(words,hdts)--->words
    dis_max_second_max_right_first=dis_max_second_max_right.map(lambda words:(words[1:5],words)).leftOuterJoin(dts).map(lambda kv:kv[1]).filter(lambda kv:kv[1]!=None)
    dis_max_second_max_right_second=dis_max_second_max_right.map(lambda words:(words[2:6],words)).leftOuterJoin(dts).map(lambda kv:kv[1]).filter(lambda kv:kv[1]!=None)
    dis_max_second_max_right_lefth=dis_max_second_max_right.map(lambda words:(words[0:4],words)).leftOuterJoin(dts).map(lambda kv:kv[1]).filter(lambda kv:kv[1]!=None)
    dis_max_second_max_right_righth=dis_max_second_max_right.map(lambda words:(words[3:7],words)).leftOuterJoin(dts).map(lambda kv:kv[1]).filter(lambda kv:kv[1]!=None)
    dis_max_r=dis_max_second_max_right.join(dis_max_second_max_right_first).join(dis_max_second_max_right_second).join(dis_max_second_max_right_lefth).join(dis_max_second_max_right_righth).map(lambda kv:(kv[0],(kv[1][0][0][0][1],kv[1][0][0][1],kv[1][0][1],kv[1][1]))).map(lambda kv:(kv[0],judge_right_second_local_maximum(kv[1])))
    
    dis_min_second_min_right=seven_words.map(lambda words:(words[1:7],words)).leftOuterJoin(d_dts).map(lambda kv:kv[1]).filter(lambda kv:kv[1]!=None).map(lambda kv:kv[0])
    #(words[1:7],(words,ddts))--->(words,ddts)--->words
    dis_min_second_min_right_first=dis_min_second_min_right.map(lambda words:(words[1:5],words)).leftOuterJoin(dts).map(lambda kv:kv[1]).filter(lambda kv:kv[1]!=None)
    dis_min_second_min_right_second=dis_min_second_min_right.map(lambda words:(words[2:6],words)).leftOuterJoin(dts).map(lambda kv:kv[1]).filter(lambda kv:kv[1]!=None)
    dis_min_second_min_right_lefth=dis_min_second_min_right.map(lambda words:(words[0:4],words)).leftOuterJoin(dts).map(lambda kv:kv[1]).filter(lambda kv:kv[1]!=None)
    dis_min_second_min_right_righth=dis_min_second_min_right.map(lambda words:(words[3:7],words)).leftOuterJoin(dts).map(lambda kv:kv[1]).filter(lambda kv:kv[1]!=None)
    dis_min_r=dis_min_second_min_right.join(dis_min_second_min_right_first).join(dis_min_second_min_right_second).join(dis_min_second_min_right_lefth).join(dis_min_second_min_right_righth).map(lambda kv:(kv[0],(kv[1][0][0][0][1],kv[1][0][0][1],kv[1][0][1],kv[1][1]))).map(lambda kv:(kv[0],judge_right_second_local_minimum(kv[1])))
    #(words,((((ddts,dts_first),dts_second),dts_right),dts_left))--->(words,(dts_first,dts_second,dts_left,dts_right))

    with open(conf.single_word_prob,'w+') as f1:
        for i in single_word_prob_collect:
            f1.write(i[0]+':'+str(i[1])+'\n')
    with open(conf.joint_words_prob,'w+') as f2:
        for i in joint_words_prob_collect:
            f2.write(i[0]+':'+str(i[1])+'\n')
    with open(conf.mutual_information,'w+') as f3:
        for i in mutual_information.take(mutual_information.count()):
            f3.write(i[0]+':'+str(i[1])+'\n')
    with open(conf.t_score,'w+') as f4:
        for i in t_score.take(t_score.count()):
            f4.write(i[0]+':'+str(i[1])+'\n')
    with open(conf.dts,'w+') as f5:
        for i in dts.take(dts.count()):
            f5.write(i[0]+':'+str(i[1])+'\n')
    with open(conf.h_dts,'w+') as f6:
        for i in h_dts.take(h_dts.count()):
            f6.write(i[0]+':'+str(i[1])+'\n')
    with open(conf.d_dts,'w+') as f7:
        for i in d_dts.take(d_dts.count()):
            f7.write(i[0]+':'+str(i[1])+'\n')
    with open(conf.dis_max_l,'w+') as f8:
        for i in dis_max_l.take(dis_max_l.count()):
            f8.write(i[0]+':'+str(i[1])+'\n')
    with open(conf.dis_min_l,'w+') as f9:
        for i in dis_min_l.take(dis_min_l.count()):
            f9.write(i[0]+':'+str(i[1])+'\n')
    with open(conf.dis_max_r,'w+') as f10:
        for i in dis_min_r.take(dis_min_r.count()):
            f10.write(i[0]+':'+str(i[1])+'\n')
 
    return (single_word_prob,joint_words_prob,mutual_information,t_score,dts,h_dts,d_dts,dis_max_l,dis_min_l,dis_max_r,dis_min_r,mu_mi,delta_mi,mu_dts,delta_dts)

def get_cond_prob_var(ternate_words,conditional_prob):
    fore_words_prob=sc.parallelize(ternate_words.map(lambda words:(words[:2],0)).leftOuterJoin(conditional_prob).map(lambda kv:(kv[0],kv[1][1])).countByValue().items())
    #((words,prob),count)
    latter_words_prob=sc.parallelize(ternate_words.map(lambda words:(words[1:],0)).leftOuterJoin(conditional_prob).map(lambda kv:(kv[0],kv[1][1])).countByValue().items())
    fmean=fore_words_prob.map(lambda kv:kv[0][1]).mean()
    lmean=latter_words_prob.map(lambda kv:kv[0][1]).mean()
    fsum=fore_words_prob.map(lambda kv:kv[1]).sum()
    lsum=latter_words_prob.map(lambda kv:kv[1]).sum()
    fvar=fore_words_prob.map(lambda kv:(kv[0][1]-fmean)**2*(kv[1]*1.0/fsum)).sum()
    lvar=latter_words_prob.map(lambda kv:(kv[0][1]-lmean)**2*(kv[1]*1.0/lsum)).sum()
    print 'Fore Words Variance',fvar,'Latter Words Variance',lvar
    return fvar,lvar

def derivsep(clause):
    Clause=[]
    for word in clause:
        Clause.append(word)
        Clause.append('/')
    return 0

def join_parameter(single_word_prob,joint_words_prob,mutual_information,t_score,dts,h_dts,d_dts,dis_max_l,dis_min_l,dis_max_r,dis_min_r,mu_mi,delta_mi,mu_dts,delta_dts):
    return 0

if __name__=='__main__':
    sc=SparkContext('','derivtrn')
    dataPATH=conf.dataPATH
    status=['bound','separated','?']
    single_word_prob,joint_words_prob,mutual_information,t_score,dts,h_dts,d_dts,dis_max_l,dis_min_l,dis_max_r,dis_min_r,mu_mi,delta_mi,mu_dts,delta_dts=basic_statistics(sc,dataPATH)