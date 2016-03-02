#-*-coding:utf-8-*-
import sys,os
import urllib,urllib2,requests
import lxml,lxml.html
import re,chardet
from urlparse import urlparse
from HTMLParser import HTMLParser
from htmlentitydefs import name2codepoint
from lxml import etree


#Auto Encode and Decode
def auto_encode(data):
    if isinstance(data, unicode):
        return data.encode('utf-8')
    enc = get_encoding(data)
    return data.decode(enc).encode('utf-8')

def auto_decode(data):
    if isinstance(data, unicode):
        return data
    enc = get_encoding(data)
    return data.decode(enc)

def get_encoding(data):
    if isinstance(data, unicode):
        return 'unicode'
    enc = 'utf-8'
    if not data.strip() or len(data) < 10:
        return enc
    res = chardet.detect(data)
    enc = res['encoding']
    return custom_decode(enc)

def custom_decode(encoding):
    encoding = encoding.lower()
    alternates = {
        'big5': 'big5hkscs',
        'gb2312': 'gb18030',
        'ascii': 'utf-8',
        'MacCyrillic': 'cp1251',
    }
    if encoding in alternates:
        return alternates[encoding]
    else:
        return encoding


#Get Product Description From Advertiser 
def get_struct(home):
    try:
        content=requests.get(home,timeout=5).content
    except Exception as e:
        print 'When getting the home,encounter error ',e
        content=''
    x=lxml.html.document_fromstring(content)
    x.make_links_absolute(home)
    y=x.xpath('//a/@href')
    urlist=[]
    dm='.'.join(urlparse(home).netloc.split('.')[1:])
    for i in y:
        if i.find(dm)>0:
            urlist.append(i)
    print 'First Level Contains ',len(urlist),' Pages.'
    if home not in urlist:
        urlist.append(home)
    return urlist

def execf(url,content,Content):
    h=[]
    for i in range(1,7):
        locals()['h'+str(i)]=''
        h.append('h'+str(i))
        try:
            exec(h[i-1]+"=re.search(r'<h"+str(i)+">([\s\S]*?)</h"+str(i)+">',content)")
            head=locals()['h'+str(i)]
            if head!=None:
                Content.append(head.group(1))
        except Exception as e:
            print 'When getting h'+str(i),e
    return Content

def get_doc(urlist):
    Content=[]
    n=0
    for url in urlist:
        n=n+1
        print 'This is the ',n,' page --------------------------'
        try:
            content=requests.get(url,timeout=5).content
        except Exception as e:
            print 'When getting the page,encounter error ',e
            content=''
        try:
            content=auto_decode(content)
        except Exception as e:
            content=''
            print 'When using auto_decode,encounter error ',e
        try:
            x=lxml.html.document_fromstring(content)
        except Exception as e:
            print 'When parsing the page,encounter error',e
        try:
            title=re.search(r"<title>([\s|\S]*?)</title>",content)
            title=title.group(1)
            Content.append(title)
        except Exception as e:
            print 'When getting title ',e
        Content=execf(url,content,Content)
        try:
            meta=re.search(r"<meta name([\s|\S]*?)>",content)
            if meta!=None:
                rmlist=meta.group(1).split('"')
                Content.append(rmlist[len(rmlist)-2])
        except Exception as e:
            print 'When getting meta ',e
        try:
            divs=x.xpath('//div/descendant::text()')
            for line in divs:
                line=line.strip()
                line=re.sub('\s','',line)
                if len(line)>6:
                    Content.append(line)
        except Exception as e:
            print 'When getting div ',e
        try:
            detail=x.xpath('//p/descendant::text()')
            c=-1
            for line in detail:
                c=c+1
                line=line.strip()
                line=re.sub('\s','',line)
                detail[c]=line
            c=-1
            for line in detail:
                c=c+1
                if len(line)>=6:
                    Content.append(line)
        except Exception as eo:
            print 'When getting p ',eo
    Content=list(set(Content))
    return Content

def ifilter(content):
    flist=['{','}','*','...','=']
    for sep in flist:
        if content.find(sep)>0:
            return True
            break
        else:
           continue
    return False

def get_clause(Content):
    global Clause
    Clause=[]
    for content in Content:
        content=content.strip()
        if not ifilter(content):
            content=re.split(u'[|£¬|¡£|£º|£¡|£¿|¡¢|,|/|?|!|]|¡°|¡±|<|>',content)
            for clause in content:
                if clause!='':
                    if not clause[0:2].isdigit():
                        if len(clause)>3:
                            clause=clause.replace('\n','')
                            Clause.append(clause)
    Clause=list(set(Clause))
    print 'Length of Clause is ',len(Clause)
    return Clause

def get_name(home):
    if home.find('www') > -1:
        name=home.split('.')[1]
    else:
        if home.find('com/') > 0:
            name=home.split('com/')[1].split('.')[0]
        else:
            home=home.replace('http://','')
            name=home.split('.')[0]
    return name

if __name__=='__main__':
    home=sys.argv[1]
    urlist=get_struct(home)
    Content=get_doc(urlist)
    Clause=get_clause(Content)
    name=get_name(home)
    reload(sys)
    sys.setdefaultencoding('utf-8')
    with open('/home/data/baijinmei/audience/genetic/data/desc/'+name+'.txt','w+') as file:
       for clause in Clause:
           file.write(clause+'\n')