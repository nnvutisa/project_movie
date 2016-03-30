#web scarping movie data from Numbers and write to a csv file 

import requests
import re
from bs4 import BeautifulSoup
from bs4 import SoupStrainer
import numpy as np
import pandas as pd
from datetime import datetime
import timeit
import time

#The Numbers movies budget data
url =r'http://www.the-numbers.com/movie/budgets/all'
home_url=ur'http://www.the-numbers.com'
source_code = requests.get(url)
text= source_code.text
soup = BeautifulSoup(text,"html5lib")
rows = soup.find_all('tr')

#extracing the whole table
release_date=[]
name=[]
budget=[]
domestic=[]
world=[]
link=[]
#read data
for row in rows:
    cells = row.find_all('td')
    if len(cells) != 0:
        release_date.append(unicode(cells[1].string))
        name.append(unicode(cells[2].string))
        budget.append(unicode(cells[3].string))
        domestic.append(unicode(cells[4].string))
        world.append(unicode(cells[5].string))
        linktext = row.find('b').a['href']
        link.append(linktext)
    
#change formats
for i in range(0,len(name)):
    release_date[i]=datetime.strptime(release_date[i],'%m/%d/%Y').date() #change to date 
    budget[i]=int(budget[i].replace(',','').replace('$','')) #turn into int
    domestic[i]=int(domestic[i].replace(',','').replace('$','')) #turn into int
    world[i]=int(world[i].replace(',','').replace('$','')) #turn into int

#create DataFrame
movie_dict={'date':release_date,'name':name,'budget':budget,
            'domestic':domestic,'world':world,'link':link}
data = pd.DataFrame(data=movie_dict)
#change to date type
data.date = data.date.map(lambda x: x.year)
#write to file
data.to_csv('movie_budget.csv',encoding='utf-8')

#get 2015 movies
data2015 = data[data.date == 2015]
#extract other infos for data2015
n = len(data2015)
critic_rating=[np.nan]*n
audience_rating=[np.nan]*n
genre=[np.nan]*n
director=[np.nan]*n
writer=[np.nan]*n
cast=np.empty((n,10),dtype=object)
for i in range(n):
    ilink = data2015.ix[data2015.index[i],'link']
    isource_code = requests.get(home_url+ilink)
    itext= isource_code.text
    isoup = BeautifulSoup(itext,"html5lib")
    ratings = isoup.find('table',id="movie_ratings").tbody.find_all(href=True)
    if len(ratings) >= 4:
        ctext=ratings[2].get_text()
        atext=ratings[3].get_text()
        critic_rating_text=ctext[len('Critics\n'):ctext.find(u'%')]
        audience_rating_text=atext[len('Audience\n'):atext.find(u'%')]
        critic_rating[i]=int(critic_rating_text)
        audience_rating[i]=int(audience_rating_text)
    table=isoup.find_all('table',id=False)[1]
    if table.find('a',href=re.compile("genre")):
        genre[i]=table.find('a',href=re.compile("genre")).string
    elif isoup.find_all('table',id=False)[0].find('a',href=re.compile("genre")):
        genre[i]= isoup.find_all('table',id=False)[0].find('a',href=re.compile("genre")).string
    cast_table=isoup.find_all('div',id="cast")
    if len(cast_table) >= 2:
        #get list of casts
        cast_list=cast_table[0].find_all('td',class_="alnright")
        for j in range(10):   
            if j+1 <= len(cast_list):
                cast[i,j]=cast_list[j].string
            else:
                break
        #get director and screen writer  
        if cast_table[1].find('td',text="Director"):
            director[i]=cast_table[1].find('td',text="Director").next_sibling.next_sibling.next_sibling.next_sibling.string
        if cast_table[1].find('td',text="Screenwriter"):
            writer[i]=cast_table[1].find('td',text="Screenwriter").next_sibling.next_sibling.next_sibling.next_sibling.string
    elif len(cast_table) == 1:
        if cast_table[0].find('td',text="Director"):
            director[i]=cast_table[0].find('td',text="Director").next_sibling.next_sibling.next_sibling.next_sibling.string
        if cast_table[0].find('td',text="writer"):
            writer[i]=cast_table[0].find('td',text="Writer").next_sibling.next_sibling.next_sibling.next_sibling.string
    
    print data2015.ix[data2015.index[i],'name'],'Genre: ',genre[i],' Director: ',director[i],' Writer: ',writer[i]
    print ' Critic: ',critic_rating[i]
    print ' Audience: ',audience_rating[i]
    time.sleep(5)
    
#concat the new data into data2015
data2015 = data2015.reset_index()
castData=pd.DataFrame(cast,columns=["cast1","cast2","cast3",
                                    "cast4","cast5","cast6","cast7","cast8","cast9","cast10"])
data2015 = pd.concat([data2015,castData],axis=1)
data2015['critic_rating']=critic_rating
data2015['audience_rating']=audience_rating
data2015['genre']=genre
data2015['director']=director
data2015['writer']=writer
#write to file
data2015.to_csv('movie2015.csv',encoding='utf-8')
