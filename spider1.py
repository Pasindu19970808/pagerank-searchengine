import sqlite3
import urllib.parse
import urllib.request
import urllib.error
from bs4 import BeautifulSoup
import ssl

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

conn = sqlite3.connect('spiderdb.sqlite')
curs = conn.cursor()

curs.execute('''CREATE TABLE IF NOT EXISTS Pages
    (id INTEGER PRIMARY KEY, url TEXT UNIQUE, html TEXT,
     error INTEGER, old_rank REAL, new_rank REAL)''')

curs.execute('''CREATE TABLE IF NOT EXISTS Links
        (fromid INTEGER, 
        toid INTEGER,
        UNIQUE(fromid,toid))''')

curs.execute('''CREATE TABLE IF NOT EXISTS Webs(url TEXT)''')

# After your initial web crawl, this takes a random url thats in the data base and starts crawling that url
curs.execute('''SELECT id,url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1''')
row0 = curs.fetchone()
if row0 is not None:
    print("Restart existing crawl. Delete the existing database to start a new crawl")
else:
    inputurl = input('Enter a web url to visit: ')
    if (len(inputurl)<1):
        inputurl = 'https://www.wikipedia.org/'
    if inputurl.endswith('/'):
        inputurl = inputurl[:-1]
        web = inputurl
    if (inputurl.endswith('.htm') or inputurl.endswith('.html')):
        pos = inputurl.rfind('/')
        web = inputurl[:pos]

    if len(web)>1:
        curs.execute('''INSERT OR IGNORE INTO Webs (url) VALUES (?)''',(web,))
        curs.execute('''INSERT OR IGNORE INTO Pages (url,html,new_rank) VALUES (?,NULL,1.0)''',(inputurl,))
        conn.commit()
        
#crawling the webpage
curs.execute('SELECT url from Webs')

webs = list()
for rows in curs:
    webs.append(str(rows[0]))
    
print(webs)

amounttoretrieve = input('How many pages to retrieve: ')
amount = int(amounttoretrieve)
while int(amount) >= 1:
    
    #amount = amount - 1
    try:
        curs.execute('''SELECT id,url FROM Pages where html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1''')
        row = curs.fetchone()
        fromid = row[0]
        searchurl = row[1]
    except:
        print("All urls have been retrieved")
        break
    #As it is unretrieved, we need to wipe out all the cases of this url is Links
    curs.execute('''DELETE FROM Links WHERE fromid = ?''',(fromid,))
    try:
        html = urllib.request.urlopen(searchurl,context = ctx).read()
        
        if urllib.request.urlopen(searchurl,context = ctx).getcode() != 200:
            print('Corrupted page. HTML Status Code: ', html.getcode())
            curs.execute('''UPDATE Pages SET error = ? WHERE url = ?''',(html.getcode(),searchurl))
        if urllib.request.urlopen(searchurl,context = ctx).info().get_content_type() != 'text/html':
            print('Invalid content contained in: ',searchurl)
            curs.execute('''DELETE FROM Pages WHERE url = ?''',(searchurl,))
            conn.commit()
            continue
        
        
        soup = BeautifulSoup(html,'html.parser')
        
    except KeyboardInterrupt:
        print('Program interrupted by User')
        break
    except:
        print("Unable to retrieve page")
        curs.execute('''UPDATE Pages SET error = -1 WHERE url = ?''',(searchurl,))
        print(searchurl)
        conn.commit()
        continue
    
    curs.execute('''INSERT OR IGNORE INTO Pages (url,html,new_rank) VALUES (?,NULL,1.0)''',(searchurl,))
    curs.execute('''UPDATE Pages SET html = ? where url = ?''',(memoryview(html),searchurl))
    conn.commit()
    
    
    tags = soup('a')
    count = 0
    for tag in tags:
        href = tag.get('href',None)
        if len(urllib.request.urlparse(href).scheme) < 1:
            href = urllib.parse.urljoin(searchurl,href)
        ipos = href.find('#')
        if (ipos > 1):
            href = href[:ipos]
        if ( href.endswith('.png') or href.endswith('.jpg') or href.endswith('.gif') ):
            continue
        if (href.endswith('/')):
            href = href[:-1]
            
        #we do not want any href which lead the search out of the website
        #if hr
        
        if str(href) in webs:
            continue
        else:
            curs.execute('''INSERT OR IGNORE INTO PAGES (url,html,new_rank) VALUES (?,NULL,1.0)''',(href,))
            count = count + 1
            conn.commit()
            
            curs.execute('''SELECT id FROM Pages WHERE url = ? LIMIT 1''',(href,))
            try:
                row1 = curs.fetchone()
                toid = row1[0]
            except:
                print('Could not retrieve id')
                continue
            curs.execute('''INSERT OR IGNORE INTO Links (fromid,toid) VALUES (?,?)''',(fromid,toid))
        
     
    amount = amount - 1

curs.close()    
    
        
        
        
        
        
        
        