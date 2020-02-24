import sqlite3

conn = sqlite3.connect('spiderdb.sqlite')

curs = conn.cursor()

print('Creating JSON output on spider.js')
howmany = int(input('How many nodes to produce: '))

curs.execute('''SELECT COUNT(fromid) AS inbound,old_rank,new_rank,url,id FROM Pages
             JOIN Links ON Pages.id = Links.toid
             WHERE html IS NOT NULL AND error IS NULL
             GROUP BY id 
             ORDER BY id,inbound''')#orders by the number of inbound links and arranges the id with it

fhand = open('spider.js','wb')
nodes = list()
maxrank = None
minrank = None

for row in curs:
    nodes.append(row)#similar to json format here
    rank = row[2]
    if maxrank is None or rank > maxrank:
        maxrank = rank
    if minrank is None or rank < minrank:
        minrank = rank
    if len(nodes) == howmany:
        break

if maxrank is None or minrank is None or maxrank == minrank:
    print('Error, please run sprank1.py again')
    quit()

#spiderJson is a dictionary
    #nodes is the key
    #value of nodes is a list
    #this list contains many dictionaries
fhand.write('spiderJson = {"nodes":[\n')
count = 0
map = dict()
ranks = dict()

for row in nodes:
    if count > 0:
        fhand.write(',\n')
    rank = row[2]
    rank = 19*((rank - minrank)/(maxrank - minrank))#normalizing the rank to be
    #the thickness of the line
    #this is writing a dictionary
    fhand.write('{'+'"weight":'+str(row[0])+',"rank":'+str(rank)+',')
    fhand.write('"id":'+str(row[4])+',"url":"'+str(rank[3])+'"}')
    map[row[4]] = count
    ranks[row[4]] = rank
    count = count + 1
fhand.write('],\n')
    
curs.execute('''SELECT DISTINCT fromid,toid from Links''')
fhand.write('"links":[\n')

count = 0
for row in curs:
    #the ids that are being highly referred to, should also be fromids
    #only ids which are also in the previous rank set are being looked into
    if row[0] not in map or row[1] not in map:
        continue
    if count>0:
        fhand.write(',\n')
    rank = ranks[row[0]]
    srank = 19*((rank - minrank)/(maxrank - minrank))
    fhand.write()
    
    