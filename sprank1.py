import sqlite3

conn = sqlite3.connect('spiderdb.sqlite')
curs = conn.cursor()

#find all the unique ids of the links which send out to other links
curs.execute('''SELECT DISTINCT fromid FROM Links''')
from_id = list()
for row in curs:
    from_id.append(row[0])
    
    
curs.execute('''SELECT DISTINCT fromid,toid FROM Links''')
to_id = list()
links = list()
for row in curs:
    fromid = row[0]
    toid = row[1]
    if fromid not in from_id:
        continue
    #we don't wants any toid links which have not been retrieved yet. We want 
    #everthing to have been retrieved. 
    if toid not in from_id:
        continue
    if fromid == toid:
        continue
    #only links that point to a page that we have already retrieved will be 
    #accessed
    links.append(row)
    if toid not in to_id:
        to_id.append(toid)
        
prev_rank = dict()
for fromidnode in from_id:
    curs.execute('''SELECT new_rank FROM Pages WHERE id = ?''',(fromidnode,))
    rows = curs.fetchone()
    prev_rank[fromidnode] = rows[0]
    
iterations = input('How many iterations: ')
amount = 1
if len(iterations) > 0:
    amount = int(iterations)
    
if len(prev_rank) < 1:
    print('Nothing to page rank')
    quit()
    
for i in range(amount):
    next_rank= dict()
    total = 0.0
    for (fromidnode,prevrank) in list(prev_rank.items()):
        total = total + prevrank
        next_rank[fromidnode] = 0.0
    for (fromidnode,prevrank) in list(prev_rank.items()):
        give_ids = list()
        for (fromid,toid) in links:
            #we want the fromid from links and fromidnode from prev_rank 
            #to be equal
            if fromid != fromidnode:
                continue
            if toid not in to_id:
                continue
            #we are putting in the ids which the fromid urls links into 
            #give_ids list is made for each toid that links into some other url
            #that has also been retrieved already
            #it is a list which shows how much a particular starting link is 
            #linked out into
            give_ids.append(toid)
        #the old ranks divided upon the number of toids it links out to
        if len(give_ids)<1:
            continue
        averageamount = prevrank/(len(give_ids))
        
        for id in give_ids:
            #we are giving each id a potion of the divided rank of the parent 
            #link 
            next_rank[id] = next_rank[id] + averageamount
    newtotal = 0
    for (fromidnode,rank) in list(next_rank.items()):
        newtotal = newtotal + rank
    #takes a fraction away from each fromid and gives it to everybody
    evap = (total - newtotal)/len(next_rank)
    
    for fromidnode in next_rank:
        next_rank[fromidnode] = next_rank[fromidnode] + evap
        
    #computes the average change in rank from prev_rank to next_rank
    #this is an indication of the stability of the page rank
    #the more the rank has changed, the more unstable it is
    totaldiff = 0
    for (fromidnode,rank) in prev_rank.items():
        totaldiff = totaldiff + abs(rank - next_rank[fromidnode])
        
    #calculate the average difference per node
    averagediff = totaldiff/len(prev_rank)
    print(i + 1,averagediff)
    
    prev_rank = next_rank

curs.execute('''UPDATE Pages SET old_rank = new_rank''')

for (fromidnode,rank) in (next_rank.items()):
    curs.execute('''UPDATE Pages SET new_rank = ? WHERE id = ?''',(rank,fromidnode))

conn.commit()
curs.close()

#the more times a link is referred to and is not the parent, the higher its rank
#is
 
