import json
import traceback
from flask import Flask,jsonify
from httpcore import ReadTimeout
import requests
from flask import request
from flask_cors import CORS, cross_origin
from hikerapi import Client
from neo4j import GraphDatabase
import time
import concurrent
import asyncio
import aiohttp
import random

api_keys =["Nr28jl6ymzkSfOdk2zENPIRdfA3jGG8o","MUScnF8PRIgHFRXs0MkrO2i0s6dRG7GC","5HmOH5T1zlcLTit9JnpvnX3DWSLP51Ta"]
cl1 = Client(token="Nr28jl6ymzkSfOdk2zENPIRdfA3jGG8o") #js2iYetdF7VxQmNQrr0LA8CD9oRH1gva
cl2=Client(token="MUScnF8PRIgHFRXs0MkrO2i0s6dRG7GC")
cl3=Client(token="5HmOH5T1zlcLTit9JnpvnX3DWSLP51Ta")


app = Flask(__name__)
cors = CORS(app)

class Neo4jConnection:
    
    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
        except Exception as e:
            print("Failed to create the driver:", e)
        
    def close(self):
        if self.__driver is not None:
            self.__driver.close()
        
    def query(self, query, parameters=None, db=None):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try: 
            session = self.__driver.session(database=db) if db is not None else self.__driver.session() 
            response = list(session.run(query, parameters))
        except Exception as e:
            print("Query failed:", e)
        finally: 
            if session is not None:
                session.close()
        return response
    
conn = Neo4jConnection("bolt://44.202.4.248:7687",user="neo4j",pwd= "dissipation-encounters-centers")


def insert_data(query, rows, batch_size = 10000,pk=None, network=False):
    # Function to handle the updating the Neo4j database in batch mode.
    
    total = 0
    batch = 0
    start = time.time()
    result = None
    conn = Neo4jConnection("bolt://44.202.4.248:7687",user="neo4j",pwd= "dissipation-encounters-centers")

    while batch * batch_size < len(rows):

        res = conn.query(query, 
                         parameters = {'rows': rows[batch*batch_size:(batch+1)*batch_size],'pk':pk,'network':network})
        total += res[0]['total']
        batch += 1
        result = {"total":total, 
                  "batches":batch, 
                  "time":time.time()-start}
        print(result)
        
    return result

def add_person_which_has_network(user_pk,user_row):
    conn = Neo4jConnection("bolt://44.202.4.248:7687",user="neo4j",pwd= "dissipation-encounters-centers")
    query='''
    MATCH (n:Person)
    WHERE (n.pk = $user_pk)
    RETURN
    n
    '''
    result = conn.query(query,parameters={'user_pk':int(user_pk)})
    if len(result)>0:
        query='''
            MATCH (n:Person{pk: $user_pk})
            SET n.network = True
            return n
            '''
        result = conn.query(query,parameters={'user_pk':int(user_pk)})
    else:
        add_persons([user_row],network=True)


def add_persons(rows, batch_size=10000,network=False):
    # Adds author nodes to the Neo4j graph as a batch job.
    if network:
        query = '''
                UNWIND $rows AS row
                MERGE (:Person {username:row.username, full_name:row.full_name, pk:row.pk, profile_pic_url:row.profile_pic_url,network:$network})
                RETURN count(*) as total
                '''
    else:
        query = '''
                UNWIND $rows AS row
                MERGE (:Person {username:row.username, full_name:row.full_name, pk:row.pk, profile_pic_url:row.profile_pic_url})
                RETURN count(*) as total
                '''
    return insert_data(query, rows, batch_size,network=network)

def add_relation_follows(pk,rows, batch_size=10000):
    # Adds author nodes to the Neo4j graph as a batch job.
    query = '''
            
            
        UNWIND $rows AS row
        
        WITH row
        
        MATCH(b:Person{pk:row.pk})
        MATCH(a:Person{pk:$pk})
        MERGE (a)-[:FRIENDS]->(b)
        
        RETURN count(*) as total
            '''
    return insert_data(query, rows, batch_size,pk)

def return_user_data_from_graph_db(user_pk):
    conn = Neo4jConnection("bolt://44.202.4.248:7687",user="neo4j",pwd= "dissipation-encounters-centers")
    query='''
    MATCH (n:Person)
    WHERE (n.pk = $user_pk) AND (n.network = True)
    RETURN
    n
    '''
    return conn.query(query,parameters={'user_pk':int(user_pk)})

def return_user_friends(user_pk):
    conn = Neo4jConnection("bolt://44.202.4.248:7687",user="neo4j",pwd= "dissipation-encounters-centers")
    query='''
    MATCH (n:Person)-[:FRIENDS]->(m:Person)
    WHERE n.pk = $user_pk
    RETURN
    m
    '''
    result = conn.query(query,parameters={'user_pk':int(user_pk)})

    result =[x.data()['m'] for x in result]

    return result

def add_to_graph_db(user_pk,user_username,user_name,user_profile_pic_url,user_following):
    user_row={'pk':user_pk,'username':user_username,'full_name':user_name,'profile_pic_url':user_profile_pic_url}
    # add_persons([user_row],network=True)
    add_person_which_has_network(user_pk,user_row)
    add_persons(user_following)
    add_relation_follows(user_pk,user_following)


# INSTAGRAM
def find_mutual_followers(user_pk, item):
    if item['is_private'] is True:
        return None
    
    re_try=True
    count=3
    
    follower_search_result = []

    while(re_try and count>0):
        try:
            cl = random.choice([cl1,cl2,cl3])
            follower_search_result = cl.user_search_followers_v1(user_pk, item['username'])
            re_try=False

        except Exception as e:
            print("1-->",e)
            time.sleep(10)
            count-=1
            if count==0:
                print(f"ReadTimeout{user_pk, item['username']}")
                re_try=False
                follower_search_result = []

    re_try=True
    count=3
                
    if len(follower_search_result) > 0:
        # item_ = follower_search_result[0]
        while(re_try and count>0):
            try:
                for item_ in follower_search_result:
                    if item['pk_id'] == item_['pk']:
                        temp = cl.user_by_username_v2(item['username'])
                        re_try=False
                        return temp['user']
                re_try=False
            except Exception as e:
                # if ('exc_type' in follower_search_result and
                #         follower_search_result['exc_type'] == 'PleaseWaitFewMinutes'):
                #     print({'error': 'Lamadava - mutual inside thread error. Waiting 5 minutes before retry'})
                    
                print("2-->",e,"Response-->",temp)
                count-=1
                cl = random.choice([cl1,cl2,cl3])
                if count==0:
                    re_try=False
                    return None

                
    return None


async def get_data_async(s,user_pk, item):
    try:
        cl = random.choice([cl1,cl2,cl3])
        re_try=True
        count=5

        
        url=f'https://api.hikerapi.com/v1/user/search/followers?user_id={user_pk}&query={item["username"]}'
        headers = {
            'Accept': 'application/json',
            'x-access-key': random.choice(api_keys)
        }
        while(re_try and count>0):

            try:

                async with s.get(url,headers=headers) as r:
                    data = await r.json()
                    
                    if len(data)==0:
                        return None
                    
                    
                    temp = None
                    re_try=True
                    count=3
                    while(re_try and count>0):
                        try:
                            for item_ in data:
                                if item['pk_id'] == item_['pk']:
                                    url = f"https://api.hikerapi.com/v1/user/by/username?username={item['username']}"
                                    headers = {
                                        'Accept': 'application/json',
                                        'x-access-key': random.choice(api_keys)
                                    }
                                    async with s.get(url,headers=headers) as r:
                                        temp = await r.json()
                                        re_try=False
                                        return temp


                                    # temp = cl.user_by_username_v2(item['username'])
                                    # re_try=False
                                    # return temp['user']
                            re_try=False
                        except Exception as e:
                            url = f"https://api.hikerapi.com/v1/user/by/username?username={item['username']}"
                            headers = {
                                        'Accept': 'application/json',
                                        'x-access-key': random.choice(api_keys)
                                    }

                            print("2-->",e,"Response-->",temp)
                            count-=1
                            cl = random.choice([cl1,cl2,cl3])
                            if count==0:
                                re_try=False
                                return 

                    return 
            except Exception as e:
                print("3-->",e)
                count-=1
                url=f'https://api.hikerapi.com/v1/user/search/followers?user_id={user_pk}&query={item["username"]}'
                headers = {
                    'Accept': 'application/json',
                    'x-access-key': random.choice(api_keys)
                }
                time.sleep(10)
                if count==0:
                    re_try=False
                    return


    except Exception as e:
        print(e)
        traceback.print_exc()
        return None



async def fetch_all(s, user_pk, user_following):
    tasks = []
    for item in user_following:
        task = asyncio.create_task(get_data_async(s, user_pk, item))
        tasks.append(task)
    res = await asyncio.gather(*tasks)
    return res

async def main(user_pk,user_following):
    async with aiohttp.ClientSession() as session:
        try:
            result = await fetch_all(session, user_pk, user_following)

            result = [x for x in result if x is not None]
            print("the length of result is ", len(result))
            return result
        except Exception as e:
            traceback.print_exc()
            print("Inside Main Block",e)



@app.route('/followers', methods=['POST'])
#@cross_origin()
@cross_origin(supports_credentials=True)

def followers():
    if request.method == 'POST':
        body = request.get_json()
        print(body)
        user_id = body.get('user_id')
        cl = random.choice([cl1,cl2,cl3])
        user_data = cl.user_by_id_v2(str(user_id))['user']
        user_pk = user_data['pk']
        user_username = user_data['username']
        user_name = user_data['full_name']
        user_profile_pic_url = user_data['profile_pic_url']
        
        get_user_data_from_db=[]
        # get_user_data_from_db = return_user_data_from_graph_db(user_pk)

        if get_user_data_from_db and len(get_user_data_from_db) > 0:
            user_following = return_user_friends(user_pk)
            res = {"selected_user_following":user_following}
            return jsonify(res)


        # x[0].data()['n']

        try:
            user_following_result = cl.user_following_v2(str(user_pk))
            user_following = user_following_result['response']['users']
            json.dump(user_following, open(f"{user_pk}.json", "w"))
        except Exception as e:
            print(e)
            user_following = []
        while user_following_result.get('next_page_id',None) is not None and user_following_result.get('next_page_id',None) != '':
            print(user_following_result['next_page_id'])
            try:
                user_following_result = cl.user_following_v2(str(user_pk), user_following_result['next_page_id'])
            except Exception as e:
                print(e)
                user_following_result = []
            try:
                user_following += user_following_result['response']['users']
            except Exception as e:
                print(e)
                print(user_following_result['response'])
                print("-"*100)
                pass
        
        user_following =[x for x in user_following if x['is_private'] is False]

        print("Total Public Profiles--> ",len(user_following))

        # json.dump(user_following, open(f"{user_pk}.json", "w"))
        # user_following= json.load(open(r'C:\Work\Personal\social-media-mutual-only\264222629.json','r'))
        mutual = []

        mutual=asyncio.run(main(user_pk,user_following))

        # try:
        #     with concurrent.futures.ThreadPoolExecutor(10) as executor:
        #         future_to_item = {executor.submit(find_mutual_followers, user_pk, item): item for item in user_following}
        #         for future in concurrent.futures.as_completed(future_to_item):
        #             result = future.result()
        #             if result is not None:
        #                 mutual.append(result)

        # except Exception as e:
        #     print({'error': f'Lamadava - mutual outside thread error. {e}'})
        
        print("Total Mutual Profiles--> ",len(mutual))
        # json.dump(mutual, open(f"{user_pk}_mutual.json", "w"))
        # add_to_graph_db(user_pk,user_username,user_name,user_profile_pic_url,mutual)
        res = {"selected_user_following":mutual}
        return jsonify(res)
    
@app.route('/media', methods=['POST'])
@cross_origin()
def media():
    if request.method == 'POST':
        body = request.get_json()
        print(body)
        sessionid = body.get('sessionid')
        user_id = body.get('user_id')
        # url = f"https://api.lamadava.com/s1/user/followers/chunk?sessionid={sessionid}&user_id={user_id}&max_amount=100"
        url = f"https://api.lamadava.com/s1/user/medias/chunk?sessionid={sessionid}&user_id={user_id}&max_amount=100"

        payload = {}
        headers = {
        'Accept': 'application/json',
        'x-access-key': 'js2iYetdF7VxQmNQrr0LA8CD9oRH1gva'
        }

        response = requests.request("GET", url, headers=headers, data=payload)

        print(response.text)
        final_output =[]
        output = response.json()

        final_output=final_output+output[0]
        if output[1] and (output[1]!='' or output[1] is not None):
            has_next_url=True
            next_cursor = output[1]
            url = f"https://api.lamadava.com/s1/user/medias/chunk?sessionid={sessionid}&user_id={user_id}&max_amount=100&end_cursor={next_cursor}"
            while(has_next_url):
                response = requests.request("GET", url, headers=headers, data=payload)
                output = response.json()
                if len(output)>0:
                    final_output=final_output+output[0]

                if output[1] is None or output[1]=='':
                    has_next_url=False
                else:
                    next_cursor = output[1]
                    url = f"https://api.lamadava.com/s1/user/medias/chunk?sessionid={sessionid}&user_id={user_id}&max_amount=100&end_cursor={next_cursor}"

        res = {"data":final_output}
        return jsonify(final_output)
    
@app.route('/taggedmedia', methods=['POST'])
@cross_origin()
def tagged_media():
    if request.method == 'POST':
        body = request.get_json()
        print(body)
        sessionid = body.get('sessionid')
        user_id = body.get('user_id')
        # url = f"https://api.lamadava.com/s1/user/followers/chunk?sessionid={sessionid}&user_id={user_id}&max_amount=100"
        url = f"https://api.lamadava.com/s1/user/tag/medias?sessionid={sessionid}&user_id={user_id}&max_amount=100"

        payload = {}
        headers = {
        'Accept': 'application/json',
        'x-access-key': 'js2iYetdF7VxQmNQrr0LA8CD9oRH1gva'
        }

        response = requests.request("GET", url, headers=headers, data=payload)

        print(response.text)
        final_output =[]
        output = response.json()

        final_output=final_output+output

        return jsonify(final_output)

if __name__ == '__main__':
    app.run(host='127.0.0.1',debug=True, port=5000)
