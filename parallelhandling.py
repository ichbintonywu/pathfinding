from neo4j import GraphDatabase
import queue
import datetime
import threading
import random
import multiprocessing as mp

# configuration part
constr = "bolt://172.16.142.6:7687"
username = "neo4j"
password = "password"
dbname = "shortestpath"

# use one session for all threads
neo4j_db = GraphDatabase.driver(constr, auth=(username, password));
session = neo4j_db.session(database=dbname)


# use multiple connection for threads
def get_session():
    neo4j_db = GraphDatabase.driver(constr, auth=(username, password));
    session = neo4j_db.session(database=dbname)
    return session


# first query result to return pairs for the further query
def get_node_pair(tx):
    query = """
       MATCH (p:City:Hero),(q:City:Hero) where id(p) <>id(q)
       return p.name as p,q.name as q
       """
    result = tx.run(query)
    values_map = {}
    tmpmap = {}
    key = 0
    for record in result:
        tmpmap[record[0]] = record[1]
        values_map[key] = tmpmap
        key = key + 1
        tmpmap = {}
    return values_map


# second query to fetch the path count or path
def get_paths_r(tx, start_name, end_name):
    query = """
            MATCH (m1:Hero:City {name:""" + "\'" + start_name + "\'" + """})
            CALL apoc.path.expandConfig(m1, {
	        relationshipFilter: ">CONNECTED_TO",
            minLevel: 0,
            maxLevel: 6,
            bfs: false
            })
            yield path
            with path,nodes(path)[-1] as n1 where n1.name = """ + "\'" + end_name + "\'" + """ 
            RETURN count(path) AS counts
            //return path
            ORDER BY counts;
            """
    result = tx.run(query)
    values = []
    for record in result:
        values.append(record.values())
    return values


def thread_with_pairs(start, end):
    start_time = datetime.datetime.now()
    with session.begin_transaction() as tx:
        the_result_data = get_paths_r(tx, start, end)
    end_time = datetime.datetime.now()
    print(end_time - start_time, the_result_data)


def thread_with_pairs_withqueue(start, end, out_queue):
    mysession = get_session()
    start_time = datetime.datetime.now()
    with session.begin_transaction() as tx:
        the_result_data = get_paths_r(tx, start, end)
    end_time = datetime.datetime.now()
    print(end_time - start_time, the_result_data[0])
    out_queue.put(the_result_data[0][0])


def main_task(map_list):
    allstartnodes = list(map_list.keys())
    random.shuffle(allstartnodes)

    # threadLock = threading.Lock()
    threads = list()
    que = queue.Queue()
    for valuemap in allstartnodes:
        key_list = list(map_list[valuemap].keys())
        value_list = list(map_list[valuemap].values())
        start = key_list[0]
        end = value_list[0]
        # t = threading.Thread(target=thread_with_pairs(start, end))
        t = threading.Thread(target=thread_with_pairs_withqueue(start, end, que), args=((start, end)))
        threads.append(t)
        # t.start()

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    allresult = 0
    while not que.empty():
        result = que.get()
        allresult = allresult + int(result)

    print("The result of total path is: " + str(allresult))


def mp_main_task(map_list):
    allstartnodes = list(map_list.keys())
    random.shuffle(allstartnodes)

    mp.set_start_method('spawn')
    que = mp.Queue()
    # p = mp.Process(target=foo, args=(q,))
    # p.start()
    # print(q.get())
    # p.join()

    for valuemap in allstartnodes:
        key_list = list(map_list[valuemap].keys())
        value_list = list(map_list[valuemap].values())
        start = key_list[0]
        end = value_list[0]
        # t = threading.Thread(target=thread_with_pairs(start, end))
        p = mp.Process(target=thread_with_pairs_withqueue(start, end, que), args=((start, end),))
        p.start()
        p.join()

    allresult = 0
    while not que.empty():
        result = que.get()
        allresult = allresult + int(result)

    print("The result of total path is: " + str(allresult))


if __name__ == "__main__":
    with session.begin_transaction() as tx:
        pair_values_map = get_node_pair(tx)
    # {0:{'Tula': 'Minsk'}, 1:{'Smolensk': 'Murmansk'},
    #             2:{'Kiev': 'Sevastopol'}}
    portList = pair_values_map
    start_time = datetime.datetime.now()
    main_task(portList)
    # mp_main_task(portList)
    end_time = datetime.datetime.now()
    print("Total time: " + str(end_time - start_time))
