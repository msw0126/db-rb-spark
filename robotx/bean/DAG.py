import networkx as nx
from robotx.bean.Beans import *
import copy

def reversEdgeList(edge_list):
    # reverse a list of edge, reverse both the orientation of list and edge
    # e.g. [(1,2), (2,3)] to [(3,2), (2,1)]
    res = []
    if(len(edge_list) > 0):
        res = [(x[1], x[0]) for x in edge_list]
        res.reverse()
    return res

def getPath(G, source=None):
    # G: the graph of which the edge is a Relation(targetTable.name, sourceTable.name)
    # return the execution path, list of tuple<str, str>
    # source is a list of node name(str) from where dfs starts.
    if(source == None):
        source = findGraphHead(G)
    else:
        if(source != findGraphHead(G)):
            raise ValueError("The input head is not the same as the real head of Graph. "
                             "head is the node which has no parent. ")
    return reversEdgeList(list(nx.edge_dfs(G, source=[source], orientation='original')))

def generateReversedGraph(relationList):
    G = nx.DiGraph()
    for x in relationList:
        tail = x.target_table.name
        head = x.source_table.name
        G.add_edge(tail, head)
    return G

def getIndex(srcName, tgName, relationList):
    # use target table name and source table name to find the index of relation in relationList
    res = -1
    for n in range(len(relationList)):
        if(tgName == relationList[n].target_table.name and srcName == relationList[n].source_table.name):
            res = n
            break
    return res

def getTable(tableName, relationList):
    # retrieve the target table from relation in relationList
    # of which the name is equal to tableName
    for x in relationList:
        tgTableName = x.target_table.name
        if(tableName == tgTableName):
            return x.target_table
    raise ValueError("tableName is not found in relationList. ")

def getSortedRelationList(relationList, source=None):
    # 1.generate graph according to relationList
    # 2.get path of the graph
    # 3.according to the path, get a sorted relationList, the new order of list is the order of execution
    # arg source is not used, maybe it will be used in future
    relationList = copy.deepcopy(relationList)
    G = generateReversedGraph(relationList)
    head = findGraphHead(G)
    tgTable = getTable(head, relationList)     # tgTable: Table, target table in relationList
    pathRl = getPath(G, source=head)
    resList = []

    if(len(pathRl) != len(relationList)):
        raise Exception("The length of path is not equal to the length of relationList.")
    for n in range(len(pathRl)):
        tgName = pathRl[n][1]
        srcName = pathRl[n][0]
        i = getIndex(srcName=srcName, tgName=tgName, relationList=relationList)
        resList.append(relationList[i])
        relationList.remove(relationList[i])
    return resList, tgTable

def findGraphHead(G):
    # G is a directed graph
    # return the node which has no parent
    # raise exception if there is zero or more than one node that has no parent
    nodes = G.nodes()
    for x in G.edges_iter():
        tail = x[0]
        head = x[1]
        if(head in nodes):
            nodes.remove(head)
    if(len(nodes) == 0):
        raise ValueError("There is zero head in graph. Please check the graph.")
    if(len(nodes) > 1):
        raise ValueError("There are more than one head in graph. Please check the graph.")
    return nodes[0]

def printRelationList(ll):
    """   
    :param ll: 
    :return: 
    """
    for n in range(len(ll)):
        print(ll[n].source_table.name,",",ll[n].target_table.name)
