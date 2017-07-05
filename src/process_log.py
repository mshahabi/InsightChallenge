# -*- coding: utf-8 -*-
"""
Created on Thu Jun 29 13:39:21 2017

@author: mshahabi
"""

#===========================Libraries and path adjustments

import os
import sys
import json
from math import sqrt

os.chdir(os.path.dirname(sys.argv[0]))

_ROOT, src = os.path.abspath(os.path.dirname(__file__)).split("src",1)
batch_log_path = _ROOT+ "\\sample_dataset\\batch_log.json"

#==============================================================


infile = open(batch_log_path,'r')
readingfile=[]

for line in infile:
   if len(line)>20:
     readingfile.append(json.loads(line)) 
   else:
       T=int(json.loads(line)["T"])
       D=int(json.loads(line)["D"])





##############********Average and SD function********###############
def MeanSSD(lst):
    """Calculates the standard deviation for a list of numbers."""
    num_items = len(lst)
    mean = sum(lst) / num_items
    differences = [x - mean for x in lst]
    sq_differences = [d ** 2 for d in differences]
    ssd = sum(sq_differences)
    variance = ssd / (num_items - 1)
    sd = sqrt(variance)
    return [mean,sd]



#================Find friends of a given node=======================
    
def befriend(inline,graph):
    temp = []
    if inline["id2"]=='5400':
           print(inline)

    if inline["id1"] in graph.keys() and "friends" in graph[inline["id1"]].keys() :
              
           temp=graph[inline["id1"]]["friends"]+[inline["id2"]]

           graph[inline["id1"]].update({"friends":temp})
                  
    if inline["id2"] in graph.keys() and "friends" in graph[inline["id2"]].keys() :
              
           temp=graph[inline["id2"]]["friends"]+[inline["id1"]]

           graph[inline["id2"]].update({"friends":temp})


    if (inline["id1"] not in graph.keys() or  "friends" not in graph[inline["id1"]].keys()) :

           if (inline["id1"] not in graph.keys()):
               graph[inline["id1"]]={}

               graph[inline["id1"]].update({"friends":[inline["id2"]]})
          
    if (inline["id2"] not in graph.keys() or  "friends" not in graph[inline["id2"]].keys()):

           if (inline["id2"] not in graph.keys()): 
               graph[inline["id2"]]={}

               graph[inline["id2"]].update({"friends":[inline["id1"]]})
               
    output_1 =graph[inline["id1"]]["friends"]           
    output_2 =graph[inline["id2"]]["friends"]      
    return  output_1,output_2         

#================unfriend a friend from a given node=======================
    
def unfriend(inline,graph):        
  
    temp = []
    a=inline["id1"]
    b=inline["id2"]
          
    if a in graph.keys() and "friends" in graph[inline["id1"]].keys() : 
              
         temp = graph[a]["friends"]            
         c = temp.index(b)
         del temp[c]
          
    if b in graph.keys() and "friends" in graph[inline["id2"]].keys() : 
              
         temp = graph[b]["friends"]            
         c = temp.index(a)
         del temp[c]           
    return graph
    
#=================Find the purchases for a given node======================= 
  
def purchase(inline,graph):
     temp = []
     if inline["id"] in graph.keys() and "purchase" in graph[inline["id"]].keys() :
         
         temp = float(inline["amount"])
         graph[inline["id"]].update({"purchase":graph[inline["id"]]["purchase"]+[(temp,readingfile[i]["timestamp"])]})

     if inline["id"] not in graph.keys()  :

         graph[inline["id"]]={}
         temp = float(inline["amount"])
         graph[inline["id"]].update({"purchase":[(temp,inline["timestamp"])]})               
         
     if inline["id"] in graph.keys() and "purchase" not in graph[inline["id"]].keys():

         temp = float(inline["amount"])
         graph[inline["id"]].update({"purchase":[(temp,inline["timestamp"])]})
        
     return graph[inline["id"]]["purchase"] 

#================uFind the the flagged purcheses for a given node======================= 


def FlagggedFunc(D,T,streamedData,graph):
    
#initialization
    results = None
    total_purchase = []
    current_id =[]
    item_cost = []
    
#mining the streamdata
    if streamedData["event_type"]=="purchase":  
        current_id= streamedData["id"]
        purchase_cost = streamedData["amount"]
        
        friends = graph[str(current_id)]["friends"]   
       
        totalfriends = friends
#find friends with degree of D        
        for i in range(1, D):
            friendsoffriend = []
            
            for item in friends:
               
                friendsoffriend = friendsoffriend + graph[str(item)]["friends"]
                total_purchase  = total_purchase  + graph[str(item)]["purchase"]
                totalfriends = totalfriends + friendsoffriend 
            friends = list(set(friendsoffriend))
        
#find the latest purcheses     
        reduced_total_purchase = sorted(total_purchase, key=lambda item: item[1])
        for item in reduced_total_purchase[-T:]:
             
             item_cost.append(item[0])
#calculating meand and standard deviation for a nodes social network with degree of D
        [mean,ssd] = MeanSSD(item_cost)
        if (float(purchase_cost)>mean+3*ssd):     
            streamedData.update({"std":round(ssd,2),"mean":round(mean,2)})
            results = streamedData
# update the datbase with streamed data
    if streamedData["event_type"]=="befriend":
        
       
        out1 , out2= befriend(streamedData,graph)
        data_str[streamedData["id1"]]["friends"] = out1
        data_str[streamedData["id2"]]["friends"] = out2
        
    if streamedData["event_type"]=="unfriend":
        
        out2 = unfriend(streamedData,data_str)
        data_str.update(out2)
        
    return results    
    
###################Execution##############

data_str = {}        
for i in range(0,len(readingfile)):        
        
    if readingfile[i]["event_type"]=="befriend":
        out1 , out2= befriend(readingfile[i],data_str)
        data_str[readingfile[i]["id1"]]["friends"]
        data_str[readingfile[i]["id2"]]["friends"]

    if readingfile[i]["event_type"]=="unfriend": 
        out2 = unfriend(readingfile[i],data_str)
        data_str.update(out2)  


for i in range(0,len(readingfile)):        
    if readingfile[i]["event_type"]=="purchase": 
        out3 = purchase(readingfile[i],data_str)
        data_str[readingfile[i]["id"]]["purchase"]=out3 
   
   
##==============================================================================
#==============================================================================
##A=======================Adjusting the path for streamed data
stream_log_path = _ROOT+ "\\sample_dataset\\stream_log.json"  
  
streamfile = open(stream_log_path,'r')
  
flaggedfile=[]
  
for item in streamfile:
      if len(item)> 3: 
          strjson = json.loads(item)
        
          
      flaggedpurchase=(FlagggedFunc(D,T, strjson, data_str)) 
      if flaggedpurchase != None:
          flaggedfile.append(flaggedpurchase)
#final output
flagged_log_path = _ROOT+ "\\log_output\\flagged_purchases.json"           
with open(flagged_log_path, 'w') as outfile:
    for i in range(0,len(flaggedfile)):
        json.dump(flaggedfile[i], outfile)          
#==============================================================================
# 
#==============================================================================
