import datetime
import math
import pandas as pd

def bfs_homies(source,final_stop_id, max_arrival_time, dict_edges_bus,dict_edges_waiting,vertices,df_index,walking_neighbors,
               dict_walk_time,dict_stop_list_hours,max_iter=12,minimum_paths=5):
    
    #find the destination nodes
    arrival_time_formatted = datetime.datetime.strptime('2019-05-13'+' '+max_arrival_time, "%Y-%m-%d %H:%M:%S")
    possible_dest_nodes=vertices.loc[(vertices.time_formatted<arrival_time_formatted) & \
                                                      (vertices.station==final_stop_id),"id"]
    
    destinations=set(possible_dest_nodes.sort_values(ascending = False).values[:5]) 
    # first iteration will be to visit the destination nodes
    to_visit=destinations.copy()
    
    # Heuristic rule: a trip duration cannot exceed 2 hours
    max_time_formatted = datetime.datetime.strptime('2019-05-13'+' '+max_arrival_time, "%Y-%m-%d %H:%M:%S")
    earliest_departure_time=max_time_formatted- datetime.timedelta(minutes = 120)
    
    # finding all the nodes correspending to the source station and possible departure times 
    possible_nodes_to_visit = vertices.loc[(vertices.time_formatted>earliest_departure_time) \
                                            & (vertices.time_formatted<max_time_formatted)]
    
    possible_sources = possible_nodes_to_visit.loc[possible_nodes_to_visit.station==source,"id"]
                                           
    possible_sources = set(possible_sources.values)
    
    
    # Initialization
    true_paths=[]
    transports=[]
    trunc_paths={}
    sources_with_path=[]
    dict_paths={}
    visited_stations={}
    
    # add destination node
    visited_stations[final_stop_id]=max_arrival_time
    
    break_condition=False
    k=0   #iteration counter
    while((not break_condition)&(k<max_iter)):
        k+=1

        # deep copy of the nodes to visit
        to_visit_tmp=to_visit.copy()
        to_visit=set()
       
        for node in to_visit_tmp:
            
            to_visit_next_from_node=set()
            
            stop_id, time = node.split('_')
            visited_stations[stop_id]=time
            node_transport="waiting"
            if (stop_id !=final_stop_id):
                node_transport=dict_paths[node][1]
            
            
            ################################ WALKING ######################
            
            #avoid double walking
            if (node_transport!="walking"):

                time_formatted = datetime.datetime.strptime('2019-05-13'+' '+time, "%Y-%m-%d %H:%M:%S")
                index_node = df_index.loc[stop_id,'index'] 
                walking_nodes=walking_neighbors[stop_id]
                for station_id_other in walking_nodes:  #iterate over the other nodes 

                    walking_time = datetime.timedelta(minutes = dict_walk_time[(stop_id, station_id_other)] )
                    departure_time = time_formatted - walking_time

                    hours_other = dict_stop_list_hours[station_id_other]
                    possible_changes = list(filter( (lambda x : (x < departure_time) &(earliest_departure_time<x)) , \
                                            map(lambda x: datetime.datetime.strptime('2019-05-13'+' '+x,"%Y-%m-%d %H:%M:%S"),\
                                                hours_other) ))

                    if(len(possible_changes) != 0):
                                                
                        first_hour_change = max(possible_changes)               
                        #destination node
                        first_hour_change_str = first_hour_change.strftime('%H:%M:%S')
                        node_id_other = station_id_other+'_'+first_hour_change_str

                        time_transfer = (first_hour_change - time_formatted).total_seconds()

                        # add the node
                        if (not (node_id_other in to_visit_tmp)):
                            # check if the we reached the station before
                            if station_id_other in visited_stations.keys():
                                # if we reached it at a better time update it
                                if first_hour_change_str>visited_stations[station_id_other]:
                                    visited_stations[station_id_other]=first_hour_change_str
                                    to_visit_next_from_node.add(node_id_other)
                            else:
                                to_visit_next_from_node.add(node_id_other)

                        # if the path is arleady added, we don't update since we favoritize bus transport    
                        if not (node_id_other in dict_paths.keys()):

                            dict_paths[node_id_other]=[node,"walking",stop_id] # node=stop_id+time

            #################### Bus ################
            if node in dict_edges_bus.keys():

                bus_nodes=dict_edges_bus[node]

                for bus_node in bus_nodes:
                    bus_node_id,bus_node_time=bus_node.split('_')
                    
                    if bus_node_id in visited_stations.keys():
                        # if we reached it at a better time update it
                        if bus_node_time>visited_stations[bus_node_id]:
                            visited_stations[bus_node_id]=bus_node_time
                            to_visit_next_from_node.add(bus_node)
                            dict_paths[bus_node]=[node,"bus",stop_id]
                    else:
                        to_visit_next_from_node.add(bus_node)
                        dict_paths[bus_node]=[node,"bus",stop_id]        
                    # here we always update since this can only be better
                    

            #################### Correspendance ##############
            if node in dict_edges_waiting.keys():
                waiting_nodes=dict_edges_waiting[node]
                to_visit_next_from_node.update(waiting_nodes)
                
                # here we always update since this can only be better
                for waiting_node in waiting_nodes:
                    waiting_node_id,waiting_node_time=waiting_node.split('_')
                    
                    if waiting_node_id in visited_stations.keys():
                        # if we reached it at a better time update it
                        if waiting_node_time>visited_stations[waiting_node_id]:
                            visited_stations[waiting_node_id]=waiting_node_time
                            
                    dict_paths[waiting_node]=[node,"waiting",stop_id]
            
            # check if new nodes are valid
            valid_nodes = filter((lambda x :(earliest_departure_time<datetime.datetime.strptime('2019-05-13'+' '+x.split('_')[1],
                                                                                               "%Y-%m-%d %H:%M:%S"))) , \
                                            to_visit_next_from_node)
                      
            to_visit_next_from_node=set()                
            for new_node in valid_nodes:
                
                if (new_node in possible_sources):
                    
                    # reconstruct the path
                    double_walk=False # for not having two consecutive walking edges
                    new_node_trans=dict_paths[new_node][1]
                    last_transport=new_node_trans
                    correct_path=[new_node]
                    trans=[last_transport]
                    trunc_path=[]
                    trunc_trans=[]
                    backprop=node
                    current_station=dict_paths[new_node][2]
                    path_stations=set()
                    path_stations.add(current_station)
                    loop=False # for not going to the same station twice
                    nb=0 #number of hops, it shouldn't exceed 200
                    while ((current_station!=final_stop_id)&(nb<200)&(not double_walk)&(not loop)):
                        nb+=1
                        node_meta=dict_paths[backprop]
                        current_station=node_meta[2]
                        
                        if (current_station in path_stations):
                            loop=True
                        else:
                            if ((node_meta[1]==last_transport=="walking")):
                                double_walk=True
                            else:
                                path_stations.add(current_station)
                                correct_path.append(backprop)
                                trunc_path.append(backprop)
                                
                                backprop=node_meta[0]
                                
                                last_transport=node_meta[1]
                                trans.append(last_transport)
                                trunc_trans.append(last_transport)
                                
                    if(nb==200):
                        print("Maximum hops reached at: ",correct_path)
                    if((not double_walk)&(not loop)):
                        trunc_key=tuple(trunc_path)
                        if (trunc_key in trunc_paths.keys()):
                            other_source=trunc_paths[trunc_key]
                            new_time=new_node.split("_")[1]
                            other_time=other_source.split("_")[1]
                            if new_time>other_time:
                                
                                # remove the old path
                                path_to_remove=trunc_path[:]
                                path_to_remove.insert(0,other_source)
                                path_to_remove.append(backprop)
                                
                                # remove the old transport
                                other_transport=dict_paths[other_source][1]
                                trans_to_remove=trunc_trans[:]
                                trans_to_remove.insert(0,other_transport)
                                trans_to_remove.append(last_transport)
                                if path_to_remove in true_paths:
                                    print("Old path starts at {}, better start at {} ".format(other_time,new_time))
                                    true_paths.remove(path_to_remove)
                                    transports.remove(trans_to_remove)
                                else:
                                    print("can't find path:", path_to_remove)
                                # update dict
                                trunc_paths[trunc_key]=new_node
                                # add updated path
                                correct_path.append(backprop)
                                trans.append(last_transport)
                                true_paths.append(correct_path)
                                transports.append(trans)
                                sources_with_path.remove(other_source)
                                sources_with_path.append(new_node)
                                
                        else:        
                            trunc_paths[trunc_key]=new_node
                            correct_path.append(backprop)
                            trans.append(last_transport)
                            true_paths.append(correct_path)
                            transports.append(trans)
                            sources_with_path.append(new_node)
                            print("found a new path at iteration ",k)
                            
                else:
                    to_visit_next_from_node.add(new_node)
                  
  
            # don't add nodes that we are currently exploring
            to_visit_next_from_node=to_visit_next_from_node-to_visit_tmp
            
            # cumulating nodes
            to_visit.update(to_visit_next_from_node)
        if (len(to_visit)>0):
            selected_nodes=pd.DataFrame(list(map(lambda x: x.split("_"),(to_visit)))). \
                                groupby(0).max().reset_index().apply(lambda x: x[0]+"_"+x[1],axis=1).values
            
            print("at iteration {} we are visiting {} nodes that corresponds to {} unique stations".
                  format(k,len(to_visit),len(selected_nodes)))
        else:
            # break if the all the graph is visited
            break_condition=True
            
        #heuristic rule : limit the minimum number of paths
        if (len(sources_with_path) >= min(minimum_paths,len(possible_sources))) :
            earliest_time=sorted(sources_with_path)[0].split("_")[1]
            earliest_formatted = datetime.datetime.strptime('2019-05-13'+' '+earliest_time, "%Y-%m-%d %H:%M:%S")
            if earliest_formatted>earliest_departure_time:
                print("update time")
                earliest_departure_time=earliest_formatted
              
    print("completed after {} iterations. Constructed tree with {} nodes. Found {} paths".
          format(k,len(dict_paths),len(true_paths)))
    
    if(len(sources_with_path) == 0):
        print("No path found, try to increase BFS's depth.")
    else: 
        earliest_time = sorted(sources_with_path)[0].split("_")[1]
        earliest_formatted = datetime.datetime.strptime('2019-05-13'+' '+earliest_time, "%Y-%m-%d %H:%M:%S")
        if ((earliest_formatted==earliest_departure_time)|(len(to_visit)>0)):
            print("If you can be more patient we can get you more paths by increasing the minimum path parameter")
        else:
            print("That's all what we can find")
    return true_paths ,transports          

def get_safe_paths(true_paths,transports,max_time_arrival,threshold,df_means, dict_walk_time):
    max_time_arrival_formatted = datetime.datetime.strptime('2019-05-13'+' '+max_time_arrival, "%Y-%m-%d %H:%M:%S")

    path_safe = []
    safe_probas=[]
    for path,transport in zip(true_paths, transports) : 

        proba_success_transfer = 1

        if(len(path) == 1):
            pritn('Error need at least 2 stations !')
        elif(len(path) == 2):

            now = path[0]
            after = path[1]
            transport_now_after = transports[0]

            if(transport == 'bus'):
                station_now, time_now = now.split('_')
                station_after, time_after = after.split('_')

                hour_now = int(time_now[0:2])
                hour_after = int(time_after[0:2])

                mean = df_means.loc[(df_means['stop_id_prev'] == station_now) & (df_means['arrival_prev_hour'] == hour_now) 
                                          & (df_means['stop_id_next'] == station_after) & (df_means['arrival_next_hour'] == hour_after),'mean_val'].values
                

                if(mean.isEmpty()):
                    lambda_ = 1e12  #equivlaent to infinity
                else : 
                    lambda_ = 1/max(mean[0], 1e-12)

                proba_success_transfer = proba_success_transfer * (1 - math.exp(-transfer_time * lambda_))

        else :

            for i in range(len(path)):  

                if (i <= len(path) - 3) : 

                    prev = path[i]
                    now = path[i+1]
                    after = path[i+2]
                    transport_now_after = transport[i+1]
                    destination = (i==(len(path)-3))

                    station_prev, time_prev = prev.split('_')
                    station_now, time_now = now.split('_')
                    station_after, time_after = after.split('_')

                    hour_prev = int(time_prev[0:2])
                    hour_now = int(time_now[0:2])

                    mean = df_means.loc[(df_means['stop_id_prev'] == station_prev) & (df_means['arrival_prev_hour'] == hour_prev) 
                                          & (df_means['stop_id_next'] == station_now) & (df_means['arrival_next_hour'] == hour_now),'mean_val'].values

                    if(mean.size == 0):
                        lambda_ = 1e12   #equivlaent to infinity
                    else : 
                        lambda_ = 1/max(mean[0], 1e-12)


                    time_prev_formatted = datetime.datetime.strptime('2019-05-13'+' '+time_prev, "%Y-%m-%d %H:%M:%S")
                    time_now_formatted = datetime.datetime.strptime('2019-05-13'+' '+time_now, "%Y-%m-%d %H:%M:%S")
                    time_after_formatted = datetime.datetime.strptime('2019-05-13'+' '+time_after, "%Y-%m-%d %H:%M:%S")

                    if(transport_now_after == 'waiting'):
                        transfer_time = (time_after_formatted - time_now_formatted)

                        if not destination:
                            transfer_time=transfer_time - datetime.timedelta(minutes = 2)

                        transfer_time=transfer_time.total_seconds()
                        proba_success_transfer = proba_success_transfer * (1 - math.exp(-transfer_time * lambda_))

                    elif(transport_now_after == 'walking'):
                        transfer_time = ( (time_after_formatted - time_now_formatted) 
                                         - datetime.timedelta(minutes = float(dict_walk_time[(station_now,station_after)])) )

                        if destination:
                            transfer_time = transfer_time + datetime.timedelta(minutes = 2)
                        transfer_time = transfer_time.total_seconds()
                        proba_success_transfer = proba_success_transfer * (1 - math.exp(-transfer_time * lambda_))

                    elif (destination  &  (transport_now_after == 'bus')):

                        transfer_time = max_time_arrival_formatted - time_after_formatted
                        transfer_time = transfer_time.total_seconds()


        if (threshold/100.0 < proba_success_transfer): 
            safe_probas.append(proba_success_transfer)
            path_safe.append([path,transport[:-1]])
    return path_safe , safe_probas

def get_final_output(path_safe,safe_probas):
    initial_node=[]
    for path in path_safe:
        initial_node.append(path[0][0])

    ordered_nodes=sorted(initial_node)[::-1]
    ordered_safe_paths=[]
    for i in range(len(ordered_nodes)):
        ordered_safe_paths.append(path_safe[initial_node.index(ordered_nodes[i])])


    final_output=[]
    for path ,transport in ordered_safe_paths:
        final_path=[]
        i=0
        taking_bus=False
        while (i<len(transport)):
            if not(taking_bus):
                final_path.append(path[i].split("_"))
                final_path.append(transport[i])
                if transport[i]=="bus":
                    taking_bus=True
            else:
                if transport[i]!="bus":
                    taking_bus=False
                    final_path.append(path[i].split("_"))
                    final_path.append(transport[i])
            i+=1

        final_path.append(path[-1].split("_"))
        final_output.append(final_path)


    output_messages=[]
    for index ,path in enumerate(final_output):

        message='- Path number {} ({:0.2f}% certainty):\n  Be at the starting station before {}.\n'.format(
            index+1,100*safe_probas[index],path[0][1])
        for i in range(1,len(path),2):
            if path[i]=="bus":
                message+="  Take the bus from {} at {} to arrive to {} at {}\n".format(
                    path[i-1][0],path[i-1][1],path[i+1][0],path[i+1][1])
            elif path[i]=="walking":
                message+="  Walk from {} to catch the bus at {} at {}\n".format(
                path[i-1][0],path[i+1][0],path[i+1][1])
            elif path[i]=="waiting":
                message+="Wait at station {} to take the bus at {}\n".format(
                path[i-1][0],path[i+1][1])
        message+="You will arrive at your destination at {}".format(path[len(path)-1][1]) 
        output_messages.append(message)
    return final_output , output_messages