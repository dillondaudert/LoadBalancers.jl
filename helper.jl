# worker helper functions

"""
Return the index of a worker in the workers() array, given its process id.
"""
w_idx(wid) = nprocs() > 1 ? wid - 1 : wid

"""
Return a reference to the message channel located on a particular worker.
"""
get_msg_chl(wid, msg_chls) = msg_chls[w_idx(wid)]

"""
Args: cap - Integer indicating the capacity of the channels.

Create nworkers() remote channels for message passing.
"""
function create_msg_chls(cap::Int)
    msg_chls = [RemoteChannel(()->Channel(cap), pid) for pid in workers()]
    msg_chls
end
