abstract type AbstractBalancer end

# ------ worker helper functions
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


# ------ helper functions for simulating work
mutable struct WorkUnit{T<:Int}
    units::T
    unitcost::T
    WorkUnit{T}(u, c) where {T<:Int} = u < 0 || c ≤ 0 ? error("negative units or cost less than 0") : new(u, c)
end
WorkUnit(u::T, c::T) where {T<:Int} = WorkUnit{T}(u, c)

function split_work(work::WorkUnit, p::T where T<:AbstractFloat)
    """
    Split a work into two WorkUnits, u and v, according to percentage p.
    WorkUnit u will have p*units units of work, and WorkUnit v will have
    v*(1-p) units of work.
    Note that 0 < p < 1, as each resulting struct must have at least 1
    unit of work.
    """

    # check that valid percentage
    !(0 < p < 1) && (@printf("p must be: 0 < p < 1, got %g\n", p); throw(DomainError()))
    # check that there are at least 2 units
    work.units < 2 && (@printf("work must have more than 2 units\n"); throw(DomainError()))

    # set the rounding mode to up or down depending on p
    mode = p ≤ .5 ? RoundUp : RoundDown
    
    u_units = round(typeof(work.units), work.units * p, mode)
    v_units = work.units - u_units

    return WorkUnit(u_units, work.unitcost), WorkUnit(v_units, work.unitcost)

end

# generate a random percentage from the closed interval (0, 1)
split_work(work::WorkUnit) = (p = rand(1:999); split_work(work, p/1000))

# ------ Message type for passing messages between processes and tasks
struct Message
    kind::Symbol
    data::Int64
    _data2 # TODO: rename data and _data2 to more sensible names
end

Message(kind, data) = Message(kind, data, -1)

# ----- receive results utility

function recv_results(balancer::T) where {T<:AbstractBalancer}
    n_ended = 0
    res_count = 0
    total_work_done = zeros(Int64, nworkers())
    while n_ended < nworkers()
        work = take!(balancer.res_chl)
        if work.kind == :work
            res_count += 1
            w_idx = nprocs() > 1 ? work.data - 1 : work.data
            # add the work this worker did
            total_work_done[w_idx] += work._data2
            @printf("Receiving results #%d from worker %d.\n", res_count, work.data)
        elseif work.kind == :end
            n_ended += 1
        end
    end

    for w_idx in 1:nworkers()
        @printf("Worker %d did %ds of work.\n", workers()[w_idx], total_work_done[w_idx])
    end
end

# --- jlancers
"""

Attempt to send a piece of work from this worker to another. If there is no
work available, then a message indicating no work will be sent.
"""
function _jlancer(balancer::T,
                  local_chl::Channel{Message},
                  msg::Message) where {T<:AbstractBalancer}
    # attempt to move some local work to the remote worker
    other_wid = msg.data
    other_msg_chl = balancer.msg_chls[w_idx(other_wid)]
    if isready(local_chl)
        if fetch(local_chl).kind == :end
            return
        end
        work = take!(local_chl)
        put!(other_msg_chl, work)
    else
        put!(other_msg_chl, Message(:nowork, myid()))
    end
end

"""
# --- workers


Pull work from local_chl to compute, and send status
updates to msg_chl.

This worker receives two message kinds (<| local_chl):
    - :work
    - :end - This worker will exit upon receiving this message

This worker also produces two message kinds (|> msg_chl)
    - :_idle - Signal that this worker is idle (local_chl empty)
    - :_nonidle - Signal that this worker is nonidle

function _worker(balancer::RPBalancer,
                 local_chl::Channel{Message})

    idle::Bool = true
    my_msg_chl = get_msg_chl(myid(), balancer.msg_chls)

    @printf("_worker starting.\n")

    while true
        # wait for an item to be *appended* (going from i -> i+1, not necessary from 0 -> 1)
        # REMARK: Could this block indefinitely if :end arrives before this call to wait() and,
        #         since no messages are expected to arrive after :end, this waits for another
        #         value to be appended. Should we assert that the channel is empty at this point?
        # REMARK: One way of solving this could be to have the msg_handler signal on the local
        #         channels put_cond... perhaps
        @assert length(local_chl.data) == 0
        wait(local_chl)

        last_signal = time()

        # while there are messages in the local channel
        while isready(local_chl)

            local msg = take!(local_chl)

            # if this message is end, exit
            if msg.kind == :end
                put!(balancer.res_chl, Message(:end, myid()))
                return
            elseif msg.kind == :work
                # if this worker has been idle, signal it is no longer idle
                if idle
                    @printf("%d working\n", myid())
                    idle = false
                    last_signal = time()
                    put!(my_msg_chl, Message(:_nonidle, myid()))
                # if it has been 1 second since the last nonsignal message
                
                elseif time() - last_signal > 1
                    put!(my_msg_chl, Message(:_nonidle, myid()))
                    last_signal = time()
                end

                # do work
                local work = msg._data2

                if work.units > 1
                    # split the work
                    work_1, work_2 = split_work(work)

                    # place the extra work back in the local channel
                    put!(local_chl, Message(:work, myid(), work_2))
                else
                    work_1 = work
                end

                # simulate calculation
                sleep(work_1.unitcost)

                # take 1 down, pass it around
                work_1.units -= 1
                if work_1.units > 0
                    # place back in local channel
                    put!(local_chl, Message(:work, myid(), work_1))
                end

                put!(balancer.res_chl, Message(:work, myid(), work_1.unitcost))
            else
                @printf("_worker received unrecognized message! %s\n", string(msg))
                exit()
            end
        end

        # local_chl is now empty
        if !idle
            idle = true
            put!(my_msg_chl, Message(:_idle, myid()))
        end
    end
end # end _worker function
"""
