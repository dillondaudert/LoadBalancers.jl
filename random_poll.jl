export RPBalancer, parallel_lb_rp

immutable RPBalancer <: AbstractBalancer
    msg_chls    # message channels
    stat_chl    # status channel
    res_chl     # results channel
    statuses    # current worker statuses
end
RPBalancer(cap::Int) = RPBalancer(create_msg_chls(cap),
                                  RemoteChannel(()->Channel{Message}(cap), 1),
                                  RemoteChannel(()->Channel{Message}(cap), 1),
                                  fill!(Array{Symbol}(nworkers()), :unstarted))


function parallel_lb(balancer::RPBalancer, work::WorkUnit)
    # TODO: Separate behavior for single process

    Tₚ = @elapsed @sync begin
        # start the worker processes
        for wid in workers()
            @spawnat wid worker(balancer)
        end

        @async recv_results(balancer)

        @sync begin
            balancer.statuses[1] = :started

            # send initial work
            @async put!(balancer.msg_chls[1], Message(:work, myid(), work))

            @async status_manager(balancer)
            
        end

        for i = 1:nworkers()
            put!(balancer.msg_chls[i], Message(:end, -1))
        end
    end
    Tₚ
end

parallel_lb_rp(cap::Int, work::WorkUnit) = parallel_lb(RPBalancer(cap), work)

function worker(balancer::RPBalancer)
    
    local_chl = Channel{Message}(10)
    
    @sync begin
        # MESSAGE HANDLER SUBTASK
        @async _msg_handler(balancer, local_chl)
        # SUBTASK 1
        @async _worker(balancer, local_chl)
    end
    put!(balancer.stat_chl, Message(:done, myid()))
end

function status_manager(balancer::RPBalancer)
    # while there are any started, nonidle nodes
    if nworkers() > 1
        @printf("status_manager is waking up idle workers:\n")
        for w_idx in 2:nworkers()
            @printf("\t... %d\n", workers()[w_idx])
            put!(balancer.msg_chls[w_idx], Message(:_idle, myid()))
        end
    end
    while any((balancer.statuses .!= :unstarted) .& (balancer.statuses .!= :idle))
        status_msg = take!(balancer.stat_chl)

        #w_idx = nprocs() > 1 ? status_msg.data - 1 : status_msg.data
        wid = status_msg.data

        if status_msg.kind == :idle
            # mark this worker as idle
            balancer.statuses[w_idx(wid)] = :idle

        elseif status_msg.kind == :nonidle
            balancer.statuses[w_idx(wid)] = :nonidle
            
        else
            error("Invalid message received by status_manager")

        end
    end
end


"""
Receive messages on its remote channel. Depending on the message,
different actions will be taken:

External Messages: (Could come from anywhere)
- :work - Pass from the remote channel to local_chl
- :jlance - Start a new task that will attempt to send work in local_chl
              to the remote worker specified by this message
- :nowork - Another worker failed to send work to this worker; request
              work from a random worker.
- :end  - Pass to local_chl and exit.

Internal Messages: (Expect to receive these only from other tasks on this process)
- :_idle - Send an :idle message to the controller via stat_chl; 
             Request work from a random worker.
- :_nonidle - Send a :nonidle message to the controller via stat_chl
"""
function _msg_handler(balancer::RPBalancer,
                      local_chl::Channel{Message})

    msg_chl = get_msg_chl(myid(), balancer.msg_chls)

    while true
        let msg = take!(msg_chl)
        # start soft local scope 
        if msg.kind == :end
            put!(local_chl, msg)
            break

        elseif msg.kind == :work
            put!(local_chl, msg)

        elseif msg.kind == :nowork
            put!(balancer.stat_chl, Message(:idle, myid()))
            other_wid = rand(workers())
            @printf("Requesting work from %d.\n", other_wid)
            other_msg_chl = get_msg_chl(other_wid, balancer.msg_chls)
            put!(other_msg_chl, Message(:jlance, myid()))

        elseif msg.kind == :jlance && msg.data > 0
            # attempt to load balance
            @schedule _jlancer(balancer, local_chl, msg)

        elseif msg.kind == :_idle
            put!(balancer.stat_chl, Message(:idle, myid()))
            # if there is only 1 worker, there is no one else to get work from
            #    and, if the only worker is idle, that means we should finish
            @printf("Worker %d idle.\n", myid())
            if nworkers() > 1
                other_wid = rand(workers())
                @printf("Requesting work from %d.\n", other_wid)
                other_msg_chl = get_msg_chl(other_wid, balancer.msg_chls)
                put!(other_msg_chl, Message(:jlance, myid()))
            end

        elseif msg.kind == :_nonidle
            # put! on remote chl blocks, so schedule in different task
            @schedule put!(balancer.stat_chl, Message(:nonidle, myid()))
        end
        # end soft local scope
        end
    end
end


# --- jlancers
"""

Attempt to send a piece of work from this worker to another. If there is no
work available, then a message indicating no work will be sent.
"""
function _jlancer(balancer::RPBalancer,
                  local_chl::Channel{Message},
                  msg::Message)
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


# --- workers
"""
Pull work from local_chl to compute, and send status
updates to msg_chl.

This worker receives two message kinds (<| local_chl):
    - :work
    - :end - This worker will exit upon receiving this message

This worker also produces two message kinds (|> msg_chl)
    - :_idle - Signal that this worker is idle (local_chl empty)
    - :_nonidle - Signal that this worker is nonidle
"""
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
