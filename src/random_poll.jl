export RPLoadBalancer, parallel_lb_rp

immutable RPLoadBalancer <: AbstractLoadBalancer
    msg_chls    # message channels
    stat_chl    # status channel
    res_chl     # results channel
    statuses    # current worker statuses
end
RPLoadBalancer(cap::Int) = RPLoadBalancer(create_msg_chls(cap),
                                          RemoteChannel(()->Channel{Message}(cap), 1),
                                          RemoteChannel(()->Channel{Message}(cap), 1),
                                          fill!(Array{Symbol}(nworkers()), :unstarted))


function parallel_lb(balancer::RPLoadBalancer, work::WorkUnit)
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

parallel_lb_rp(cap::Int, work::WorkUnit) = parallel_lb(RPLoadBalancer(cap), work)

function status_manager(balancer::RPLoadBalancer)
    # while there are any started, nonidle nodes
    if nworkers() > 1
        info("status_manager is waking up idle workers...")
        for w_idx in 2:nworkers()
            info("\t... ", workers()[w_idx])
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
function _msg_handler(balancer::RPLoadBalancer,
                      local_chl::Channel{Message})

    msg_chl = get_msg_chl(balancer)

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
            other_msg_chl = get_msg_chl(balancer, other_wid)
            put!(other_msg_chl, Message(:jlance, myid()))

        elseif msg.kind == :jlance && msg.data > 0
            # attempt to load balance
            @schedule send_work(balancer, local_chl, msg)

        elseif msg.kind == :_idle
            put!(balancer.stat_chl, Message(:idle, myid()))
            # if there is only 1 worker, there is no one else to get work from
            #    and, if the only worker is idle, that means we should finish
            info("worker ", myid(), " idle")
            if nworkers() > 1
                other_wid = rand(workers())
                other_msg_chl = get_msg_chl(balancer, other_wid)
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
