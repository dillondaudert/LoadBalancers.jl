export SBLoadBalancer, parallel_lb_sb

immutable SBLoadBalancer <: AbstractLoadBalancer
    msg_chls
    stat_chl
    res_chl
    statuses
end
SBLoadBalancer(cap::Int) = SBLoadBalancer(create_msg_chls(cap),
                                          RemoteChannel(()->Channel{Message}(cap), 1),
                                          RemoteChannel(()->Channel{Message}(cap), 1),
                                          fill!(Array{Symbol}(nworkers()), :unstarted))

function parallel_lb(balancer::SBLoadBalancer, work::WorkUnit)

    Tₚ = @elapsed @sync begin

        # start the worker processes
        for i = 1:nworkers()
            @spawnat workers()[i] worker(balancer)
        end

        @async recv_results(balancer)

        @sync begin
            balancer.statuses[1] = :started

            @async put!(balancer.msg_chls[1], Message(:work, myid(), work))

            @async status_manager(balancer)
        end

        for i = 1:nworkers()
            put!(balancer.msg_chls[i], Message(:end, -1))
        end
    end
    Tₚ
end

parallel_lb_sb(cap::Int, work::WorkUnit) = parallel_lb(SBLoadBalancer(cap), work)

function status_manager(balancer::SBLoadBalancer)
    # while there are any started, nonidle nodes
    info("status_manager started")
    while any((balancer.statuses .!= :unstarted) .& (balancer.statuses .!= :idle))
        status_msg = take!(balancer.stat_chl)
        #w_idx = nprocs() > 1 ? status_msg.data - 1 : status_msg.data
        wid = status_msg.data

        if status_msg.kind == :idle
            # mark this worker as idle
            balancer.statuses[w_idx(wid)] = :idle

        elseif status_msg.kind == :nonidle
            balancer.statuses[w_idx(wid)] = :nonidle

            # look for a nonbusy worker
            w_msg_chl = balancer.msg_chls[w_idx(wid)]

            # get the indices of idle or unstarted workers
            idle_idxs = filter(x->balancer.statuses[x] != :nonidle,
                               1:nworkers())

            if length(idle_idxs) == 0
                # no idle workers
                put!(w_msg_chl, Message(:jlance, -1))
            else
                # randomly select the index of an idle worker
                idle_w_idx = rand(idle_idxs)
                balancer.statuses[idle_w_idx] = :nonidle
                put!(w_msg_chl, Message(:jlance, workers()[idle_w_idx]))
            end
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
- :nowork - Another worker failed to send work to this worker; pass an :idle
              message to the controller via stat_chl
- :end  - Pass to local_chl and exit.

Internal Messages: (Expect to receive these only from other tasks on this process)
- :_idle - Send an :idle message to the controller via stat_chl 
- :_nonidle - Send a :nonidle message to the controller via stat_chl
"""
function _msg_handler(balancer::SBLoadBalancer,
                      local_chl::Channel{Message})

    msg_chl = get_msg_chl(myid(), balancer.msg_chls)

    while true
        let msg = take!(msg_chl)
        # begin let scope
        if msg.kind == :end
            put!(local_chl, msg)
            break

        elseif msg.kind == :work
            put!(local_chl, msg)

        elseif msg.kind == :nowork
            info("worker ", myid(), " idle")
            put!(balancer.stat_chl, Message(:idle, myid()))

        elseif msg.kind == :jlance && msg.data > 0
            # attempt to load balance
            @schedule _jlancer(balancer, 
                               local_chl,
                               msg)

        elseif msg.kind == :_idle
            put!(balancer.stat_chl, Message(:idle, myid()))

        elseif msg.kind == :_nonidle
            info("worker ", myid(), " working")
            # put! on remote chl blocks, so schedule in different task
            @schedule put!(balancer.stat_chl, Message(:nonidle, myid()))
        end
        # end let scope    
        end
    end
end
