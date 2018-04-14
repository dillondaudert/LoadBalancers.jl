export ARRLoadBalancer, parallel_lb_arr

"""
Load balancing through asynchronous round robin.
"""
immutable ARRLoadBalancer <: AbstractLoadBalancer
    msg_chls::Array{RemoteChannel{Channel{Message}}}
    stat_chl::RemoteChannel{Channel{Message}}
    res_chl::RemoteChannel{Channel{Message}}
    statuses::Array{Symbol}
end
ARRLoadBalancer(cap::Integer=64) = ARRLoadBalancer(create_msg_chls(cap),
                                                 RemoteChannel(()->Channel{Message}(cap), 1),
                                                 RemoteChannel(()->Channel{Message}(cap), 1),
                                                 fill!(Array{Symbol}(nworkers()), :unstarted))


"""
    parallel_lb(balancer::ARRLoadBalancer, work::WorkUnit)

Compute the task associated with `work` using asynchronous round robin load balancing. Return
the elapsed time.
"""
function parallel_lb(balancer::ARRLoadBalancer, work::WorkUnit)

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

"""
    parallel_lb_arr(work::WorkUnit)

Compute the task associated with `work` using the default ARRLoadBalancer
"""
parallel_lb_arr(work::WorkUnit) = parallel_lb(ARRLoadBalancer(), work)


"""
    status_manager(balancer::ARRLoadBalancer)

Track worker status through update messages.
"""
function status_manager(balancer::ARRLoadBalancer)
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
    _msg_handler(balancer::ARRLoadBalancer, local_chl::Channel{Message}

Handle worker messages.

Asynchronous round robin: each worker maintains a target worker to request work from.
The target is incremented each time a request is made (mod nworkers)
"""
function _msg_handler(balancer::ARRLoadBalancer,
                      local_chl::Channel{Message})

    msg_chl = get_msg_chl(balancer)
    t = (w_idx() % nworkers()) + 1
    target_idx() = (idx=t; t%=nworkers(); t+=1; idx)
                    

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
            other_wid = workers()[target_idx()]
            #info("worker ", myid(), " requesting work from ", other_wid)
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
                other_wid = workers()[target_idx()]
                #info("worker ", myid(), " requesting work from ", other_wid)
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
