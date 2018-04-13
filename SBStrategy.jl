# Scheduler-Based adaptive load balancing strategy
# OVERVIEW
# The controller acts as a scheduler and keeps an idle-status list
# - Worker i alerts the controller that it is non-idle
# - The controller sends the id of an idle worker j to worker i
#     and sets the status of j to non-idle
# - Worker i will then send work to j, or a message that it has 
#     no available work
# - Worker j then begins working or sends a message to the 
#     controller that it is idle.

module SBController
using WorkUnits
using SubTasks

export sb_controller

function worker(msg_chls::Array{RemoteChannel{Channel{Message}}},
                stat_chl::RemoteChannel{Channel{Message}},
                res_chl::RemoteChannel{Channel{Message}})
    
    local_chl = Channel{Message}(10)
    w_idx = nprocs() > 1 ? myid() - 1 : myid()
    my_msg_chl = msg_chls[w_idx]

    
    @sync begin
        # MESSAGE HANDLER SUBTASK
        @async _msg_handler(local_chl, msg_chls, stat_chl)
        # SUBTASK 1
        @async _worker(local_chl, my_msg_chl, res_chl)
    end
    put!(stat_chl, Message(:done, myid()))

    #Profile.print(combine=true, mincount=80)
end

function status_manager(msg_chls::Array{RemoteChannel{Channel{Message}}},
                        stat_chl::RemoteChannel{Channel{Message}},
                        statuses::Array{Symbol})
    # while there are any started, nonidle nodes
    @printf("status_manager started\n")
    while any((statuses .!= :unstarted) .& (statuses .!= :idle))
        status_msg = take!(stat_chl)
        w_idx = nprocs() > 1 ? status_msg.data - 1 : status_msg.data

        if status_msg.kind == :idle
            # mark this worker as idle
            statuses[w_idx] = :idle

        elseif status_msg.kind == :nonidle
            statuses[w_idx] = :nonidle

            # look for a nonbusy worker
            w_msg_chl = msg_chls[w_idx]

            # get the indices of idle or unstarted workers
            idle_idxs = filter(x->statuses[x] != :nonidle,
                               1:nworkers())

            if length(idle_idxs) == 0
                # no idle workers
                put!(w_msg_chl, Message(:jlance, -1))
            else
                # randomly select the index of an idle worker
                idle_w_idx = rand(idle_idxs)
                statuses[idle_w_idx] = :nonidle
                put!(w_msg_chl, Message(:jlance, workers()[idle_w_idx]))
            end
        end
    end
end

function recv_results(res_chl)
    n_ended = 0
    res_count = 0
    total_work_done = zeros(Int64, nworkers())
    while n_ended < nworkers()
        work = take!(res_chl)
        if work.kind == :work
            res_count += 1
            w_idx = nprocs() > 1 ? work.data - 1 : work.data
            # add the work this worker did
            total_work_done[w_idx] += work._data2
        elseif work.kind == :end
            n_ended += 1
        end
    end

    for w_idx in 1:nworkers()
        @printf("Worker %d did %ds of work.\n", workers()[w_idx], total_work_done[w_idx])
    end
end

# measure total time to finish calculation
function sb_controller(work::WorkUnit)
    msg_chls = [RemoteChannel(()->Channel{Message}(32*nworkers()), pid) for pid in workers()]
    stat_chl = RemoteChannel(()->Channel{Message}(32), 1)
    res_chl = RemoteChannel(()->Channel{Message}(32), 1)
    statuses = fill!(Array{Symbol}(nworkers()), :unstarted)
    Tₚ = @elapsed @sync begin

        # start the worker processes
        for i = 1:nworkers()
            @spawnat workers()[i] worker(msg_chls, stat_chl, res_chl)
        end

        @async recv_results(res_chl)

        @sync begin
            statuses[1] = :started

            @async put!(msg_chls[1], Message(:work, myid(), work))

            @async status_manager(msg_chls, stat_chl, statuses)
        end

        for i = 1:nworkers()
            put!(msg_chls[i], Message(:end, -1))
        end
    end
    Tₚ
end
    
end 