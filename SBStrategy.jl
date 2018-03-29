# subtask_handler, now with multiple workers
@printf("Num workers: %d.\n", nworkers())
print("Worker Process IDs: ", workers())

@everywhere using SubTasks

@everywhere function worker(msg_chls::Array{RemoteChannel{Channel{Message}}},
                            stat_chl::RemoteChannel{Channel{Message}})
    
    local_chl = Channel{Message}(10)
    w_idx = nprocs() > 1 ? myid() - 1 : myid()
    @printf("WORKER %D W_IDX: %d.\n", myid(), w_idx)
    my_msg_chl = msg_chls[w_idx]

    
    @printf("Worker begun.\n")
    @sync begin
        # MESSAGE HANDLER SUBTASK
        @async _msg_handler(local_chl, msg_chls, stat_chl)
        # SUBTASK 1
        @async _worker(local_chl, my_msg_chl)
    end
    @printf("Worker terminating.\n")
    put!(stat_chl, Message(:done, myid()))
end


@printf("Starting.\n")
const msg_chls = [RemoteChannel(()->Channel{Message}(20), pid) for pid in workers()]
const stat_chl = RemoteChannel(()->Channel{Message}(nworkers()), 1)
const statuses = fill!(Array{Symbol}(nworkers()), :unstarted)

function status_manager()
    @printf("Status manager started.\n")
    # while there are any started, nonidle nodes
    while any((statuses .!= :unstarted) .& (statuses .!= :idle))
        status_msg = take!(stat_chl)
        w_idx = nprocs() > 1 ? status_msg.data - 1 : status_msg.data

        if status_msg.kind == :idle
            # mark this worker as idle
            @printf("Worker %d marked as idle.\n", status_msg.data)
            statuses[w_idx] = :idle

        elseif status_msg.kind == :nonidle
            @printf("Worker %d signalling nonidle.\n", status_msg.data)
            statuses[w_idx] = :nonidle

            # look for a nonbusy worker
            w_msg_chl = msg_chls[w_idx]

            # get the indices of idle or unstarted workers
            idle_idxs = filter(x->statuses[x] != :nonidle,
                               1:nworkers())

            if length(idle_idxs) == 0
                # no idle workers
                @printf("Sending worker %d Message(:jlance, -1).\n", status_msg.data)
                put!(w_msg_chl, Message(:jlance, -1))
            else
                # randomly select the index of an idle worker
                idle_w_idx = rand(idle_idxs)
                @printf("Telling worker %d to send work to worker %d.\n", status_msg.data, workers()[idle_w_idx])
                put!(w_msg_chl, Message(:jlance, workers()[idle_w_idx]))
            end
        end
    end
end

@sync begin
    @sync begin
        statuses[1] = :started
        # create the work producer process
        @async begin
            nwork = 15
            for i = 1:nwork
                put!(msg_chls[1], Message(:work, rand(1:6)))
            end
        end
        # start the worker processes
        for i = 1:nworkers()
            @async remote_do(worker, workers()[i], msg_chls, stat_chl)
        end

        @async status_manager()
    end

    @printf("Sending :end messages to workers.\n")
    for i = 1:nworkers()
        put!(msg_chls[i], Message(:end, -1))
    end
end

@printf("Controller terminating.\n")
