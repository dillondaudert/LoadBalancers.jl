# subtask_handler, now with multiple workers
@everywhere using SubTasks

@everywhere function worker(msg_chl, stat_chl)
    
    local_chl = Channel{Message}(10)
    idle = true
    
    @sync begin
        # MESSAGE HANDLER SUBTASK
        @async _msg_handler(local_chl, msg_chl)
        # SUBTASK 1
        @async _worker(local_chl, msg_chl)
    end
    put!(stat_chl, Message(:done, myid()))
end


@printf("Starting.\n")

function producer(msg_chl)
    # produce work for the worker given by w_idx
    nwork = 5
    for i = 1:nwork
        put!(msg_chl, Message(:work, rand(1:nwork)))
    end
    put!(msg_chl, Message(:end, -1))
end

@sync begin
    local msg_chls = [RemoteChannel(()->Channel{Message}(20), pid) for pid in workers()]
    local stat_chl = RemoteChannel(()->Channel{Message}(1), 1)
    @sync begin
        # create tasks to send messages to worker
        
        # start the worker processes
        for i = 1:nworkers()

            @async producer(msg_chls[i])

            @spawnat workers()[i] worker(msg_chls[i], stat_chl)

        end
        
    end
    @async begin
        for i = 1:nworkers()
            msg = take!(stat_chl)
            @printf("Controller received %s message.\n", string(msg))
        end
    end
end
@printf("Controller terminating.\n")
