# subtask_handler, now with multiple workers
@printf("Num workers: %d.\n", nworkers())
print("Worker Process IDs: ", workers())

@everywhere using SubTasks

@everywhere function worker(msg_chl, stat_chl)
    
    local_chl = Channel{Message}(10)
    idle = true
    
    @printf("Worker begun.\n")
    @sync begin
        # MESSAGE HANDLER SUBTASK
        @async _msg_handler(local_chl, msg_chl)
        # SUBTASK 1
        @async _worker(local_chl, msg_chl)
    end
    @printf("Worker terminating.\n")
    put!(stat_chl, Message(:done, myid()))
end


@printf("Starting.\n")
const msg_chls = [RemoteChannel(()->Channel{Message}(20), pid) for pid in workers()]
const stat_chl = RemoteChannel(()->Channel{Message}(1), 1)

function producer(w_idx)
    # produce work for the worker given by w_idx
    nwork = 8
    for i = 1:nwork
        @printf("Sending work #%d to worker %d.\n", i, workers()[w_idx])
        put!(msg_chls[w_idx], Message(:work, rand(1:nwork)))
    end
    put!(msg_chls[w_idx], Message(:end, -1))
end

@sync begin
    # create tasks to send messages to worker
    
    # start the worker processes
    for i = 1:nworkers()

        @async producer(i)

        @async remote_do(worker, workers()[i], msg_chls[i], stat_chl)

    end
    
    
end
@sync begin
    @async begin
        for i = 1:nworkers()
            msg = take!(stat_chl)
            @printf("Controller received %s message.\n", string(msg))
        end
    end
end
@printf("Controller terminating.\n")
