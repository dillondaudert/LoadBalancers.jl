# An example of a message handler passing messages to the
#   correct subtasks, and updating the state.

# A message handler waits for messages. Depending on the type
#   of message, it routes it to the appropriate location.
#   - :work message - forward to local channel
#   - :end message  - forward to local channel, exit
#
# The worker subtask will take messages off the local channel.
#   If there are no messages, it will put an :idle message
#   in the msg_chl. Once a :work message arrives, it will
#   put a :nonidle message in the msg_chl and do work.
#   If an :end message arrives, it will terminate.

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
const msg_chl = RemoteChannel(()->Channel{Message}(10), workers()[1])
const stat_chl = RemoteChannel(()->Channel{Message}(1), 1)

@sync begin
    # create task to send messages to worker
    @async begin
        @printf("Controller work producer begun.\n")
        nwork = 10
        for i = 1:nwork
            put!(msg_chl, Message(:work, rand(1:2i)))
        end
        put!(msg_chl, Message(:end, -1))
    end
    
    # start the worker process
    #@spawnat workers()[1] worker(msg_chl)
    @async remote_do(worker, workers()[1], msg_chl, stat_chl)
    
    @async begin
        msg = take!(stat_chl)
        @printf("Controller received %s message.\n", string(msg))
    end
    
end
