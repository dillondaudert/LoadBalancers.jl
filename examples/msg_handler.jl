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

addprocs(1)

@everywhere struct Message
    name::Symbol
    id::Int
end

const msg_chl = RemoteChannel(()->Channel{Message}(10), workers()[1])
const stat_chl = RemoteChannel(()->Channel{Message}(1), 1)

@everywhere function worker(msg_chl, stat_chl)
    
    _local_chl = Channel{Message}(10)
    idle = true
    
    @sync begin
        @printf("Worker begun.\n")
        
        # MESSAGE HANDLER SUBTASK
        @async begin
            @printf("Message Handler begun.\n")
            while true
                msg = take!(msg_chl)
                
                @printf("Handler received %s message.\n", string(msg))
                
                # handle :end
                if msg.name == :end
                    put!(_local_chl, msg)
                    break
                # handle :work
                elseif msg.name == :work
                    put!(_local_chl, msg)
                                        
                # handle :idle
                elseif msg.name == :idle
                    idle = true
                    # IDEA: Message controller and get more work?
                # handle :nonidle
                elseif msg.name == :nonidle
                    idle = false
                end
                
            end
        end
        
        # SUBTASK 1
        @async begin
            @printf("Worker subtask begun.\n")
            while true
                wait(_local_chl)
                @printf("Worker sending :nonidle message.\n")
                put!(msg_chl, Message(:nonidle, myid()))
                while isready(_local_chl)
                    msg = take!(_local_chl)

                    @printf("Worker received %s message.\n", string(msg))

                    # handle :end
                    if msg.name == :end
                        return
                    elseif msg.name == :work
                        sleep(rand(1:5))                        
                    end
                end
                @printf("Worker sending :idle message.\n")
                put!(msg_chl, Message(:idle, myid()))
            end
        end
        
    end
    
    put!(stat_chl, Message(:done, myid()))
    
end

@printf("Starting.\n")
@sync begin
    
    # create task to send messages to worker
    @async begin
        @printf("Controller work producer begun.\n")
        nwork = 10
        for i = 1:nwork
            put!(msg_chl, Message(:work, i))
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