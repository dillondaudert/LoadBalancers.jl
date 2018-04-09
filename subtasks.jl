# useful generic subtasks that can be reused

module SubTasks
include("work.jl")

export WorkUnit, Message, _worker, _msg_handler, _jlancer, _msg_handler_rp

struct Message
    kind::Symbol
    data::Int64
    _data2 # TODO: rename data and _data2 to more sensible names
end

Message(kind, data) = Message(kind, data, -1)

# --- message handlers

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
function _msg_handler(local_chl::Channel{Message}, 
                      msg_chls::Array{RemoteChannel{Channel{Message}}},
                      stat_chl::RemoteChannel{Channel{Message}})

    w_idx = nprocs() > 1 ? myid() - 1 : myid()
    msg_chl = msg_chls[w_idx]

    @assert msg_chl.where == myid()

    @printf("_msg_handler started\n")

    while true
        let msg = take!(msg_chl)
        # begin let scope
        if msg.kind == :end
            put!(local_chl, msg)
            break

        elseif msg.kind == :work
            put!(local_chl, msg)

        elseif msg.kind == :nowork
            put!(stat_chl, Message(:idle, myid()))

        elseif msg.kind == :jlance && msg.data > 0
            # attempt to load balance
            @schedule _jlancer(msg, local_chl, msg_chls)

        elseif msg.kind == :_idle
            put!(stat_chl, Message(:idle, myid()))

        elseif msg.kind == :_nonidle
            # put! on remote chl blocks, so schedule in different task
            @schedule put!(stat_chl, Message(:nonidle, myid()))
        end
        # end let scope    
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
function _msg_handler_rp(local_chl::Channel{Message}, 
                         msg_chls::Array{RemoteChannel{Channel{Message}}},
                         stat_chl::RemoteChannel{Channel{Message}})

    w_idx = nprocs() > 1 ? myid() - 1 : myid()
    msg_chl = msg_chls[w_idx]


    # other worker ids
#    other_wrkr_idxs = filter(x->workers()[x] != myid(), 1:nworkers())

    @assert msg_chl.where == myid()

    while true
        let msg = take!(msg_chl)
        # start soft local scope 
        if msg.kind == :end
            put!(local_chl, msg)
            break

        elseif msg.kind == :work
            put!(local_chl, msg)

        elseif msg.kind == :nowork
            put!(stat_chl, Message(:idle, myid()))
#            other_wrkr_idx = rand(other_wrkr_idxs)
            other_wrkr_idx = rand(1:nworkers())
            @printf("Requesting work from %d.\n", workers()[other_wrkr_idx])
            other_msg_chl = msg_chls[other_wrkr_idx]
            put!(other_msg_chl, Message(:jlance, myid()))

        elseif msg.kind == :jlance && msg.data > 0
            # attempt to load balance
            @schedule _jlancer(msg, local_chl, msg_chls)

        elseif msg.kind == :_idle
            put!(stat_chl, Message(:idle, myid()))
            # if there is only 1 worker, there is no one else to get work from
            #    and, if the only worker is idle, that means we should finish
            @printf("Worker %d idle.\n", myid())
            if nworkers() > 1
                #other_wrkr_idx = rand(other_wrkr_idxs)
                other_wrkr_idx = rand(1:nworkers())
                @printf("Requesting work from %d.\n", workers()[other_wrkr_idx])
                other_msg_chl = msg_chls[other_wrkr_idx]
                put!(other_msg_chl, Message(:jlance, myid()))
            end

        elseif msg.kind == :_nonidle
            # put! on remote chl blocks, so schedule in different task
            @schedule put!(stat_chl, Message(:nonidle, myid()))
        end
        # end soft local scope
        end
    end
end


# --- jlancers
"""
\t(msg::Message, local_chl::Channel{Message}, msg_chls::Array{RemoteChannel{Channel{Message}}})

Attempt to send a piece of work from this worker to another. If there is no
work available, then a message indicating no work will be sent.
"""
function _jlancer(msg::Message,
                  local_chl::Channel{Message},
                  msg_chls::Array{RemoteChannel{Channel{Message}}})
    # attempt to move some local work to the remote worker
    other_w_idx = nprocs() > 1 ? msg.data - 1 : msg.data
    other_msg_chl = msg_chls[other_w_idx]
    if isready(local_chl)
        if fetch(local_chl).kind == :end
            return
        end
        work = take!(local_chl)
        @printf("%d jlancer passing work to worker %d.\n", myid(), msg.data)
        put!(other_msg_chl, work)
    else
        @printf("%d jlancer has no work to pass to %d.\n", myid(), msg.data)
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
function _worker(local_chl::Channel{Message},
                 msg_chl::RemoteChannel{Channel{Message}},
                 res_chl::RemoteChannel{Channel{Message}})

    idle::Bool = true

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
                put!(res_chl, Message(:end, myid()))
                return
            elseif msg.kind == :work
                # if this worker has been idle, signal it is no longer idle
                if idle
                    @printf("%d working\n", myid())
                    idle = false
                    last_signal = time()
                    put!(msg_chl, Message(:_nonidle, myid()))
                # if it has been 1 second since the last nonsignal message
                
                elseif time() - last_signal > 1
                    put!(msg_chl, Message(:_nonidle, myid()))
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

                put!(res_chl, Message(:work, myid(), work_1.unitcost))
            else
                @printf("_worker received unrecognized message! %s\n", string(msg))
                exit()
            end
        end

        # local_chl is now empty
        if !idle
            @printf("%d idle\n", myid())
            idle = true
            put!(msg_chl, Message(:_idle, myid()))
        end
    end
end # end _worker function

end # End SubTasks Module
