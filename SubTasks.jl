# useful generic subtasks that can be reused

module SubTasks

export Message, _worker, _msg_handler, _jlancer

struct Message
    kind::Symbol
    data::Int64
end

# --- message handlers

function _msg_handler(local_chl::Channel{Message}, msg_chl::RemoteChannel{Channel{Message}})
    """
    Receive messages on the remote channel msg_chl. Depending on the message,
    different actions will be taken:
        :work - Pass from the remote channel to local_chl
        :_idle - Print 
        :_nonidle - Print
        :end  - Pass to local_chl and exit.
    """

    while true
        msg = take!(msg_chl)

        if msg.kind == :end
            put!(local_chl, msg)
            break
        elseif msg.kind == :work
            put!(local_chl, msg)
        elseif msg.kind == :_idle
            @printf("_msg_handler received idle signal from _worker on %d.\n", msg.data)
        elseif msg.kind == :_nonidle
            @printf("_msg_handler received nonidle signal from _worker on %d.\n", msg.data)
        end

    end
end

function _msg_handler(local_chl::Channel{Message}, 
                      msg_chls::Array{RemoteChannel{Channel{Message}}},
                      stat_chl::RemoteChannel{Channel{Message}})
    """
    Receive messages on its remote channel. Depending on the message,
    different actions will be taken:
    External Messages: (Could come from anywhere)
        :work - Pass from the remote channel to local_chl
        :jlance - Start a new task that will attempt to send work in local_chl
                  to the remote worker specified by this message
        :nowork - Another worker failed to send work to this worker; pass an :idle
                  message to the controller via stat_chl
        :end  - Pass to local_chl and exit.
    Internal Messages: (Expect to receive these only from other tasks on this process)
        :_idle - Send an :idle message to the controller via stat_chl 
        :_nonidle - Send a :nonidle message to the controller via stat_chl
    """

    w_idx = nprocs() > 1 ? myid() - 1 : myid()
    msg_chl = msg_chls[w_idx]

    @assert msg_chl.where == myid()

    while true
        msg = take!(msg_chl)

        if msg.kind == :end
            put!(local_chl, msg)
            break

        elseif msg.kind == :work
            put!(local_chl, msg)

        elseif msg.kind == :nowork
            put!(stat_chl, Message(:idle, myid()))

        elseif msg.kind == :jlance && msg.data > 0
            local wrk_msg::Message = msg
            # attempt to load balance
            @schedule _jlancer(wrk_msg, local_chl, msg_chls)

        elseif msg.kind == :_idle
            put!(stat_chl, Message(:idle, myid()))

        elseif msg.kind == :_nonidle
            put!(stat_chl, Message(:nonidle, myid()))

        end

    end
end

# --- jlancers
function _jlancer(msg::Message,
                  local_chl::Channel{Message},
                  msg_chls::Array{RemoteChannel{Channel{Message}}})
    # attempt to move some local work to the remote worker
    other_w_idx = nprocs() > 1 ? msg.data - 1 : msg.data
    other_msg_chl = msg_chls[other_w_idx]
    if isready(local_chl)
        work = take!(local_chl)
        @assert work.kind == :work
        @printf("_jlancer passing work to worker %d.\n", msg.data)
        put!(other_msg_chl, work)
    else
        @printf("_jlancer has no work to pass to %d.\n", msg.data)
        put!(other_msg_chl, Message(:nowork, myid()))
    end

end


# --- workers

function _worker(local_chl::Channel{Message}, msg_chl::RemoteChannel{Channel{Message}})
    """
    Pull work from local_chl to compute, and send status
    updates to msg_chl.
    This worker receives two message kinds (<| local_chl):
        :work
        :end - This worker will exit upon receiving this message
    This worker also produces two message kinds (|> msg_chl)
        :_idle - Signal that this worker is idle (local_chl empty)
        :_nonidle - Signal that this worker is nonidle
    """

    idle::Bool = true

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

            msg = take!(local_chl)

            # if this message is end, exit
            if msg.kind == :end
                return
            elseif msg.kind == :work
                # if this worker has been idle, signal it is no longer idle
                if idle
                    @printf("%d WORKING %d\n", myid(), myid())
                    idle = false
                    last_signal = time()
                    put!(msg_chl, Message(:_nonidle, myid()))
                # if it has been 1 second since the last nonsignal message
                
                elseif time() - last_signal > 1
                    # sleep to force the scheduler to switch to another (hopefully jlance) task
                    sleep(0.05)
                    put!(msg_chl, Message(:_nonidle, myid()))
                    last_signal = time()
                end

                # do work
                sleep(rand(1:msg.data))
            else
                @printf("_worker received unrecognized message! %s\n", string(msg))
                exit()
            end
        end

        # local_chl is now empty
        if !idle
            @printf("IDLE\n")
            idle = true
            put!(msg_chl, Message(:_idle, myid()))
        end
    end
end # end _worker function

end # End SubTasks Module
