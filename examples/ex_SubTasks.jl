# useful generic subtasks that can be reused

module SubTasks

export Message, _worker, _msg_handler

struct Message
    kind::Symbol
    data::Int64
    _data2 # TODO: rename data and _data2 to more sensible names
end

Message(kind, data) = Message(kind, data, -1)

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
# --- workers

function _worker(local_chl::Channel{Message},
                 msg_chl::RemoteChannel{Channel{Message}})
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
        wait(local_chl)

        # while there are messages in the local channel
        while isready(local_chl)

            local msg = take!(local_chl)

            # if this message is end, exit
            if msg.kind == :end
                return
            elseif msg.kind == :work
                # if this worker has been idle, signal it is no longer idle
                if idle
                    @printf("%d working\n", myid())
                    idle = false
                    put!(msg_chl, Message(:_nonidle, myid()))
                end

                # simulate calculation
                sleep(msg.data)

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
