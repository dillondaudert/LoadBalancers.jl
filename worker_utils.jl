# worker utilities; functions used by all lb strategies

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
function do_work(balancer::T,
                 local_chl::Channel{Message}) where {T<:AbstractBalancer}

    idle::Bool = true
    my_msg_chl = get_msg_chl(myid(), balancer.msg_chls)

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
                put!(balancer.res_chl, Message(:end, myid()))
                return
            elseif msg.kind == :work
                # if this worker has been idle, signal it is no longer idle
                if idle
                    @printf("%d working\n", myid())
                    idle = false
                    last_signal = time()
                    put!(my_msg_chl, Message(:_nonidle, myid()))
                # if it has been 1 second since the last nonsignal message
                
                elseif time() - last_signal > 1
                    put!(my_msg_chl, Message(:_nonidle, myid()))
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

                put!(balancer.res_chl, Message(:work, myid(), work_1.unitcost))
            else
                @printf("_worker received unrecognized message! %s\n", string(msg))
                exit()
            end
        end

        # local_chl is now empty
        if !idle
            idle = true
            put!(my_msg_chl, Message(:_idle, myid()))
        end
    end
end # end _worker function
