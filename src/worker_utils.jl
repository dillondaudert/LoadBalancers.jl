
"""
    worker(balancer<:AbstractLoadBalancer)

Start a worker that receives work messages and performs computation asynchronously.
"""
function worker(balancer::T) where {T<:AbstractLoadBalancer}
    
    local_chl = Channel{Message}(10)
    
    @sync begin
        # MESSAGE HANDLER SUBTASK
        @async _msg_handler(balancer, local_chl)
        # SUBTASK 1
        @async do_work(balancer, local_chl)
    end
    put!(balancer.stat_chl, Message(:done, myid()))
end


"""
    do_work(balancer<:AbstractLoadBalancer, local_chl::Channel{Message})

Pull work from local_chl to compute, and send status updates to msg_chl.

This worker receives two message kinds (<| `local_chl`):
- `:work`
- `:end` - This worker will exit upon receiving this message

This worker also produces two message kinds (|> `balancer.msg_chl`)
- `:_idle` - Signal that this worker is idle (`local_chl` empty)
- `:_nonidle` - Signal that this worker is nonidle
"""
function do_work(balancer::T,
                 local_chl::Channel{Message}) where {T<:AbstractLoadBalancer}

    idle::Bool = true
    my_msg_chl = get_msg_chl(myid(), balancer.msg_chls)

    info("worker ", string(myid()), " do_work")

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
                    info("worker ", string(myid()), " working")
                    idle = false
                    last_signal = time()
                    put!(my_msg_chl, Message(:_nonidle, myid()))
                # if it has been 1 second since the last nonidle message
                
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
                err("_worker received unrecognized message! ", string(msg))
                error("do_work error")
            end
        end

        # local_chl is now empty
        if !idle
            idle = true
            put!(my_msg_chl, Message(:_idle, myid()))
        end
    end
end # end _worker function

"""
    send_work(balancer<:AbstractLoadBalancer, local_chl::Channel{Message}, msg::Message)

Attempt to send a piece of work from this worker to another. 
"""
function send_work(balancer::T,
                   local_chl::Channel{Message},
                   msg::Message) where {T<:AbstractLoadBalancer}
    # attempt to move some local work to the remote worker
    other_wid = msg.data
    other_msg_chl = get_msg_chl(balancer, other_wid)
    if isready(local_chl)
        if fetch(local_chl).kind == :end
            return
        end
        work = take!(local_chl)
        put!(other_msg_chl, work)
    else
        put!(other_msg_chl, Message(:nowork, myid()))
    end
end

