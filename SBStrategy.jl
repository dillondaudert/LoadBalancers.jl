export sb_controller

# measure total time to finish calculation
function sb_controller(work::WorkUnit)
    msg_chls = [RemoteChannel(()->Channel{Message}(32*nworkers()), pid) for pid in workers()]
    stat_chl = RemoteChannel(()->Channel{Message}(32), 1)
    res_chl = RemoteChannel(()->Channel{Message}(32), 1)
    statuses = fill!(Array{Symbol}(nworkers()), :unstarted)
    Tₚ = @elapsed @sync begin

        # start the worker processes
        for i = 1:nworkers()
            @spawnat workers()[i] worker(msg_chls, stat_chl, res_chl)
        end

        @async recv_results(res_chl)

        @sync begin
            statuses[1] = :started

            @async put!(msg_chls[1], Message(:work, myid(), work))

            @async status_manager(msg_chls, stat_chl, statuses)
        end

        for i = 1:nworkers()
            put!(msg_chls[i], Message(:end, -1))
        end
    end
    Tₚ
end

function worker(msg_chls::Array{RemoteChannel{Channel{Message}}},
                stat_chl::RemoteChannel{Channel{Message}},
                res_chl::RemoteChannel{Channel{Message}})
    
    local_chl = Channel{Message}(10)
    w_idx = nprocs() > 1 ? myid() - 1 : myid()
    my_msg_chl = msg_chls[w_idx]

    
    @sync begin
        # MESSAGE HANDLER SUBTASK
        @async _msg_handler(local_chl, msg_chls, stat_chl)
        # SUBTASK 1
        @async _worker(local_chl, my_msg_chl, res_chl)
    end
    put!(stat_chl, Message(:done, myid()))

    #Profile.print(combine=true, mincount=80)
end

function status_manager(msg_chls::Array{RemoteChannel{Channel{Message}}},
                        stat_chl::RemoteChannel{Channel{Message}},
                        statuses::Array{Symbol})
    # while there are any started, nonidle nodes
    @printf("status_manager started\n")
    while any((statuses .!= :unstarted) .& (statuses .!= :idle))
        status_msg = take!(stat_chl)
        w_idx = nprocs() > 1 ? status_msg.data - 1 : status_msg.data

        if status_msg.kind == :idle
            # mark this worker as idle
            statuses[w_idx] = :idle

        elseif status_msg.kind == :nonidle
            statuses[w_idx] = :nonidle

            # look for a nonbusy worker
            w_msg_chl = msg_chls[w_idx]

            # get the indices of idle or unstarted workers
            idle_idxs = filter(x->statuses[x] != :nonidle,
                               1:nworkers())

            if length(idle_idxs) == 0
                # no idle workers
                put!(w_msg_chl, Message(:jlance, -1))
            else
                # randomly select the index of an idle worker
                idle_w_idx = rand(idle_idxs)
                statuses[idle_w_idx] = :nonidle
                put!(w_msg_chl, Message(:jlance, workers()[idle_w_idx]))
            end
        end
    end
end

function recv_results(res_chl)
    n_ended = 0
    res_count = 0
    total_work_done = zeros(Int64, nworkers())
    while n_ended < nworkers()
        work = take!(res_chl)
        if work.kind == :work
            res_count += 1
            w_idx = nprocs() > 1 ? work.data - 1 : work.data
            # add the work this worker did
            total_work_done[w_idx] += work._data2
        elseif work.kind == :end
            n_ended += 1
        end
    end

    for w_idx in 1:nworkers()
        @printf("Worker %d did %ds of work.\n", workers()[w_idx], total_work_done[w_idx])
    end
end

    
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

    msg_chl = get_msg_chl(myid(), msg_chls)

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
            @printf("Worker idle\n")
            put!(stat_chl, Message(:idle, myid()))

        elseif msg.kind == :jlance && msg.data > 0
            # attempt to load balance
            @schedule _jlancer(msg, local_chl, msg_chls)

        elseif msg.kind == :_idle
            put!(stat_chl, Message(:idle, myid()))

        elseif msg.kind == :_nonidle
            @printf("Worker working\n")
            # put! on remote chl blocks, so schedule in different task
            @schedule put!(stat_chl, Message(:nonidle, myid()))
        end
        # end let scope    
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
        put!(other_msg_chl, work)
    else
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
            idle = true
            put!(msg_chl, Message(:_idle, myid()))
        end
    end
end # end _worker function