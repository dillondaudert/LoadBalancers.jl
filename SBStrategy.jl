# Scheduler-Based adaptive load balancing strategy
# OVERVIEW
# The controller acts as a scheduler and keeps an idle-status list
# - Worker i alerts the controller that it is non-idle
# - The controller sends the id of an idle worker j to worker i
#     and sets the status of j to non-idle
# - Worker i will then send work to j, or a message that it has 
#     no available work
# - Worker j then begins working or sends a message to the 
#     controller that it is idle.

# ------------------------------------------------------------

# create workers processes
np = 3
addprocs(np)

const is_idle = ones(Bool, nworkers())
is_idle[1] = false

# create remote channels on all processes
const work_chls = [RemoteChannel(()->Channel(64), id) for id in workers()]
# channel for passing status updates
const stat_chl = RemoteChannel(()->Channel(64), 1)
# create a dedicated remote channel for results
const res_chl = RemoteChannel(()->Channel(64), 1)


@everywhere function get_idle(id)
    """
    Get the id of an idle process.
    
    NOTE: This is called from workers, but should only ever run
    on the controller process.
    """

    # if no idle workers, return nothing
    if all(.!is_idle)
        return nothing
    end

    # get the indices of idle workers
    idle_idxs = filter(x->is_idle[x], 1:nworkers())

    # randomly select the index of one idle worker
    p_idx = rand(idle_idxs)

    # pass the worker id back to caller
    #put!(stat_chl, (workers()[p_idx], :nonidle))
    return workers()[p_idx]

end

# WORKER
@everywhere function worker(work_chls, res_chl, stat_chl)
    """
    A worker task. This task acts as a manager that launches sub-tasks
    for the various functions of a worker, including 
    sending/receiving messages, and computing work.
    In general, each node/process will run a single instance of this
    task.
    """
    
    # distinguish between controller-as-worker and not
    p_idx = nworkers() > 1 ? myid() - 1 : myid()
    
    work_chl = work_chls[p_idx]
    
    local_work_chl = Channel(1)
    
    nonidle_cond = Condition()
    
    @sync begin
        # SUBTASK: route incoming messages
        @async begin
            while true
                # block until item is available
                item = fetch(work_chl)

                if item == :finish
                    # propagate finish signal
                    put!(local_work_chl, :finish)
                    break
                elseif item == :nowork
                    # remove the message and signal idle
                    take!(work_chl)
                    #put!(stat_chl, (myid(), :idle))
                    continue
                else
                    # remove an item of work
                    work = take!(work_chl)
                    # if we have extra work
                    if isready(work_chl)
                        # notify the task that we are nonidle
                        notify(nonidle_cond)
                    end
                    @printf("Doing %g seconds of work.\n", work)
                    put!(local_work_chl, work)
                end
            end
        end
        # SUBTASK: Pull work off work_chl, compute, push to result channel
        @async begin
            while true
                work = take!(local_work_chl)
                if work == :finish
                    break
                end
                sleep(work)
                put!(res_chl, (work, myid()))
                if !isready(local_work_chl)
                    put!(stat_chl, (myid(), :idle))
                end
            end
        end
        
        # SUBTASK: Signal non-idle then potentially distribute work
        @schedule begin
            while true
                wait(nonidle_cond)
                # signal that we are not idle
                put!(stat_chl, (myid(), :nonidle))
                pid = remotecall_fetch(get_idle, 1, myid())
                if pid != nothing
                    # there is an idle node
                    p_idx = nworkers() > 1 ? pid - 1 : pid
                    other_work_chl = work_chls[p_idx]
                    if isready(work_chl)
                        # there is work to distribute
                        work = take!(work_chl)
                        @printf("|>> Moving work to worker %d\n", pid)
                        put!(other_work_chl, work)
                    else
                        # the local worker already took this work
                        @printf("|>> No work to move to worker %d\n", pid)
                        put!(other_work_chl, :nowork)                    
                    end
                else
                    # there are no idle nodes, sleep to prevent
                    # spam nonidle messages
                    sleep(1)
                end
            end
        end

    end
end

function status_manager()
    while any(.!is_idle)
        status = take!(stat_chl)
        pid = status[1]
        message = status[2]
        p_idx = nworkers() > 1 ? pid - 1 : pid

        if message == :idle
            @printf("~| Worker %d is idle.\n", pid)
            is_idle[p_idx] = true
        elseif message == :nonidle
            @printf("~| Worker %d is nonidle.\n", pid)
            is_idle[p_idx] = false
        end
    end
end

@printf("Controller starting.\n")
n = 20
max_work = 10
is_idle[1] = false
exec_time = zeros(Int, nworkers())
@sync begin
    # controller
    @sync begin

        @async begin
            for i in 1:n
                @printf("|> Sending initial work unit %d to worker %d.\n", i, workers()[1])
                put!(work_chls[1], rand(1:max_work))
            end
        end

        for pid in workers()
            @printf("Starting worker on process %d.\n", pid)
            @async remote_do(worker, pid, work_chls, res_chl, stat_chl)
        end

        @schedule begin
            for i in 1:n
                result = take!(res_chl)
                p_idx = nworkers() > 1 ? result[2] - 1 : result[2]
                exec_time[p_idx] += result[1]
            end
        end
    end

    @async status_manager()

end

@sync begin
    @printf("SIG: Signalling to workers to finish.\n")
    for chl in work_chls
        @async put!(chl, :finish)
    end
end
print("Controller terminating.\n")
for i in 1:nworkers()
    @printf("Worker %d spent %g seconds doing work.\n", workers()[i], exec_time[i])
end
