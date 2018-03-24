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

# only worker 1 is non-idle to begin
const is_idle = ones(Bool, nworkers())

# create remote channels on all processes
const work_chls = [RemoteChannel(()->Channel(64), id) for id in workers()]
# create a dedicated remote channel for results
const res_chl = RemoteChannel(()->Channel(64), 1)

# Signalling Functions
@everywhere function signal_idle(id)
    """
    Signal to the controller that the calling worker is idle.
    Set the status of the caller to idle.
    
    NOTE: This is called from workers, but should only ever run
    on the controller process.
    """
    if nworkers() > 1
        is_idle[id-1] = true
    else
        is_idle[id] = true
    end
    @printf("SIG: Worker %d signalling idle...", id)
    @printf("Idle status: %s\n", string(is_idle))
end

@everywhere function signal_nonidle(id)
    """
    Signal to the controller that the calling worker is non-idle.
    Set the status of the caller to non-idle, then return the id
    of an idle worker, or nothing if all workers are non-idle.
    
    NOTE: This is called from workers, but should only ever run
    on the controller process.
    """
    @printf("SIG: Worker %d signalling non-idle, ", id)
    if nworkers() > 1
        is_idle[id-1] = false
    else
        is_idle[id] = false
    end
    
    for pid in workers()
        
        p_idx = nworkers() > 1 ? pid-1 : pid
        
        if is_idle[p_idx]
            @printf("returning idle worker %d\n", pid)
            is_idle[p_idx] = false
            return pid
        end
    end
    @printf("no idle worker.\n")
    return nothing
end

# WORKER
@everywhere function worker(work_chls, res_chl)
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
    
    local_work_chl = Channel(5)
    
    nonidle_cond = Condition()
    
    @sync begin

        # SUBTASK: Pull work off work_chl, compute, push to result channel
        @schedule begin
            while true
                work = take!(local_work_chl)
                sleep(work)
                put!(res_chl, (work, myid()))
            end
        end
        
        # SUBTASK: Signal non-idle then potentially distribute work
        @schedule begin
            while true
                wait(nonidle_cond)
                pid = remotecall_fetch(signal_nonidle, 1, myid())
                if pid != nothing
                    # there is an idle node
                    p_idx = nworkers() > 1 ? pid - 1 : pid
                    other_work_chl = work_chls[p_idx]
                    if isready(local_work_chl)
                        # there is work to distribute
                        work = take!(local_work_chl)
                        @printf("|>> Worker %d moving work to worker %d\n", myid(), pid)
                        put!(other_work_chl, work)
                    else
                        # the local worker already took this work
                        @printf("|>> Worker %d telling worker %d no work available\n", myid(), pid)
                        put!(other_work_chl, :nowork)                    
                    end
                else
                    # there are no idle nodes, sleep to prevent
                    # spam nonidle messages
                    sleep(3)
                end
            end
        end

        # SUBTASK: monitor idle status, notify other subtasks
        @async begin
            while true
                remotecall(signal_idle, 1, myid())
                # block until item is available
                item = fetch(work_chl)

                if item == :finish
                    # close local channels and break
                    close(local_work_chl)
                    break
                elseif item == :nowork
                    # remove the message and signal idle
                    take!(work_chl)
                    remotecall(signal_idle, 1, myid())
                    continue
                else
                    # remove an item of work
                    work = take!(work_chl)
                    put!(local_work_chl, work)
                    # notify the task that we are nonidle
                    notify(nonidle_cond)
                end
            end
        end
    end
    @printf("Worker %d finished.", myid())
    # exit worker 
end

@printf("Controller starting.\n")
@sync begin
    # controller
    @sync begin
        n = 10
        is_idle[1] = false

        @async begin
            for i in 1:n
                @printf("|> Sending initial work unit %d to worker %d.\n", i, workers()[1])
                put!(work_chls[1], rand(1:6))
            end
        end

        @async begin
            while any(.!is_idle)
                sleep(1)
            end
            @printf("All workers idle!\n")
        end

        for pid in workers()
            @printf("Starting worker on process %d.\n", pid)
            @async remote_do(worker, pid, work_chls, res_chl)
        end

        @schedule begin
            for i in 1:n
                result = take!(res_chl)
                @printf("Printing result: %g seconds on worker %d.\n", result[1], result[2])
            end
        end
    end

    @printf("SIG: Signalling to workers to finish.\n")
    for chl in work_chls
        @async put!(chl, :finish)
    end
end

print("Controller terminating.\n")
