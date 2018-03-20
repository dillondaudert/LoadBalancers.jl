# this example demonstrates how remote workers will signal to a central controller
# that they are idle or nonidle, using remote calls and remote references

# OVERVIEW
# 1. Create spawn controller and worker processes. Each worker is initially non-idle.
# 2. After a random wait, each worker will signal that it is idle.
# 3. Once all workers are idle, the controller will terminate all workers and exit.

# ------------------------------------------------------------

# create workers processes
np = 3
addprocs(np)

# remote channel for communication
const is_idle = zeros(Bool, nworkers())

# create remote channels on worker processes
const work_chls = [RemoteChannel(()->Channel(10), id) for id in workers()]

# everywhere loads the code on all processes
@everywhere function compute(secs)
    # simulate computing by simply waiting a random amount of time
    sleep(secs)
end

@everywhere function signal_idle(id)
    # change a worker's status to idle
    is_idle[id] = true
end

@everywhere function signal_nonidle(id)
    # return an idle worker's work channel to the calling worker
    # and set that worker to nonidle (this will only be called when
    # the calling function has work to distribute)
    # if there are no idle workers, then return nothing
    is_idle[id] = false
    for wid in 1:nworkers()
        if is_idle[wid]
            return work_chls[wid]
        end
    end 
    return nothing
end

# simulate a worker process doing work and signalling
@everywhere function do_work(work_chl::AbstractChannel)
    #local_work_chl = Channel(1)
    # create a local task to compute work
    #compute_task = @task compute(local_work_chl)
    #@schedule compute_task
    
    fut_work = nothing
    
    while true
        # if the channel is empty AND either there's no work task running, or that task is done
        if !isready(work_chl) && (fut_work == nothing || isready(fut_worK))
            # signal that this worker is idle
            remotecall(signal_idle, 1, my_id())
        end
        
        # take that item
        item = take!(work_chl)
        
        if item == :finish
            if fut_work != nothing
                # finish work 
                wait(fut_work)
            end
            # terminate worker
            break    
        end
        
        # there is work available
        if fut_work == nothing || isready(fut_work)
            # assign it locally
            fut_work = @spawnat myid() compute(item)
        
        else 
            # signal non-idle
            remote_work_chl = remotecall_fetch(signal_nonidle, 1, myid())
            # if there is an idle worker
            if remote_work_chl != nothing
                # pass work to other worker
                put!(remote_work_chl, item)
            else
                # no idle nodes, so compute our work
                wait(fut_work)
                fut_work = @spawnat myid() compute(item)
            end
        end
    end
end

