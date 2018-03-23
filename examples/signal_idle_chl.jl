# Signal idle to controller using a dedicated task and a remote channel# Spawn a number of workers that are marked as nonidle
# The controller waits for all workers to signal that 
# they are idle, then terminates.

# set up
np = 3
addprocs(np)

# idle status, initialized to False
const is_idle = zeros(Bool, nworkers())
const job_chls = [RemoteChannel(()->Channel{Any}(2), id) for id in workers()]
const idle_chl = RemoteChannel(()->Channel{Int}(nworkers()), 1)

# non-idle signal function is called from the workers but only
# worker functions
@everywhere function worker_fn(job_chls, idle_chl)
    # wait a random amount of time
    if nworkers() > 1
        p_idx = myid() - 1
    else
        p_idx = myid()
    end
    job_chl = job_chls[p_idx]
    work = take!(job_chl)
    sleep(work)
    # signal idle
    put!(idle_chl, myid())
    stop = take!(job_chl)
    @printf("Stop signal received on node %d\n", myid())
end

function idle_manager()
    while any(.!is_idle)
        id = take!(idle_chl)
        @printf("Worker %d signalling idle.\n", id)
        if nworkers() > 1
            is_idle[id-1] = true
        else
            is_idle[id] = true
        end
    end
    @printf("All workers have signalled idle!\n")
    return
end


@sync begin

    @sync begin
        print("Spawning workers.\n")
        for p in workers()
            if nworkers() > 1
                p_idx = p-1
            else
                p_idx = p
            end

            @printf("Adding work to worker %d channel\n", p)
            @async begin 
                job_chl = job_chls[p_idx]
                put!(job_chl, rand(1:5))
            end

            @printf("Spawning worker on process %d\n", p)
            @async remote_do(worker_fn, p, job_chls, idle_chl)

            # create the idle status manager
        end
        @async idle_manager()
    end

    @printf("Sending stop signals to workers.\n")
    for job_chl in job_chls
        @async put!(job_chl, :done)
    end
end

print("Controller terminating.\n")
