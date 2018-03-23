# Spawn a number of workers that are marked as nonidle
# The controller waits for all workers to signal that 
# they are idle, then terminates.

# set up
np = 3
addprocs(np)

# idle status, initialized to False
const is_idle = zeros(Bool, nworkers())
const trm_chl = Channel(1)

# non-idle signal function is called from the workers but only
# runs on the controller
@everywhere function signal_idle(id)
    @printf("Worker %d signalling idle.\n", id)
    if nworkers() > 1
        is_idle[id-1] = true
    else
        is_idle[id] = true
    end
    
    if all(is_idle)
        # signal to controller to terminate
        put!(trm_chl, true)
        @printf("All workers signalled idle!\n")
    end
end

# worker functions
@everywhere function worker_fn()
    # @printf("Worker %d, reporting in!!", myid())
    # wait a random amount of time
    sleep(rand(1:5))
    # signal idle
    @async remote_do(signal_idle, 1, myid())
end

# spawn the workers
print("Spawning workers.\n")
for p in workers()
    remote_do(worker_fn, p)
end

print("Waiting for workers.\n")
end_signal = take!(trm_chl)
print("Controller terminating.\n")
