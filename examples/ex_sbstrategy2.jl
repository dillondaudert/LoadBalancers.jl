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

np = 3
# create workers processes
addprocs(np)

@everywhere mutable struct WorkerState
    started::Bool
    signalling::Bool
    worker::Nullable{Task}
end

@everywhere struct Message
    name::Symbol
    content::Int
end

# create remote channels with global references on all workers
const work_chls = [RemoteChannel(()->Channel(64), id) for id in workers()]
# channel for passing status updates
const stat_chl = RemoteChannel(()->Channel(64), 1)
# create a dedicated remote channel for results
const res_chl = RemoteChannel(()->Channel(64), 1)

const statuses = fill!(Array{Symbol}(nworkers()), :unstarted)

function controller()
    
    @printf("Controller starting.\n")
    

    n = 5
    max_work = 5
    statuses[1] = :started
    exec_time = zeros(Int, nworkers())
    @sync begin
        # controller
        @async begin
            for i in 1:n
                @printf("|> Sending initial work unit %d to worker %d.\n", i, workers()[1])
                put!(work_chls[1], Message(:work, rand(1:max_work)))
            end
        end

        for pid in workers()
            @printf("Starting worker on process %d.\n", pid)
            @async remote_do(worker, pid, work_chls, stat_chl, res_chl)
        end

        @schedule begin
            for i in 1:n
                result = take!(res_chl)
                p_idx = nworkers() > 1 ? result[1] - 1 : result[1]
                exec_time[p_idx] += result[2]
            end
        end

        @async status_manager()

    end

    @sync begin
        @printf("SIG: Signalling to workers to finish.\n")
        for chl in work_chls
            @async put!(chl, Message(:finish, -1))
        end
    end
    print("Controller terminating.\n")
    for i in 1:nworkers()
        @printf("Worker %d spent %g seconds doing work.\n", workers()[i], exec_time[i])
    end

end

@everywhere function worker_subtask()
    # when we start, don't signal; just wait for work
    while true
        wait(_local_work_chl)
        while isready(_local_work_chl)
            local work_message
            work_message = take!(_local_work_chl)
            if work_message.name == :finish
                return
            end
            @assert work_message.name == :work
            sleep(work_message.content)
            put!(res_chl, (myid(), work_message.content))
        end
        # there is no more work in the local channel, signal to self
        put!(msg_chl, Message(:idle, -1))
    end
end

@everywhere function do_work(work_message::Message, res_chl)
    @printf("Doing work: %s\n", string(work_message))
    if work_message.name != :work
        @printf("NON-WORK MESSAGE IN DO_WORK ON WORKER %d.\n", myid())
    end
    # simulate doing work
    sleep(work_message.content)
    put!(res_chl, (myid(), work_message.content))
end

# WORKER
@everywhere function worker(work_chls, stat_chl, res_chl)
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
    
    state = WorkerState(false, false, Nullable{Task}())
    
    while true
        # NEW --
        if state.started && (isnull(state.worker) || get(state.worker).state == :done)
            # if we've started, and currently are doing no work
        end
        message = take!(work_chl)
        local message
        sleep(1)

        # route message
        if message.name == :finish
            # terminate message
            @assert !state.signalling
            @assert isnull(state.worker) || get(state.worker).state == :done
            @printf("Terminating.\n")
            break

        elseif message.name == :work
            # work message
            if !state.started
                # signal that this worker has started
                @printf("Starting\n")
                put!(stat_chl, Message(:nonidle, myid()))
                state.started = true
                @assert !state.signalling & isnull(state.worker)
                # launch worker task
                state.worker = @schedule do_work(message, res_chl)
                state.signalling = true
            elseif isnull(state.worker) || get(state.worker).state == :done
                # not currently doing work, so do work
                state.worker = @schedule do_work(message, res_chl)
            elseif !state.signalling
                # signal that we're nonidle
                put!(stat_chl, Message(:nonidle, myid()))
                state.signalling = true
                # Same idea here as below; we want to yield control
                #   before cycling the messages
                wait(work_chl)
                yield()
                put!(work_chl, message)
            else
                # we are working and signalling
                # IDEA: If we wait on the work_chl, then it will
                #   yield to another task if there are no messages
                #   in the channel; this gives the worker a chance
                #   chance to finish. If there ARE messages in the
                #   work channel, then since we are signalling,
                #   this will cycle through the messages so 
                #   eventually we will receive the :jlance message.
                # Is it possible the next message could be :finish?
                #   No, because signalling == true implies that this
                #   node is marked as nonidle.
                sleep(1)
                put!(work_chl, message)
            end

        elseif message.name == :jlance
            @assert state.signalling
            # message from controller with an idle worker id

            if message.content != -1
                local next_message
                # load-balance message
                other_worker_idx = nworkers() > 1 ? message.content-1 : message.content
                other_worker_chl = work_chls[other_worker_idx]
                # if we have work messages
                if isready(work_chl) && fetch(work_chl).name == :work
                    next_message = take!(work_chl)
                    @printf("|>> Sending %s to worker %d.\n", string(next_message), message.content)
                    put!(other_worker_chl, next_message)
                else
                    # no work to share
                    @printf("|>> Sending :nowork to worker %d.\n", message.content)
                    put!(other_worker_chl, Message(:nowork, myid()))
                end
            else
                @printf("Handling (:jlance, -1) message.\n")
            end

            # indicate that we've handled the signalling
            state.signalling = false
        elseif message.name == :nowork
            @printf("Received :nowork message from %d.\n", message.content)
            @assert isnull(state.worker) || get(state.worker).state == :done
            @assert !state.signalling
            # message from other worker indicating no work to share
            put!(stat_chl, Message(:idle, myid()))
        end
    
    end
end

function status_manager()
    #  while there are any started, nonidle nodes
    while any((statuses .!= :unstarted) .& (statuses .!= :idle))
        status = take!(stat_chl)
        p_idx = nworkers() > 1 ? status.content - 1 : status.content

        if status.name == :idle
            @printf("|~ Worker %d is idle.\n", status.content)
            statuses[p_idx] = :idle
        elseif status.name == :nonidle
            @printf("|~ Worker %d is signalling nonidle, ", status.content)
            statuses[p_idx] = :nonidle
            # look for an unstarted or idle worker
            work_chl = work_chls[p_idx]

            # get the indices of idle or unstarted workers
            idle_idxs = filter(
                    x->(statuses[x] == :unstarted || statuses[x] == :idle),
                    1:nworkers()
                )
            if length(idle_idxs) == 0
                # send a jlance message indicating no idle workers
                @printf("sending (:jlance, -1).\n")
                put!(work_chl, Message(:jlance, -1))
            else
                # randomly select the index of one idle worker
                idle_idx = rand(idle_idxs)
                @printf("sending (:jlance, %d).\n", workers()[idle_idx])
                put!(work_chl, Message(:jlance, workers()[idle_idx]))
            end
        end
    end
end

controller()
