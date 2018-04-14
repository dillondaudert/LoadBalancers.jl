abstract type AbstractLoadBalancer end

# ------ worker helper functions
"""
    w_idx(wid::Integer)
    w_idx()

Return the index of a worker in the workers() array, given its process id.

If no wid is specified, then this returns the index of the calling process.
Note that this should only be called from the controller if the controller
is also a worker.
"""
w_idx(wid::Integer) = nprocs() > 1 ? wid - 1 : wid
w_idx() = w_idx(myid())


"""
    get_msg_chl(balancer<:AbstractLoadBalancer, wid::Integer)
    get_msg_chl(balancer<:AbstractLoadBalancer)

Return a reference to the message channel associated with worker wid.
"""
get_msg_chl(balancer::T, wid::Integer) where {T<:AbstractLoadBalancer} = balancer.msg_chls[w_idx(wid)]
get_msg_chl(balancer::T) where {T<:AbstractLoadBalancer} = get_msg_chl(balancer, myid())


"""
    create_msg_chls(capacity::Integer=64)

Create nworkers() remote channels for message passing.

# Arguments
- `capacity`: the capacity of the channels

"""
function create_msg_chls(capacity::Integer=64)
    msg_chls = [RemoteChannel(()->Channel{Message}(capacity), pid) for pid in workers()]
    msg_chls
end

# -- Simulate work 

mutable struct WorkUnit{T<:Integer, S<:Real}
    units::T
    unitcost::S
    WorkUnit{T, S}(u, c) where {T<:Integer, S<:Real} = u < 0 || c ≤ 0 ? error("negative units or cost less than 0") : new(u, c)
end
WorkUnit(u::T, c::S) where {T<:Integer, S<:Real} = WorkUnit{T, S}(u, c)


"""
    split_work(work::WorkUnit, p<:AbstractFloat)
    split_work(work::WorkUnit)

Split a work into two WorkUnits, u and v, according to percentage p.

work must have at least 2 units. work will always be split in such a way to guarantee
that both u and v have at least 1 unit each. If `p` is not given, then a random value
0 < p < 1 will be used.

# Arguments
- `work`: An object of type WorkUnit to be split
- `p`: The percentage of units to give to the first split, u.

# Examples

```jldoctest
julia> w = WorkUnit(10, 3.)
WorkUnit{Int64,Float64}(10, 3.0)
julia> u, v = split_work(w, .6)
(WorkUnit{Int64,Float64}(6, 3.0), WorkUnit{Int64,Float64}(4, 3.0))
```
"""
function split_work(work::WorkUnit, p::T where T<:AbstractFloat)

    # check that valid percentage
    !(0 < p < 1) && (@printf("p must be: 0 < p < 1, got %g\n", p); throw(DomainError()))
    # check that there are at least 2 units
    work.units < 2 && (@printf("work must have more than 2 units\n"); throw(DomainError()))

    # set the rounding mode to up or down depending on p
    mode = p ≤ .5 ? RoundUp : RoundDown
    
    u_units = round(typeof(work.units), work.units * p, mode)
    v_units = work.units - u_units

    return WorkUnit(u_units, work.unitcost), WorkUnit(v_units, work.unitcost)

end
split_work(work::WorkUnit) = (p = rand(1:999); split_work(work, p/1000))


# ------ Message type for passing messages between processes and tasks
struct Message
    kind::Symbol
    data::Int64
    _data2 # TODO: rename data and _data2 to more sensible names
end

Message(kind, data) = Message(kind, data, -1)


"""
    recv_results(balancer<:AbstractLoadBalancer)

Receive and handle the results from workers passed to res_chl.
"""
function recv_results(balancer::T) where {T<:AbstractLoadBalancer}
    n_ended = 0
    res_count = 0
    total_work_done = zeros(Int64, nworkers())
    while n_ended < nworkers()
        work = take!(balancer.res_chl)
        if work.kind == :work
            res_count += 1
            w_idx = nprocs() > 1 ? work.data - 1 : work.data
            # add the work this worker did
            total_work_done[w_idx] += work._data2
            info("Receiving results #", string(res_count), " from worker ", string(work.data))
        elseif work.kind == :end
            n_ended += 1
        end
    end

    for w_idx in 1:nworkers()
        info("Worker ", string(workers()[w_idx]), " did ", string(total_work_done[w_idx]), "s of work.")
    end
end
