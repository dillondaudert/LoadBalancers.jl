abstract type AbstractBalancer end

# ------ worker helper functions
"""
Return the index of a worker in the workers() array, given its process id.
"""
w_idx(wid) = nprocs() > 1 ? wid - 1 : wid

"""
Return a reference to the message channel located on a particular worker.
"""
get_msg_chl(wid, msg_chls) = msg_chls[w_idx(wid)]

"""
Args: cap - Integer indicating the capacity of the channels.

Create nworkers() remote channels for message passing.
"""
function create_msg_chls(cap::Int)
    msg_chls = [RemoteChannel(()->Channel(cap), pid) for pid in workers()]
    msg_chls
end


# ------ helper functions for simulating work
mutable struct WorkUnit{T<:Int}
    units::T
    unitcost::T
    WorkUnit{T}(u, c) where {T<:Int} = u < 0 || c ≤ 0 ? error("negative units or cost less than 0") : new(u, c)
end
WorkUnit(u::T, c::T) where {T<:Int} = WorkUnit{T}(u, c)

function split_work(work::WorkUnit, p::T where T<:AbstractFloat)
    """
    Split a work into two WorkUnits, u and v, according to percentage p.
    WorkUnit u will have p*units units of work, and WorkUnit v will have
    v*(1-p) units of work.
    Note that 0 < p < 1, as each resulting struct must have at least 1
    unit of work.
    """

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

# generate a random percentage from the closed interval (0, 1)
split_work(work::WorkUnit) = (p = rand(1:999); split_work(work, p/1000))

# ------ Message type for passing messages between processes and tasks
struct Message
    kind::Symbol
    data::Int64
    _data2 # TODO: rename data and _data2 to more sensible names
end

Message(kind, data) = Message(kind, data, -1)
