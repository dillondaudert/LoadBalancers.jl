# helper functions for simulating work

struct WorkUnit{T<:Int}
    units::T
    unitcost::T
end

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

    # set the rounding mode to up or down depending on p
    mode = p ≤ .5 ? RoundUp : RoundDown
    
    u_units = round(typeof(work.units), work.units * p, mode)
    v_units = work.units - u_units

    return WorkUnit(u_units, work.unitcost), WorkUnit(v_units, work.unitcost)

end

# generate a random percentage from the closed interval (0, 1)
split_work(work::WorkUnit) = (p = rand(1:999); split_work(work, p/1000))
