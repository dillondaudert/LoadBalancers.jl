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
    """

    # check that valid percentage
    !(0 < p < 1) && (@printf("p must be: 0 < p < 1, got %g\n", p); throw(DomainError()))
    
    u_units = ceil(Int32, work.units * p)
    v_units = work.units - u_units

    return WorkUnit(u_units, work.unitcost), WorkUnit(v_units, work.unitcost)

end

split_work(work::WorkUnit) = (p = rand(); split_work(work, p))
