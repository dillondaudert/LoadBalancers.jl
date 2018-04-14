# driver for benchmarking different load balancing algorithms

P = 3
addprocs(P)

# import just because this is not an installed package yet
@everywhere include(string(homedir(), "/github/LoadBalancers.jl/src/LoadBalancers.jl"))
@everywhere using LoadBalancers

W = 20
Uᵧ = 3
Tᵧ = W*Uᵧ
    
@printf("ARR Strategy\n")
work = WorkUnit(W, Uᵧ)
Tₚ = parallel_lb_arr(work)

Tₒ = P*Tₚ - Tᵧ
S = Tᵧ/Tₚ
E = S/P

@printf("Walltime: %4.2f\nOverhead: %4.2f\nSpeedup: %4.2f\nEfficiency: %4.2f\n", Tₚ, Tₒ, S, E)

@printf("RP Strategy\n")
work = WorkUnit(W, Uᵧ)
Tₚ = parallel_lb_rp(work)

Tₒ = P*Tₚ - Tᵧ
S = Tᵧ/Tₚ
E = S/P

@printf("Walltime: %4.2f\nOverhead: %4.2f\nSpeedup: %4.2f\nEfficiency: %4.2f\n", Tₚ, Tₒ, S, E)


@printf("SB Strategy\n")
work = WorkUnit(W, Uᵧ)
Tₚ = parallel_lb_sb(work)

Tₒ = P*Tₚ - Tᵧ
S = Tᵧ/Tₚ
E = S/P

@printf("Walltime: %4.2f\nOverhead: %4.2f\nSpeedup: %4.2f\nEfficiency: %4.2f\n", Tₚ, Tₒ, S, E)
