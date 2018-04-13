# driver for benchmarking different load balancing algorithms

P = 3
addprocs(P)

@everywhere include(string(homedir(), "/github/jlance/LoadBalancers.jl"))
@everywhere using LoadBalancers

W = 20
Uᵧ = 3
Tᵧ = W*Uᵧ
    
@printf("RP Strategy\n")
work = WorkUnit(W, Uᵧ)
Tₚ = parallel_lb_rp(32, work)

Tₒ = P*Tₚ - Tᵧ
S = Tᵧ/Tₚ
E = S/P

@printf("Walltime: %4.2f\nOverhead: %4.2f\nSpeedup: %4.2f\nEfficiency: %4.2f\n", Tₚ, Tₒ, S, E)


@printf("SB Strategy\n")
work = WorkUnit(W, Uᵧ)
Tₚ = parallel_lb_sb(32, work)

Tₒ = P*Tₚ - Tᵧ
S = Tᵧ/Tₚ
E = S/P

@printf("Walltime: %4.2f\nOverhead: %4.2f\nSpeedup: %4.2f\nEfficiency: %4.2f\n", Tₚ, Tₒ, S, E)
