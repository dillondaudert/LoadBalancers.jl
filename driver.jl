# driver for benchmarking different load balancing algorithms

P = 3
addprocs(P)

@everywhere using LoadBalancers

W = 20
Uᵧ = 3
Tᵧ = W*Uᵧ
    
@printf("ARR Strategy\n")
work = WorkUnit(W, Uᵧ)
Tₚ = @elapsed parallel_lb_arr(work)

Tₒ = P*Tₚ - Tᵧ
S = Tᵧ/Tₚ
E = S/P

@printf("Walltime: %4.2f\nOverhead/P: %4.2f\nSpeedup: %4.2f\nEfficiency: %4.2f\n", Tₚ, Tₒ/P, S, E)

@printf("RP Strategy\n")
work = WorkUnit(W, Uᵧ)
Tₚ = @elapsed parallel_lb_rp(work)

Tₒ = P*Tₚ - Tᵧ
S = Tᵧ/Tₚ
E = S/P

@printf("Walltime: %4.2f\nOverhead/P: %4.2f\nSpeedup: %4.2f\nEfficiency: %4.2f\n", Tₚ, Tₒ/P, S, E)


@printf("SB Strategy\n")
work = WorkUnit(W, Uᵧ)
Tₚ = @elapsed parallel_lb_sb(work)

Tₒ = P*Tₚ - Tᵧ
S = Tᵧ/Tₚ
E = S/P

@printf("Walltime: %4.2f\nOverhead/P: %4.2f\nSpeedup: %4.2f\nEfficiency: %4.2f\n", Tₚ, Tₒ/P, S, E)
