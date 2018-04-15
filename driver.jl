# driver for benchmarking different load balancing algorithms

using DataFrames
using CSV

P = 2
addprocs(P)

@everywhere using LoadBalancers

header = [:strategy, :nprocs, :nwork, :walltime, :overhead, :speedup, :efficiency]

df = ["strategy" 1 1 1 1 1 1;]

W = 20
Uᵧ = 3
Tᵧ = W*Uᵧ
    
@printf("ARR Strategy\n")
work = WorkUnit(W, Uᵧ)
Tₚ = @elapsed parallel_lb_arr(work)

Tₒ = P*Tₚ - Tᵧ
S = Tᵧ/Tₚ
E = S/P

df = [df; "arr" P W Tₚ Tₒ S E]
#@printf("Walltime: %4.2f\nOverhead/P: %4.2f\nSpeedup: %4.2f\nEfficiency: %4.2f\n", Tₚ, Tₒ/P, S, E)

@printf("RP Strategy\n")
work = WorkUnit(W, Uᵧ)
Tₚ = @elapsed parallel_lb_rp(work)

Tₒ = P*Tₚ - Tᵧ
S = Tᵧ/Tₚ
E = S/P

#@printf("Walltime: %4.2f\nOverhead/P: %4.2f\nSpeedup: %4.2f\nEfficiency: %4.2f\n", Tₚ, Tₒ/P, S, E)
df = [df; "rp" P W Tₚ Tₒ S E]


@printf("SB Strategy\n")
work = WorkUnit(W, Uᵧ)
Tₚ = @elapsed parallel_lb_sb(work)

Tₒ = P*Tₚ - Tᵧ
S = Tᵧ/Tₚ
E = S/P

#@printf("Walltime: %4.2f\nOverhead/P: %4.2f\nSpeedup: %4.2f\nEfficiency: %4.2f\n", Tₚ, Tₒ/P, S, E)
df = [df; "sb" P W Tₚ Tₒ S E]

df = DataFrame(df[2:end,1:end], header)

display(df)

CSV.write("out.csv", df)
