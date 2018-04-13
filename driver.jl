# driver for benchmarking different load balancing algorithms

P = 3
addprocs(P)

@everywhere include(string(homedir(), "/github/jlance/work.jl"))
@everywhere using WorkUnits
@everywhere include(string(homedir(), "/github/jlance/subtasks.jl"))
@everywhere using SubTasks
@everywhere include(string(homedir(), "/github/jlance/RPStrategy.jl"))
@everywhere include(string(homedir(), "/github/jlance/SBStrategy.jl"))
@everywhere using RPController
@everywhere using SBController

W = 20
Uᵧ = 3
Tᵧ = W*Uᵧ
    
@printf("RP Strategy\n")
work = WorkUnit(W, Uᵧ)
Tₚ = rp_controller(work)

Tₒ = P*Tₚ - Tᵧ
S = Tᵧ/Tₚ
E = S/P

@printf("Walltime: %4.2f\nOverhead: %4.2f\nSpeedup: %4.2f\nEfficiency: %4.2f\n", Tₚ, Tₒ, S, E)


@printf("SB Strategy\n")
work = WorkUnit(W, Uᵧ)
Tₚ = sb_controller(work)

Tₒ = P*Tₚ - Tᵧ
S = Tᵧ/Tₚ
E = S/P

@printf("Walltime: %4.2f\nOverhead: %4.2f\nSpeedup: %4.2f\nEfficiency: %4.2f\n", Tₚ, Tₒ, S, E)