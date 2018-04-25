
using DataFrames
using CSV

P = 2
addprocs(P)

@everywhere using LoadBalancers

header = [:strategy, :nprocs, :nwork, :walltime, :overhead, :speedup, :efficiency]

df = ["strategy" 1 1 1 1 1 1;]

Uᵧ = 2
    
bench(strat, W, U) = (gc_enable(false);
                      balancer = strat();
                      work = WorkUnit(W, U);
                      t = @elapsed(parallel_lb(balancer, work));
                      gc_enable(true);
                      gc();
                      t)

for (name, strat) in [("arr", ARRLoadBalancer), 
                      ("rp", RPLoadBalancer),
                      ("sb", SBLoadBalancer)]

    @printf("Benchmarking %s strategy\n", name)


    results = [(bench(strat, w, Uᵧ), w) for w = 5:5:20]

    for (Tₚ, W) in results
        Tᵧ = W*Uᵧ

        Tₒ = P*Tₚ - Tᵧ
        S = Tᵧ/Tₚ
        E = S/P

        df = [df; name P W Tₚ Tₒ S E]
    end
end

df = DataFrame(df[2:end,1:end], header)

rmprocs(workers())

display(df)

CSV.write("out.csv", df)
