# tests for the SubTasks module

using Base.Test
include("subtasks.jl")

@testset "SubTasks Tests" begin

    if nprocs() > 1
        @testset "Single Processor Tests" begin


        end
    else  
        @testset "Multi Processor Tests" begin


        end
    end
end
