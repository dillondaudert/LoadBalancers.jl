# tests for work helper functions
using Base.Test
include("work.jl")


@testset "split_work tests" begin
    testwork = WorkUnit(10, 3)
    testwork2 = WorkUnit(11, 4)

    @test_throws DomainError split_work(testwork, -0.5)
    @test_throws DomainError split_work(testwork2, 1.63)


end
